import { readFile } from "node:fs/promises";
import { stripControlCharacters } from "@gajae-gateway/log";
import type { GatewayConfig } from "../config";
import type { MonitorPropagator } from "./propagate";
import type { MonitorRegistry } from "./registry";
import { type CronCatchUpPolicy, startCron } from "./triggers/cron";
import { startScript } from "./triggers/script";
import { startWatcher } from "./triggers/watcher";
import { startWebhook, type WebhookMonitor } from "./triggers/webhook";

type Stop = () => void;
type WebhookTrigger = { kind: "webhook"; route: string; auth?: { kind: "hmac" | "bearer"; credentialFile: string } };

export class MonitorRuntimeError extends Error {
	readonly code = "monitor_runtime_invalid";
}

export class MonitorRuntime {
	readonly #config: GatewayConfig;
	readonly #registry: MonitorRegistry;
	readonly #propagator: MonitorPropagator;
	#stops: Stop[] = [];
	readonly #clock?: () => Date;
	readonly #catchUp?: CronCatchUpPolicy;
	#restartTail: Promise<void> = Promise.resolve();
	#queuedRestart: Promise<void> | null = null;
	constructor(
		config: GatewayConfig,
		registry: MonitorRegistry,
		propagator: MonitorPropagator,
		options: { now?: () => Date; catchUp?: CronCatchUpPolicy } = {},
	) {
		this.#config = config;
		this.#registry = registry;
		this.#propagator = propagator;
		this.#clock = options.now;
		this.#catchUp = options.catchUp;
	}
	/**
	 * Restart every enabled monitor from the current registry. Restarts run one at a time; a call made
	 * while one is in flight queues exactly one more restart, and calls made while that restart is still
	 * queued share it. The queued restart reads the registry when it begins, so it sees every mutation
	 * made before it starts.
	 */
	start(): Promise<void> {
		if (this.#queuedRestart) return this.#queuedRestart;
		const restart = this.#restartTail.then(() => {
			this.#queuedRestart = null;
			return this.#restart();
		});
		this.#queuedRestart = restart;
		this.#restartTail = restart.catch(() => undefined);
		return restart;
	}
	refresh(): Promise<void> {
		return this.start();
	}
	async #restart(): Promise<void> {
		await this.stop();
		const monitors = this.#registry.list().filter((monitor) => monitor.enabled);
		const invalid = this.#registry.invalid();
		const invalidIds = new Set(invalid.map((record) => record.monitorId));
		const webhooks = monitors.filter((monitor) => monitor.trigger.kind === "webhook");
		// Start webhook group, tracking per-monitor failures. A shared config or bind error
		// aborts all webhooks; per-monitor credential/preparation errors are isolated.
		const webhookFailures = new Map<string, string>();
		if (webhooks.length) {
			try {
				await this.#startWebhook(webhooks, webhookFailures);
			} catch (error) {
				// Global webhook config or listen failure: skip all webhooks.
				const reason = stripControlCharacters(
					(error instanceof Error ? error.message : String(error)).replace(/\s+/g, " ").slice(0, 300),
				);
				for (const webhook of webhooks) {
					webhookFailures.set(webhook.monitorId, reason);
				}
			}
		}
		// Report invalid monitors that were loaded (e.g., cron schedules that fail validation)
		// and monitors that cannot be deserialized (loadable:false). The latter are skipped
		// and cannot be started, so log a warning at startup.
		for (const record of invalid) {
			if (invalidIds.has(record.monitorId)) {
				const reason = stripControlCharacters(record.reason);
				const monitor = this.#registry.get(record.monitorId);
				const kind = monitor?.trigger.kind ?? "unknown";
				console.error(`monitor_invalid id=${record.monitorId} kind=${kind} reason=${reason}`);
			}
		}
		// One stored monitor that cannot start (a schedule saved before write validation, a trigger the
		// gateway config cannot serve) is skipped and logged; it never takes the gateway down with it.
		for (const monitor of monitors) {
			try {
				// Skip monitors with invalid schedules that are marked as loadable but not startable.
				if (invalidIds.has(monitor.monitorId)) continue;
				// Skip webhook monitors (they're handled by #startWebhook, failures tracked above).
				if (monitor.trigger.kind === "webhook") {
					if (webhookFailures.has(monitor.monitorId)) {
						const reason = stripControlCharacters(webhookFailures.get(monitor.monitorId) ?? "unknown error");
						console.error(`monitor_invalid id=${monitor.monitorId} kind=webhook reason=${reason}`);
					}
					continue;
				}
				await this.#startOne(monitor);
			} catch (error) {
				const reason = stripControlCharacters(
					(error instanceof Error ? error.message : String(error)).replace(/\s+/g, " ").slice(0, 300),
				);
				console.error(`monitor_invalid id=${monitor.monitorId} kind=${monitor.trigger.kind} reason=${reason}`);
			}
		}
	}
	async #startOne(monitor: ReturnType<MonitorRegistry["list"]>[number]): Promise<void> {
		const eventType = monitor.eventTypes[0];
		if (eventType === undefined) throw new MonitorRuntimeError("monitor must declare at least one event type");
		if (monitor.trigger.kind === "cron")
			this.#stops.push(
				startCron(
					monitor.trigger.schedule,
					{
						// The cursor cannot precede monitor creation, even if skip state or
						// an old slot-ledger row somehow does.
						cursor: () => {
							const created = Date.parse(monitor.createdAt);
							const stored = this.#registry.cronCursor(monitor.monitorId);
							return new Date(stored === undefined ? created : Math.max(created, Date.parse(stored)));
						},
						// submitSlot returns an id for both admitted and overlap-skipped
						// outcomes; only an already-claimed slot returns null.
						fire: (slotAt) =>
							this.#propagator.submitSlot(monitor.monitorId, eventType, { at: slotAt.toISOString() }, slotAt) !== null,
						skipped: (skip) => {
							const oldest = skip.oldest.toISOString();
							const newest = skip.newest.toISOString();
							this.#registry.recordCronSkip(
								monitor.monitorId,
								{ count: skip.count, oldest, newest },
								(this.#clock?.() ?? new Date()).toISOString(),
							);
							console.error(
								`monitor_slots_skipped monitor=${monitor.monitorId} count=${skip.count} oldest=${oldest} newest=${newest}`,
							);
						},
					},
					{ now: this.#clock, policy: this.#catchUp, timezone: monitor.trigger.timezone },
				),
			);
		if (monitor.trigger.kind === "watcher") {
			if (!this.#config.watcherRoots?.length) throw new MonitorRuntimeError("watcherRoots must be configured");
			this.#stops.push(
				await startWatcher(
					monitor.trigger.root,
					this.#config.watcherRoots,
					(path) => this.#propagator.submit(monitor.monitorId, eventType, { path }),
					monitor.trigger.debounceMs,
				),
			);
		}
		if (monitor.trigger.kind === "script") {
			if (!this.#config.scriptRoot) throw new MonitorRuntimeError("scriptRoot must be configured");
			this.#stops.push(
				await startScript(monitor.trigger.command, monitor.trigger.intervalMs, this.#config.scriptRoot, (stdout) =>
					this.#propagator.submit(monitor.monitorId, eventType, { stdout }),
				),
			);
		}
	}
	async stop(): Promise<void> {
		for (const stop of this.#stops.splice(0)) stop();
	}
	async #startWebhook(monitors: ReturnType<MonitorRegistry["list"]>, failures: Map<string, string>): Promise<void> {
		const config = this.#config.webhook;
		if (!config) throw new MonitorRuntimeError("webhook configuration is required");
		const bind = config.bind ?? "127.0.0.1";
		const nonLoopback = bind !== "127.0.0.1" && bind !== "::1";
		const records: WebhookMonitor[] = [];
		// Per-monitor preparation: credential files, event type validation.
		// Shared preparation (config/bind) above will throw if invalid, aborting all.
		for (const monitor of monitors) {
			try {
				const trigger = monitor.trigger as WebhookTrigger;
				const eventType = monitor.eventTypes[0];
				if (eventType === undefined) throw new MonitorRuntimeError("monitor must declare at least one event type");
				if (nonLoopback && (!config.exposeNonLoopback || !trigger.auth))
					throw new MonitorRuntimeError(
						"non-loopback webhook binding requires exposeNonLoopback and auth on every webhook monitor",
					);
				const secret = trigger.auth ? (await readFile(trigger.auth.credentialFile, "utf8")).trim() : undefined;
				records.push({
					monitorId: monitor.monitorId,
					route: trigger.route,
					eventType,
					...(trigger.auth && secret ? { auth: { kind: trigger.auth.kind, secret } } : {}),
				});
			} catch (error) {
				const reason = stripControlCharacters(
					(error instanceof Error ? error.message : String(error)).replace(/\s+/g, " ").slice(0, 300),
				);
				failures.set(monitor.monitorId, reason);
			}
		}
		// Only start webhook server if there are valid records.
		if (records.length === 0) return;
		const server = startWebhook({
			bind,
			port: config.port,
			exposeNonLoopback: config.exposeNonLoopback,
			monitors: () => records,
			submit: (monitorId, eventType, payload) => this.#propagator.submit(monitorId, eventType, payload),
		});
		this.#stops.push(() => server.stop(true));
	}
}
