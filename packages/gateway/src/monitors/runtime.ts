import { readFile } from "node:fs/promises";
import type { GatewayConfig } from "../config";
import type { MonitorPropagator } from "./propagate";
import type { MonitorRegistry } from "./registry";
import { startCron } from "./triggers/cron";
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
	constructor(
		config: GatewayConfig,
		registry: MonitorRegistry,
		propagator: MonitorPropagator,
		options: { now?: () => Date } = {},
	) {
		this.#config = config;
		this.#registry = registry;
		this.#propagator = propagator;
		this.#clock = options.now;
	}
	async start(): Promise<void> {
		await this.stop();
		const monitors = this.#registry.list().filter((monitor) => monitor.enabled);
		const webhooks = monitors.filter((monitor) => monitor.trigger.kind === "webhook");
		if (webhooks.length) await this.#startWebhook(webhooks);
		for (const monitor of monitors) {
			if (monitor.trigger.kind === "cron")
				this.#stops.push(
					startCron(
						monitor.trigger.schedule,
						(slotAt) => {
							// Atomic slot-claim + event admission inside the propagator;
							// duplicate slots (restart overlap) return null and are skipped.
							this.#propagator.submitSlot(
								monitor.monitorId,
								monitor.eventTypes[0]!,
								{ at: slotAt.toISOString() },
								slotAt,
							);
						},
						{ now: this.#clock },
					),
				);
			if (monitor.trigger.kind === "watcher") {
				if (!this.#config.watcherRoots?.length) throw new MonitorRuntimeError("watcherRoots must be configured");
				this.#stops.push(
					await startWatcher(
						monitor.trigger.root,
						this.#config.watcherRoots,
						(path) => this.#propagator.submit(monitor.monitorId, monitor.eventTypes[0]!, { path }),
						monitor.trigger.debounceMs,
					),
				);
			}
			if (monitor.trigger.kind === "script") {
				if (!this.#config.scriptRoot) throw new MonitorRuntimeError("scriptRoot must be configured");
				this.#stops.push(
					await startScript(monitor.trigger.command, monitor.trigger.intervalMs, this.#config.scriptRoot, (stdout) =>
						this.#propagator.submit(monitor.monitorId, monitor.eventTypes[0]!, { stdout }),
					),
				);
			}
		}
	}
	async refresh(): Promise<void> {
		await this.start();
	}
	async stop(): Promise<void> {
		for (const stop of this.#stops.splice(0)) stop();
	}
	async #startWebhook(monitors: ReturnType<MonitorRegistry["list"]>): Promise<void> {
		const config = this.#config.webhook;
		if (!config) throw new MonitorRuntimeError("webhook configuration is required");
		const bind = config.bind ?? "127.0.0.1";
		const nonLoopback = bind !== "127.0.0.1" && bind !== "::1";
		const records: WebhookMonitor[] = [];
		for (const monitor of monitors) {
			const trigger = monitor.trigger as WebhookTrigger;
			if (nonLoopback && (!config.exposeNonLoopback || !trigger.auth))
				throw new MonitorRuntimeError(
					"non-loopback webhook binding requires exposeNonLoopback and auth on every webhook monitor",
				);
			const secret = trigger.auth ? (await readFile(trigger.auth.credentialFile, "utf8")).trim() : undefined;
			records.push({
				monitorId: monitor.monitorId,
				route: trigger.route,
				eventType: monitor.eventTypes[0]!,
				...(trigger.auth && secret ? { auth: { kind: trigger.auth.kind, secret } } : {}),
			});
		}
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
