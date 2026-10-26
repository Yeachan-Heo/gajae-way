import { open, readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { stripVTControlCharacters } from "node:util";
import type { GatewayStatusResult, SessionListResult, WorkJobsResult } from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import { formatDuration, originId } from "../../admin/src/format";
import { parseUpdateArgs, runUpdate, type UpdateResult } from "../../cli/src/update";
import metadata from "../../gateway/package.json";
import { Table } from "./tui-components";
import { ANSIRenderer, type Color, KeyboardInput, redact, terminalDimensions } from "./tui-core";

export type ConsolePanel = "status" | "sessions" | "lanes" | "updates" | "logs";
export interface ConsoleClient {
	request<T = unknown>(verb: string, params?: unknown): Promise<T>;
	on(event: string, handler: (payload: unknown) => void): () => void;
	onDisconnect(handler: (error: Error) => void): () => void;
	close(): Promise<void>;
}
export interface ConsoleOptions {
	socket?: string;
	home?: string;
	panel?: ConsolePanel;
	renderer?: Pick<ANSIRenderer, "clear" | "moveCursor" | "write" | "hideCursor" | "showCursor" | "resetStyle">;
	input?: Pick<KeyboardInput, "readKey" | "close">;
	client?: ConsoleClient;
}
const PANELS: ConsolePanel[] = ["status", "sessions", "lanes", "updates", "logs"];
const UNAVAILABLE = "Unavailable (not exposed by gateway)";

/** Bounded reads avoid loading a potentially multi-gigabyte daemon log. */
async function tailLog(path: string): Promise<string[]> {
	const file = await open(path, "r");
	try {
		const { size } = await file.stat();
		const start = Math.max(0, size - 128 * 1024);
		const buffer = Buffer.alloc(Math.min(size, 128 * 1024));
		const { bytesRead } = await file.read(buffer, 0, buffer.length, start);
		const lines = buffer.subarray(0, bytesRead).toString("utf8").split(/\r?\n/);
		if (start > 0) lines.shift();
		if (lines.at(-1) === "") lines.pop();
		return lines.slice(-1000);
	} finally {
		await file.close();
	}
}

/** Interactive read-mostly dashboard. No unrecognized management RPCs are sent. */
export async function runConsole(options: ConsoleOptions = {}): Promise<void> {
	const home = options.home ?? process.env.GAJAEWAY_HOME ?? join(homedir(), ".gajaeway");
	const socket = options.socket ?? process.env.GAJAEWAY_SOCKET ?? join(home, "gateway.sock");
	const renderer = options.renderer ?? new ANSIRenderer();
	const input = options.input ?? new KeyboardInput();
	const secrets = Object.entries(process.env)
		.filter(([key, value]) => /token|secret|password|api.?key|authorization/i.test(key) && value)
		.map(([, value]) => value as string);
	let configSummary = "unavailable";
	let logsSafe = true;
	try {
		const config = JSON.parse(await readFile(join(home, "config.json"), "utf8"));
		const collect = async (value: unknown): Promise<void> => {
			if (!value || typeof value !== "object") return;
			for (const [key, item] of Object.entries(value)) {
				if (/credentialFile|tokenFile|secretFile|passwordFile/i.test(key) && typeof item === "string") {
					try {
						const secret = (await readFile(resolve(home, item), "utf8")).trim();
						if (secret) secrets.push(secret);
					} catch {
						logsSafe = false;
					}
				} else if (/token|secret|password|api.?key/i.test(key) && typeof item === "string") secrets.push(item);
				else await collect(item);
			}
		};
		await collect(config);
		configSummary = `loaded; ${Object.keys(config.credentials ?? {}).length} credential references (hidden)`;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") logsSafe = false;
	}
	const safe = (value: unknown): string =>
		Array.from(stripVTControlCharacters(redact(String(value ?? "—"), secrets)))
			.map((character) => (character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127 ? " " : character))
			.join("")
			.replace(/Bearer\s+[^\s,"'}]+/gi, "Bearer [REDACTED]")
			.replace(
				/((?:token|password|secret|api[_-]?key|authorization)["']?\s*[:=]\s*["']?)([^\s,"'}]+)/gi,
				"$1[REDACTED]",
			);
	let client: ConsoleClient | undefined;
	let connected = false;
	let stopped = false;
	let panel = Math.max(0, PANELS.indexOf(options.panel ?? "status"));
	let status: GatewayStatusResult | undefined;
	let sessions: SessionListResult["sessions"] = [];
	let jobs: WorkJobsResult["jobs"] = [];
	let totalJobs = 0;
	let logs: string[] = [];
	let logOffset = 0;
	let autoScroll = true;
	let selected = 0;
	let inspecting = false;
	let notice = "";
	let rss = UNAVAILABLE;
	let update: UpdateResult | undefined;
	let checkedAt: string | undefined;
	let updateError = "";
	let confirming = false;
	let refreshing: Promise<void> | undefined;
	let timer: ReturnType<typeof setInterval> | undefined;
	const subscriptions: (() => void)[] = [];
	const errors = new Map<string, string>();
	const line = (text: unknown, color: Color = "white") => renderer.write(`${safe(text)}\n`, color);
	const uptime = () => (status ? formatDuration(Date.now() - Date.parse(status.startedAt)) : "—");
	const draw = () => {
		if (stopped) return;
		renderer.clear();
		line(`Gajaeway Console  v${metadata.version}  ${new Date().toLocaleTimeString()}`, "cyan");
		line(
			`Daemon: ${connected ? (status?.readiness?.ready === false ? "degraded" : status ? "connected" : "unavailable") : "disconnected"} | Uptime: ${uptime()} | RSS: ${rss}`,
			connected && status?.readiness?.ready !== false ? "green" : "red",
		);
		line(
			PANELS.map(
				(name, index) => `${index === panel ? ">" : " "}[${index + 1}] ${name[0]?.toUpperCase()}${name.slice(1)}`,
			).join("  "),
			"cyan",
		);
		const current = PANELS[panel];
		if (errors.has(current ?? "")) line(errors.get(current ?? ""), "red");
		if (current === "status") {
			line(`PID: ${status?.pid ?? "—"}`);
			line(`Socket: ${socket}`);
			line(`Uptime: ${uptime()}`);
			line(`Memory (RSS): ${rss}`);
			line(`Database: ${status ? `schema ${status.schemaVersion}` : "unavailable"}`);
			line(`Health: ${status?.readiness?.state ?? (status ? "connected (readiness unavailable)" : "unavailable")}`);
			line(
				`Adapters connected: ${
					status?.clients
						?.filter((entry) => /discord|slack|telegram/i.test(entry.name))
						.map((entry) => entry.name)
						.join(", ") || "none reported"
				}`,
			);
			line(`Config: ${configSummary}`);
			line(`GJC version: ${UNAVAILABLE}`);
			line("Next update check: on opening Updates or pressing r there");
		} else if (current === "sessions") {
			line(`Sessions: ${sessions.length} total`);
			if (!sessions.length) line("No sessions active.");
			const visible = sessions.slice(Math.max(0, selected - 12), Math.max(0, selected - 12) + 13);
			const rows = visible.map((session) =>
				[
					session === sessions[selected] ? `> ${originId(session.origin)}` : originId(session.origin),
					session.origin.peerId ?? "—",
					session.createdAt,
					"not reported",
					"bound",
				].map(safe),
			);
			new Table({
				columns: ["Origin", "User", "Started", "Model", "Status"].map((header) => ({ header })),
				rows,
			}).render(renderer as ANSIRenderer, { row: 6, col: 1 }, terminalDimensions().width);
			renderer.moveCursor(20, 1);
			const inspected = sessions[selected];
			if (inspecting && inspected)
				line(
					`Session: ${originId(inspected.origin)} | Last activity: ${inspected.lastActivityAt ?? "—"} | Epoch: ${inspected.epoch}`,
				);
			line("Up/down: select | i: inspect");
		} else if (current === "lanes") {
			line(`Lanes: ${totalJobs} total (${jobs.length} loaded)`);
			line("Name | Status | Progress | Started | Elapsed", "cyan");
			if (!jobs.length) line("No work lanes.");
			for (const job of jobs.slice(0, 14)) {
				const start = job.attempt?.started_at ?? job.accepted_at;
				const end = job.attempt?.ended_at;
				const color: Color = /fail/.test(job.state)
					? "red"
					: /running|dispatch/.test(job.state)
						? "green"
						: /paused|held|awaiting|pending/.test(job.state)
							? "yellow"
							: "reset";
				line(
					`${job.lane_key} | ${job.state} | ${job.last_commit ? `${job.last_commit.sha.slice(0, 8)} ${job.last_commit.subject}` : "not reported"} | ${start ?? "—"} | ${start ? formatDuration((end ? Date.parse(end) : Date.now()) - Date.parse(start)) : "—"}`,
					color,
				);
			}
		} else if (current === "updates") {
			line(`Current version: ${update?.current ?? metadata.version}`);
			line(`Latest version: ${update?.latest ?? "not checked"}`);
			line(`Last check: ${checkedAt ?? "never"}`);
			line(
				updateError || (update?.upToDate ? "Up to date" : update ? "[A]pply update" : "Press r to check for updates"),
				updateError ? "yellow" : "green",
			);
		} else {
			line(`Logs: ${autoScroll ? "live / auto-scroll" : "paused"} | up/down: scroll | space: resume`);
			if (!logs.length) line("No gateway logs available.");
			if (!logsSafe) line("Logs hidden: credential redaction sources could not be loaded.", "yellow");
			else for (const entry of logs.slice(logOffset, logOffset + 20)) line(entry);
		}
		renderer.moveCursor(Math.max(26, Math.min(terminalDimensions().height - 2, 30)), 1);
		if (confirming) line("Apply update and restart services? Enter confirms; Escape cancels.", "yellow");
		if (notice) line(notice, "yellow");
		line("[q]uit | [r]efresh | [1-5]navigate | [u]pdate | [g]c | [d]octor", "cyan");
	};
	const refresh = (): Promise<void> => {
		if (refreshing) return refreshing;
		refreshing = (async () => {
			if (!client || !connected) return;
			await Promise.all([
				client.request<GatewayStatusResult>("gateway.status").then(
					(value) => {
						status = value;
						errors.delete("status");
					},
					() => {
						errors.set("status", "Daemon status unavailable");
						status = undefined;
					},
				),
				client.request<SessionListResult>("session.list").then(
					(value) => {
						sessions = [...value.sessions].sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt));
						selected = Math.min(selected, Math.max(0, sessions.length - 1));
						errors.delete("sessions");
					},
					() => errors.set("sessions", "Session list unavailable"),
				),
				client.request<WorkJobsResult>("work.jobs", { limit: 100, offset: 0 }).then(
					(value) => {
						jobs = value.jobs;
						totalJobs = value.total;
						errors.delete("lanes");
					},
					() => errors.set("lanes", "Work lanes unavailable"),
				),
				tailLog(join(home, "gateway.log")).then(
					(value) => {
						logs = value;
						if (autoScroll) logOffset = Math.max(0, logs.length - 20);
						else logOffset = Math.min(logOffset, Math.max(0, logs.length - 20));
					},
					() => {
						logs = [];
					},
				),
			]);
			rss = UNAVAILABLE;
			// Only inspect the daemon PID on the local default socket, never a remote identity.
			if (socket === join(home, "gateway.sock") && Number.isSafeInteger(status?.pid)) {
				const memory = await readFile(`/proc/${status?.pid}/status`, "utf8").catch(() => "");
				const kb = memory.match(/^VmRSS:\s+(\d+)\s+kB/m)?.[1];
				if (kb) rss = `${(Number(kb) / 1024).toFixed(1)} MiB`;
			}
			draw();
		})()
			.catch(() => {
				notice = "Gateway refresh failed; displayed data may be stale.";
				draw();
			})
			.finally(() => {
				refreshing = undefined;
			});
		return refreshing;
	};
	const checkUpdate = async () => {
		notice = "Checking updates…";
		draw();
		try {
			update = await runUpdate({ args: parseUpdateArgs(["--check"]), home, gatewaySocketPath: socket });
			updateError = "";
		} catch {
			update = undefined;
			updateError = "Update check unavailable";
		}
		checkedAt = new Date().toLocaleString();
		notice = "";
		draw();
	};
	try {
		renderer.hideCursor();
		renderer.clear();
		line("Gajaeway Console — Connecting to gateway…", "cyan");
		try {
			client =
				options.client ??
				(await GajaewayClient.connectSocket(socket, { clientName: "gajaeway-console", requestTimeoutMs: 5000 }));
			connected = true;
			subscriptions.push(
				client.onDisconnect(() => {
					connected = false;
					notice = "Gateway disconnected. Quit and reopen to reconnect.";
					draw();
				}),
			);
			for (const event of ["chat.message", "chat.progress", "monitor.event"])
				subscriptions.push(
					client.on(event, () => {
						void refresh();
					}),
				);
			subscriptions.push(
				client.on("gateway.stopping", () => {
					connected = false;
					notice = "Gateway stopping.";
					draw();
				}),
			);
			await refresh();
		} catch {
			connected = false;
			notice = "Unable to connect to gateway. Start the daemon and reopen the console.";
			draw();
			throw new Error(notice);
		}
		if (PANELS[panel] === "updates") await checkUpdate();
		timer = setInterval(() => {
			void refresh();
		}, 2000);
		for (;;) {
			let event: Awaited<ReturnType<KeyboardInput["readKey"]>>;
			try {
				event = await input.readKey();
			} catch {
				break;
			}
			const key = event.key.toLowerCase();
			if (key === "q" || (event.isCtrl && key === "c")) break;
			if (confirming) {
				if (key === "escape") confirming = false;
				else if (key === "enter") {
					confirming = false;
					notice = "Applying update…";
					draw();
					try {
						update = await runUpdate({ args: parseUpdateArgs([]), home, gatewaySocketPath: socket });
						notice = update.upToDate ? "Already up to date" : "Update applied";
					} catch {
						notice = "Update failed; inspect the installation before retrying.";
					}
				}
				draw();
				continue;
			}
			const previousPanel = panel;
			if (/^[1-5]$/.test(key)) panel = Number(key) - 1;
			else if (key === "right" || key === "tab") panel = (panel + 1) % PANELS.length;
			else if (key === "left") panel = (panel + PANELS.length - 1) % PANELS.length;
			else if (key === "r") {
				await refresh();
				if (PANELS[panel] === "updates") await checkUpdate();
			} else if (key === "g")
				notice = "Cleanup unavailable: this gateway exposes no safe garbage-collection operation.";
			else if (key === "d") {
				if (!client || !connected) notice = "Diagnostics unavailable: daemon disconnected.";
				else
					try {
						const result = await client.request<{ phase: string; gates?: unknown[] }>("ops.cycle", {
							limit: 100,
							offset: 0,
						});
						notice = `Diagnostics: ${result.phase ?? "completed"}; ${result.gates?.length ?? 0} gates`;
					} catch {
						notice = "Diagnostics unavailable.";
					}
			} else if (key === "u" || key === "a") {
				panel = 3;
				if (!update) await checkUpdate();
				if (update && !update.upToDate) confirming = true;
			} else if (PANELS[panel] === "sessions") {
				if (key === "down") selected = Math.min(sessions.length - 1, selected + 1);
				if (key === "up") selected = Math.max(0, selected - 1);
				if (key === "i" || key === "enter") inspecting = !inspecting;
			} else if (PANELS[panel] === "logs") {
				if (key === "up" || key === "down") {
					autoScroll = false;
					logOffset = Math.max(0, Math.min(Math.max(0, logs.length - 20), logOffset + (key === "up" ? -1 : 1)));
				}
				if (key === " " || key === "space") {
					autoScroll = true;
					logOffset = Math.max(0, logs.length - 20);
				}
			}
			if (panel !== previousPanel && panel === 3 && !checkedAt) await checkUpdate();
			draw();
		}
	} finally {
		stopped = true;
		if (timer) clearInterval(timer);
		for (const unsubscribe of subscriptions) unsubscribe();
		try {
			await client?.close();
		} finally {
			input.close();
			renderer.resetStyle();
			renderer.showCursor();
		}
	}
}
