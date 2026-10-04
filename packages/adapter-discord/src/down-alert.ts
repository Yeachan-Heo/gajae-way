import { open } from "node:fs/promises";
import type { GatewayDownAlertConfig } from "./config";

/** Longer than any routine gateway restart (an update restart is back in well under a minute). */
export const DEFAULT_GATEWAY_DOWN_ALERT_MS = 5 * 60_000;
const QUOTED_ERROR_CHARS = 200;
const LOG_TAIL_BYTES = 8192;

export interface GatewayDownAlarmPorts {
	readonly now: () => number;
	readonly post: (text: string) => Promise<void>;
	/** Last line of the gateway's error log; undefined when there is none to quote. */
	readonly lastError: () => Promise<string | undefined>;
	readonly log: Pick<Console, "log" | "error">;
}

/**
 * One line when the gateway link has been down `afterMs`, one when it returns.
 *
 * `down()` is called on every failed reconnect, `up()` when the link is back.
 * The clock starts at the first failure of an outage, including an adapter that
 * never reached the gateway after its own start. A failed post is retried on
 * the next reconnect; the recovery line only follows a down line that landed.
 */
export class GatewayDownAlarm {
	readonly #afterMs: number;
	#downSince: number | undefined;
	#alerted = false;
	#pending: Promise<boolean> | undefined;

	constructor(
		readonly config: GatewayDownAlertConfig,
		readonly ports: GatewayDownAlarmPorts,
	) {
		this.#afterMs = config.afterMs ?? DEFAULT_GATEWAY_DOWN_ALERT_MS;
	}

	down(): Promise<void> {
		const now = this.ports.now();
		this.#downSince ??= now;
		if (this.#alerted || this.#pending || now - this.#downSince < this.#afterMs) return Promise.resolve();
		const pending = this.#postDown(this.#downSince);
		this.#pending = pending;
		return pending.then((posted) => {
			// up() took the outage over while this post was in flight; it settles it.
			if (this.#pending !== pending) return;
			this.#pending = undefined;
			if (posted) this.#alerted = true;
		});
	}

	async up(): Promise<void> {
		const since = this.#downSince;
		const alerted = this.#alerted;
		const pending = this.#pending;
		this.#downSince = undefined;
		this.#alerted = false;
		this.#pending = undefined;
		if (since === undefined) return;
		if (!alerted && !(pending && (await pending))) return;
		const minutes = Math.max(1, Math.round((this.ports.now() - since) / 60_000));
		try {
			await this.ports.post(`가재 중계 서버 돌아왔어(${minutes}분 꺼져 있었어).`);
			this.ports.log.log(`gateway_down_alert recovered downMinutes=${minutes}`);
		} catch (error) {
			this.ports.log.error(`gateway_down_alert recovery post failed: ${errorText(error)}`);
		}
	}

	async #postDown(since: number): Promise<boolean> {
		const lastError = await this.ports.lastError().catch(() => undefined);
		const mention = this.config.mentionUserId ? `<@${this.config.mentionUserId}> ` : "";
		const text = `${mention}가재 중계 서버가 ${clockTime(since)}부터 안 붙어. 마지막 오류: ${lastError ?? "-"}. 살펴줘`;
		try {
			await this.ports.post(text);
			this.ports.log.log(`gateway_down_alert posted downSince=${new Date(since).toISOString()}`);
			return true;
		} catch (error) {
			this.ports.log.error(`gateway_down_alert post failed (retried on the next reconnect): ${errorText(error)}`);
			return false;
		}
	}
}

/** Last non-empty line of a log, read from its tail so a large log costs one small read. */
export async function lastLogLine(path: string): Promise<string | undefined> {
	const handle = await open(path, "r");
	try {
		const { size } = await handle.stat();
		const length = Math.min(size, LOG_TAIL_BYTES);
		const buffer = Buffer.alloc(length);
		await handle.read(buffer, 0, length, size - length);
		const line = buffer
			.toString("utf8")
			.split("\n")
			.map((entry) => entry.trim())
			.filter((entry) => entry !== "")
			.at(-1);
		return line?.slice(0, QUOTED_ERROR_CHARS);
	} finally {
		await handle.close();
	}
}

/** Local wall-clock HH:MM: the reader compares it with when they last heard from the bot. */
function clockTime(ms: number): string {
	const at = new Date(ms);
	return `${String(at.getHours()).padStart(2, "0")}:${String(at.getMinutes()).padStart(2, "0")}`;
}

function errorText(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
