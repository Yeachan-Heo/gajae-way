/**
 * I7a/I7b provider health (owner decision Q2: passive turn outcomes PLUS an
 * active credential probe, fused). The live incident this protects against was
 * an `OPENAI_BASE_URL` on http: the redirect dropped the bearer, every turn
 * failed with a folded `provider_rejected`, and `gateway.status` stayed green.
 *
 * Passive: consecutive provider-class terminal failures within a window set the
 * gate. Active: `GET ${OPENAI_BASE_URL}/models` with the same bearer gjc uses,
 * `redirect: manual` so a 3xx is itself the finding (`provider_redirect`), never
 * followed; no body logged, Location host only. Fusion: the gate is SET by
 * passive (>= 2 consecutive in 10 min) or active `auth|redirect`; it is CLEARED
 * only when the latest active probe is `ok` AND (a passive success postdates
 * the last passive failure OR no passive failure in 10 min). `/models` success
 * alone never clears a sustained inference failure. Status records which signal
 * set/cleared the gate (`provenance`).
 */

export const PROVIDER_FAILURE_CODES = new Set([
	"provider_rejected",
	"provider_down",
	"provider_unavailable",
	"prompt_failed",
	"agent_error",
]);

export function isProviderFailureCode(code: string | undefined): boolean {
	if (!code) return false;
	return PROVIDER_FAILURE_CODES.has(code) || code.startsWith("provider_http_");
}

export type ActiveClass = "ok" | "auth" | "redirect" | "unreachable" | "server";

export interface ActiveProbeResult {
	readonly at: number;
	readonly httpStatus: number | undefined;
	readonly class: ActiveClass;
	/** Redirect target host only (never the full URL, never a body). */
	readonly locationHost?: string;
	readonly detail?: string;
}

export interface ProviderStatus {
	readonly passive: {
		readonly lastCode: string | undefined;
		readonly consecutiveFailures: number;
		readonly lastFailureAt: number | undefined;
		readonly lastSuccessAt: number | undefined;
	};
	readonly active: ActiveProbeResult | undefined;
	readonly gate: "ok" | "provider_failing";
	readonly provenance: {
		readonly setBy: "passive" | "active" | undefined;
		readonly clearedBy: "active" | undefined;
		readonly changedAt: number | undefined;
	};
}

export interface ProviderHealthOptions {
	readonly now?: () => number;
	readonly windowMs?: number;
	readonly failuresToGate?: number;
	readonly log?: (line: string) => void;
}

export class ProviderHealth {
	readonly #now: () => number;
	readonly #windowMs: number;
	readonly #failuresToGate: number;
	readonly #log: (line: string) => void;
	#lastCode: string | undefined;
	#consecutive = 0;
	#lastFailureAt: number | undefined;
	#lastSuccessAt: number | undefined;
	#active: ActiveProbeResult | undefined;
	#gated = false;
	#setBy: "passive" | "active" | undefined;
	#clearedBy: "active" | undefined;
	#changedAt: number | undefined;

	constructor(options: ProviderHealthOptions = {}) {
		this.#now = options.now ?? (() => Date.now());
		this.#windowMs = options.windowMs ?? 10 * 60_000;
		this.#failuresToGate = options.failuresToGate ?? 2;
		this.#log = options.log ?? ((line) => console.error(line));
	}

	get gated(): boolean {
		return this.#gated;
	}

	/** A terminal turn outcome. Provider-class failures count; anything else is a success for the passive signal. */
	recordTurn(outcome: { ok: boolean; code?: string }): void {
		const now = this.#now();
		if (!outcome.ok && isProviderFailureCode(outcome.code)) {
			if (this.#lastFailureAt !== undefined && now - this.#lastFailureAt > this.#windowMs) this.#consecutive = 0;
			this.#consecutive += 1;
			this.#lastCode = outcome.code;
			this.#lastFailureAt = now;
			if (!this.#gated && this.#consecutive >= this.#failuresToGate) this.#set("passive");
			return;
		}
		if (outcome.ok) {
			this.#consecutive = 0;
			this.#lastSuccessAt = now;
			this.#maybeClear();
		}
	}

	recordActive(result: ActiveProbeResult): void {
		this.#active = result;
		if (result.class === "auth" || result.class === "redirect") {
			if (!this.#gated) this.#set("active");
			return;
		}
		if (result.class === "ok") this.#maybeClear();
	}

	/** Cadence: 60 s while gated, 5 min idle. */
	nextProbeDelayMs(): number {
		return this.#gated ? 60_000 : 5 * 60_000;
	}

	status(): ProviderStatus {
		return {
			passive: {
				lastCode: this.#lastCode,
				consecutiveFailures: this.#consecutive,
				lastFailureAt: this.#lastFailureAt,
				lastSuccessAt: this.#lastSuccessAt,
			},
			active: this.#active,
			gate: this.#gated ? "provider_failing" : "ok",
			provenance: { setBy: this.#setBy, clearedBy: this.#clearedBy, changedAt: this.#changedAt },
		};
	}

	#set(by: "passive" | "active"): void {
		this.#gated = true;
		this.#setBy = by;
		this.#clearedBy = undefined;
		this.#changedAt = this.#now();
		this.#log(
			`provider_failing set_by=${by} last_code=${this.#lastCode ?? "none"} active=${this.#active?.class ?? "none"}`,
		);
	}

	#maybeClear(): void {
		if (!this.#gated) return;
		const now = this.#now();
		const activeOk = this.#active?.class === "ok";
		const passiveRecovered =
			this.#lastFailureAt === undefined ||
			(this.#lastSuccessAt !== undefined && this.#lastSuccessAt > this.#lastFailureAt) ||
			now - this.#lastFailureAt > this.#windowMs;
		if (!activeOk || !passiveRecovered) return;
		this.#gated = false;
		this.#clearedBy = "active";
		this.#changedAt = now;
		this.#log("provider_failing cleared_by=active passive_recovered=true");
	}
}

/** Redacted, never-following `GET ${base}/models`. */
export async function probeProvider(input: {
	baseUrl: string | undefined;
	apiKey: string | undefined;
	timeoutMs?: number;
	fetchImpl?: typeof fetch;
	now?: () => number;
}): Promise<ActiveProbeResult> {
	const at = (input.now ?? (() => Date.now()))();
	if (!input.baseUrl) return { at, httpStatus: undefined, class: "unreachable", detail: "OPENAI_BASE_URL unset" };
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), input.timeoutMs ?? 5_000);
	try {
		const response = await (input.fetchImpl ?? fetch)(`${input.baseUrl.replace(/\/+$/, "")}/models`, {
			method: "GET",
			redirect: "manual",
			signal: controller.signal,
			headers: input.apiKey ? { Authorization: `Bearer ${input.apiKey}` } : {},
		});
		const status = response.status;
		if (status >= 300 && status < 400) {
			let host: string | undefined;
			try {
				host = new URL(response.headers.get("location") ?? "", input.baseUrl).host;
			} catch {
				host = undefined;
			}
			return {
				at,
				httpStatus: status,
				class: "redirect",
				...(host ? { locationHost: host } : {}),
				detail: "provider_redirect",
			};
		}
		if (status === 401 || status === 403) return { at, httpStatus: status, class: "auth" };
		if (status >= 500) return { at, httpStatus: status, class: "server" };
		if (status >= 200 && status < 300) return { at, httpStatus: status, class: "ok" };
		return { at, httpStatus: status, class: "server", detail: `unexpected ${status}` };
	} catch (error) {
		return { at, httpStatus: undefined, class: "unreachable", detail: error instanceof Error ? error.name : "error" };
	} finally {
		clearTimeout(timer);
	}
}
