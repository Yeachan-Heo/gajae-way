import type {
	FunctionalReadinessCategory,
	FunctionalReadinessFailure,
	GatewayReadinessResult,
} from "@gajae-gateway/protocol";

export const FUNCTIONAL_READINESS_WINDOW_MS = 5 * 60_000;
export const FUNCTIONAL_READINESS_FAILURE_THRESHOLD = 3;

interface FailureObservation {
	readonly atMs: number;
	readonly failure: FunctionalReadinessFailure;
}

/** Process-local, bounded evidence for functional health independent of liveness. */
export class FunctionalReadiness {
	readonly #failures = new Map<FunctionalReadinessCategory, FailureObservation[]>();

	recordFailure(input: {
		readonly category: FunctionalReadinessCategory;
		readonly code: string;
		readonly detail: string;
		readonly at?: number;
		readonly origin?: string;
	}): void {
		const atMs = Number.isFinite(input.at) ? (input.at as number) : Date.now();
		const active = (this.#failures.get(input.category) ?? []).filter(
			(observation) => atMs >= observation.atMs && atMs - observation.atMs < FUNCTIONAL_READINESS_WINDOW_MS,
		);
		const code = bounded(input.code, 80).replace(/[^A-Za-z0-9_.-]/g, "_") || "functional_failure";
		const failure: FunctionalReadinessFailure = {
			category: input.category,
			code,
			detail: bounded(input.detail, 200),
			at: new Date(atMs).toISOString(),
			...(input.origin ? { origin: bounded(input.origin, 160) } : {}),
			consecutiveFailures: Math.min(active.length + 1, FUNCTIONAL_READINESS_FAILURE_THRESHOLD),
		};
		active.push({ atMs, failure });
		if (active.length > FUNCTIONAL_READINESS_FAILURE_THRESHOLD) active.shift();
		this.#failures.set(input.category, active);
	}

	recordSuccess(category: FunctionalReadinessCategory): void {
		this.#failures.delete(category);
	}

	snapshot(nowMs = Date.now()): GatewayReadinessResult {
		let latest: FailureObservation | undefined;
		for (const [category, observations] of this.#failures) {
			const active = observations.filter(
				(observation) => nowMs >= observation.atMs && nowMs - observation.atMs < FUNCTIONAL_READINESS_WINDOW_MS,
			);
			if (active.length < FUNCTIONAL_READINESS_FAILURE_THRESHOLD) continue;
			const current = active.at(-1);
			if (current && (!latest || current.atMs > latest.atMs)) latest = current;
			if (active.length !== observations.length) this.#failures.set(category, active);
		}
		if (!latest) {
			return {
				ready: true,
				state: "ready",
				windowMs: FUNCTIONAL_READINESS_WINDOW_MS,
				failureThreshold: FUNCTIONAL_READINESS_FAILURE_THRESHOLD,
				reason: null,
				failure: null,
			};
		}
		const failure = latest.failure;
		return {
			ready: false,
			state: "degraded",
			windowMs: FUNCTIONAL_READINESS_WINDOW_MS,
			failureThreshold: FUNCTIONAL_READINESS_FAILURE_THRESHOLD,
			reason: `${failure.category}: ${failure.code} (${FUNCTIONAL_READINESS_FAILURE_THRESHOLD} consecutive failures within 5m)`,
			failure,
		};
	}
}

function bounded(value: string, maxLength: number): string {
	const characters: string[] = [];
	let length = 0;
	for (const character of value) {
		const code = character.codePointAt(0) ?? 0;
		const safeCharacter = code <= 0x1f || code === 0x7f ? " " : character;
		if (length + safeCharacter.length > maxLength) break;
		characters.push(safeCharacter);
		length += safeCharacter.length;
	}
	return characters.join("").trim();
}
