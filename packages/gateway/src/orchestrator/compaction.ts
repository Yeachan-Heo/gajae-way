/**
 * The gateway's seam onto GJC's native compaction control surface (issue #68).
 *
 * GJC's SDK exposes a `compaction.run` control action, and compaction is the
 * runtime's job — the gateway must never implement its own summarising
 * compaction, because a second summary contract would drift from GJC's.
 *
 * The gateway, however, does not hold a control channel today: every monitor
 * authoring turn is a ONE-SHOT `gjc --session <id> -p --mode json` process, and
 * that path exposes only `--resume`, `--session-dir` and `--no-session`. There
 * is nothing to send a control action over once the process has exited, and the
 * flags do not include compaction.
 *
 * So this is an interface with an honest default: `UnavailableCompactionPort`
 * reports `unavailable` instead of pretending a compaction ran. When the gateway
 * gains an SDK/control session (or gjc grows a compaction flag on the
 * non-interactive path), a real implementation drops in here and every caller —
 * there is exactly one — keeps working unchanged.
 */

/** What a compaction attempt did. `unavailable` means it was never attempted. */
export type CompactionStatus = "compacted" | "skipped" | "failed" | "unavailable";

export interface CompactionOutcome {
	readonly status: CompactionStatus;
	/**
	 * Stable, public-safe code explaining a non-`compacted` result. Never a raw
	 * runtime message: this value is logged and may reach an operator surface.
	 */
	readonly code: string;
}

export interface CompactionRequest {
	readonly sessionId: string;
	readonly originKey: string;
	readonly epoch: number;
}

export interface CompactionPort {
	/**
	 * Asks the runtime to compact this session NOW. Implementations must not
	 * throw: a failure is an outcome (`failed`), because the caller's decision
	 * table needs a status either way.
	 */
	run(request: CompactionRequest): Promise<CompactionOutcome>;
}

/** The code reported when no control channel exists to carry `compaction.run`. */
export const NO_CONTROL_CHANNEL = "no_control_channel_oneshot_cli";

/**
 * The default port: the gateway drives gjc as a one-shot CLI process, so there
 * is no control channel for `compaction.run`. Reporting `unavailable` is what
 * lets the caller distinguish "the runtime declined to compact" from "the
 * gateway never asked" — and it is what unlocks the last-resort digest roll.
 */
export class UnavailableCompactionPort implements CompactionPort {
	async run(_request: CompactionRequest): Promise<CompactionOutcome> {
		return { status: "unavailable", code: NO_CONTROL_CHANNEL };
	}
}
