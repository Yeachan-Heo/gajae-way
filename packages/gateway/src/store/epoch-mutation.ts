export const EPOCH_MUTATION_REASONS = [
	"operator_new",
	"monitor_context_roll",
	"create_key_poisoned",
	"steer_refused_session_broken",
	"session_disowned_dead",
	"credential_stale",
	"execution_uncertain_fence_expired",
] as const;
export type EpochMutationReason = (typeof EPOCH_MUTATION_REASONS)[number];
export type EpochScope = "persona" | "monitor" | "work";
export interface EpochMutationInput {
	scope: EpochScope;
	reason: EpochMutationReason;
	opRef?: string;
	cause: { kind: "retirement" | "audit" | "operator" | "policy"; ref?: string };
	actor?: string;
	/** Present for explicit resets; recovery preserves the stored origin payload. */
	originRefJson?: string;
}
export interface EpochMutationRow {
	id: number;
	originKey: string;
	scope: EpochScope;
	fromEpoch: number;
	toEpoch: number;
	fromSessionId: string | null;
	brokerGeneration: string | null;
	reason: EpochMutationReason;
	opRef: string | null;
	causeKind: EpochMutationInput["cause"]["kind"];
	causeRef: string | null;
	actor: string | null;
	at: string;
}
export interface HoldRow {
	opRef: string | null;
	originKey: string;
	epoch: number | null;
	state: "bound" | "accepted" | "done";
	reason: string | null;
	since: string | null;
	deadline: string | null;
	sweeps: number;
	fence: { opRef: string; since: string | null; deadline: string | null } | null;
}
