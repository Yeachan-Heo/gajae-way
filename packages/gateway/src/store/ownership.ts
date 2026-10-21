export type OwnerGroup = "core" | "bindings" | "outbox" | "inbox" | "work" | "monitors" | "memory" | "journal";

export const TABLE_OWNER = {
	meta: "core",
	schema_migrations: "core",
	sessions: "bindings",
	broker_owned_bindings: "bindings",
	broker_tail_cursors: "bindings",
	session_tail_cursors: "bindings",
	conversation_model: "bindings",
	broker_authority: "bindings",
	broker_retired_sessions: "bindings",
	broker_quarantine: "bindings",
	broker_cutovers: "bindings",
	broker_operations: "bindings",
	deliveries: "outbox",
	inbound_messages: "inbox",
	thread_mutes: "inbox",
	cross_session_watches: "inbox",
	conversation_context: "inbox",
	conversation_context_state: "inbox",
	recall_snippets: "inbox",
	lane_jobs: "work",
	work_attempt_runtime: "work",
	lane_reports: "work",
	monitors: "monitors",
	monitor_events: "monitors",
	authored_outputs: "monitors",
	monitor_failures: "monitors",
	monitor_slots: "monitors",
	dispatch_leases: "monitors",
	memory_intents: "memory",
	work_journal: "journal",
} as const satisfies Record<string, OwnerGroup>;

export type OwnedTable = keyof typeof TABLE_OWNER;

export const OWNER_FILE: Readonly<Record<OwnerGroup, string>> = {
	core: "store/core.ts",
	bindings: "store/groups/bindings.ts",
	outbox: "store/groups/outbox.ts",
	inbox: "store/groups/inbox.ts",
	work: "store/groups/work.ts",
	monitors: "store/groups/monitors.ts",
	memory: "store/groups/memory.ts",
	journal: "store/groups/journal.ts",
};
