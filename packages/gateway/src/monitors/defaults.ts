import type { GatewayDatabase } from "../store/db";
import type { MonitorRegistry } from "./registry";

/**
 * Default memory-maintenance monitors, seeded once per gateway database on
 * first boot (generic product behavior, not per-deployment setup):
 * - memory.canonicalize every 6 hours: consolidate daily captures into the
 *   canonical memory axes.
 * - memory.audit daily: run the structural validator and report.
 *
 * Seeding is once-only via a meta flag: an operator who removes a default is
 * never fought by the next boot.
 */
export const DEFAULT_MEMORY_MONITORS = [
	{
		name: "memory-canonicalize",
		trigger: { kind: "cron", schedule: "30 */6 * * *" } as const,
		eventTypes: ["memory.canonicalize"],
		burstPolicy: "dedupe" as const,
	},
	{
		name: "memory-audit",
		trigger: { kind: "cron", schedule: "0 6 * * *" } as const,
		eventTypes: ["memory.audit"],
		burstPolicy: "dedupe" as const,
	},
];

const SEED_FLAG = "default_monitors_seeded";

export function seedDefaultMonitors(registry: MonitorRegistry, database: GatewayDatabase): number {
	if (database.metaGet(SEED_FLAG)) return 0;
	const existingNames = new Set(registry.list().map((monitor) => monitor.name));
	let seeded = 0;
	for (const spec of DEFAULT_MEMORY_MONITORS) {
		if (existingNames.has(spec.name)) continue;
		registry.add(spec);
		seeded++;
	}
	database.metaSet(SEED_FLAG, new Date().toISOString());
	return seeded;
}
