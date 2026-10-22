import type { Store } from "../store";
import type { MonitorRegistry } from "./registry";

/**
 * Default memory-maintenance monitors, seeded once per gateway database on
 * first boot (generic product behavior, not per-deployment setup):
 * - memory.canonicalize every 6 hours: consolidate daily captures into the
 *   canonical memory axes.
 * - memory.audit daily: run the structural validator and report.
 * - memory.debloat daily: detect files exceeding the configured byte threshold
 *   and route them for restructuring.
 *
 * Seeding is per-name via meta flags: an operator who removes a default is
 * never fought by the next boot. Existing DBs with the legacy global flag
 * `default_monitors_seeded` are treated as having seeded the first two monitors
 * but not memory-debloat, so they receive it on the next boot.
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
	{
		name: "memory-debloat",
		trigger: { kind: "cron", schedule: "15 4 * * *" } as const,
		eventTypes: ["memory.debloat"],
		burstPolicy: "dedupe" as const,
	},
];

const SEED_FLAG = "default_monitors_seeded";
const SEED_FLAG_PREFIX = "default_monitor_seeded:";

export function seedDefaultMonitors(registry: MonitorRegistry, database: Store): number {
	const existingNames = new Set(registry.list().map((monitor) => monitor.name));
	const legacyFlag = database.core.metaGet(SEED_FLAG);
	let seeded = 0;

	for (const spec of DEFAULT_MEMORY_MONITORS) {
		if (existingNames.has(spec.name)) continue;

		// Skip if already seeded (per-name flag)
		const perNameFlag = `${SEED_FLAG_PREFIX}${spec.name}`;
		if (database.core.metaGet(perNameFlag)) continue;

		// For legacy DBs with the global flag, skip the first two monitors
		if (legacyFlag && spec.name !== "memory-debloat") {
			database.core.metaSet(perNameFlag, new Date().toISOString());
			continue;
		}

		registry.add(spec);
		database.core.metaSet(perNameFlag, new Date().toISOString());
		seeded++;
	}

	return seeded;
}
