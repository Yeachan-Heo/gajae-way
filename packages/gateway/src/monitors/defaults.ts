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

/** Former once-for-all flag: a gateway that set it seeded only the first two defaults. */
const LEGACY_SEED_FLAG = "default_monitors_seeded";
const LEGACY_SEEDED = new Set(["memory-canonicalize", "memory-audit"]);
const seedFlag = (name: string) => `default_monitor_seeded:${name}`;

/**
 * Seeds each default at most once per database. A default that already exists
 * (operator-created under the same name) is recorded as seeded, so removing it
 * later is final too.
 */
export function seedDefaultMonitors(registry: MonitorRegistry, database: Store): number {
	const existingNames = new Set(registry.list().map((monitor) => monitor.name));
	const legacy = database.core.metaGet(LEGACY_SEED_FLAG) !== undefined;
	const at = new Date().toISOString();
	let seeded = 0;
	for (const spec of DEFAULT_MEMORY_MONITORS) {
		const flag = seedFlag(spec.name);
		if (database.core.metaGet(flag)) continue;
		if (!existingNames.has(spec.name) && !(legacy && LEGACY_SEEDED.has(spec.name))) {
			registry.add(spec);
			seeded++;
		}
		database.core.metaSet(flag, at);
	}
	return seeded;
}
