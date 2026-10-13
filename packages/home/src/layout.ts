import { join } from "node:path";

/** Every resource that a single lease owns. One process may hold several, acquired in a fixed order. */
export type LeaseName =
	| "gateway"
	| "broker"
	| "update"
	| "memory"
	| "adapter-discord"
	| "adapter-slack"
	| "adapter-telegram";

export const LEASE_NAMES: readonly LeaseName[] = [
	"gateway",
	"broker",
	"update",
	"memory",
	"adapter-discord",
	"adapter-slack",
	"adapter-telegram",
];

export interface HomeLayout {
	readonly home: string;
	readonly leasesDir: string;
	lease(name: LeaseName): string;
	readonly brokerAgentDir: string;
	readonly brokerProvisionDir: string;
	readonly brokerProvisionManifest: string;
	readonly backupsDir: string;
	readonly gjcPrefix: string;
	readonly gjcCurrent: string;
	gjcExecutable(version?: string): string;
	readonly socket: string;
	readonly db: string;
}

/** The single source of truth for paths under $GAJAEWAY_HOME. */
export function homeLayout(home: string): HomeLayout {
	const leasesDir = join(home, "leases");
	const brokerDir = join(home, "broker");
	const brokerProvisionDir = join(brokerDir, "provision");
	const gjcPrefix = join(home, "gjc");
	const gjcCurrent = join(gjcPrefix, "current");
	return {
		home,
		leasesDir,
		lease: (name) => join(leasesDir, name),
		brokerAgentDir: join(brokerDir, "agent"),
		brokerProvisionDir,
		brokerProvisionManifest: join(brokerProvisionDir, "manifest.json"),
		backupsDir: join(home, "backups"),
		gjcPrefix,
		gjcCurrent,
		gjcExecutable: (version) => join(version === undefined ? gjcCurrent : join(gjcPrefix, version), "bin", "gjc"),
		socket: join(home, "gateway.sock"),
		db: join(home, "gateway.db"),
	};
}
