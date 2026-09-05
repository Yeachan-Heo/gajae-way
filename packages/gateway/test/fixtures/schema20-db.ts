import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import type { GatewayDatabase } from "../../src/store/db";

/** Execute the actual pre-21 migrations, not a downgraded latest-schema database. */
export async function openSchema20(path: string): Promise<GatewayDatabase> {
	const sourcePath = resolve(import.meta.dir, "../../src/store/db.ts");
	let source = await Bun.file(sourcePath).text();
	const marker = "\t\tif (current < 21) {";
	if (source.split(marker).length !== 2) throw new Error("expected exactly one schema21 migration");
	const start = source.indexOf(marker);
	const end = source.indexOf("\n\t}\n", start);
	if (end < start) throw new Error("schema21 migration boundary missing");
	source = source.slice(0, start) + source.slice(end);
	if (source.split("const LATEST_SCHEMA_VERSION = 21;").length !== 2)
		throw new Error("schema version fixture boundary changed");
	source = source.replace("const LATEST_SCHEMA_VERSION = 21;", "const LATEST_SCHEMA_VERSION = 20;");
	source = source.replace(
		'"@gajaeway/protocol"',
		JSON.stringify(resolve(import.meta.dir, "../../../protocol/src/index.ts")),
	);
	source = source.replace(
		'"./epoch-mutation"',
		JSON.stringify(resolve(import.meta.dir, "../../src/store/epoch-mutation.ts")),
	);
	const directory = await mkdtemp(join(tmpdir(), "gw-schema20-module-"));
	try {
		const modulePath = join(directory, "db.ts");
		await Bun.write(modulePath, source);
		const module = await import(modulePath);
		return await module.GatewayDatabase.open(path);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
}
