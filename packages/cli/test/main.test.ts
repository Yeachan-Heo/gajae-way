import { describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgs, restoreDatabase, socketPath } from "../src/main";

describe("cli arguments", () => {
	test("resolves home and socket override", () => {
		expect(socketPath("/tmp/gajae")).toBe("/tmp/gajae/gateway.sock");
		expect(parseArgs(["--socket", "/tmp/x", "status"])).toEqual({ command: "status", rest: [], socket: "/tmp/x" });
	});
});

test("offline restore preserves the current database then copies a header-validated backup", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-restore-"));
	const previousHome = process.env.GAJAEWAY_HOME;
	process.env.GAJAEWAY_HOME = home;
	try {
		const databasePath = join(home, "gateway.db");
		const backupPath = join(home, "backup.db");
		await writeFile(databasePath, "current database");
		await writeFile(backupPath, Buffer.concat([Buffer.from("SQLite format 3\0"), Buffer.from(" backup")]));
		await restoreDatabase(join(home, "gateway.sock"), backupPath);
		expect(await Bun.file(databasePath).text()).toBe("SQLite format 3\0 backup");
		const preserved = (await Array.fromAsync(new Bun.Glob("gateway.db.pre-restore-*").scan({ cwd: home })))[0];
		expect(preserved).toBeString();
		expect(await Bun.file(join(home, preserved as string)).text()).toBe("current database");
	} finally {
		if (previousHome === undefined) delete process.env.GAJAEWAY_HOME;
		else process.env.GAJAEWAY_HOME = previousHome;
		await rm(home, { recursive: true, force: true });
	}
});
