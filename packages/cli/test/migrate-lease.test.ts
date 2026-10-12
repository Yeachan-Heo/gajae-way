import { expect, test } from "bun:test";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, leaseHeld } from "@gajae-gateway/home";
import { migrate } from "../src/migrate";
import { main } from "../src/main";


async function sourceHome(): Promise<string> {
	const source = await mkdtemp(join(tmpdir(), "gajaeway-migrate-source-"));
	await writeFile(join(source, "config.json"), JSON.stringify({ persona: { name: "lease-test" } }));
	return source;
}

test("migration refuses a held memory lease without changing target bytes", async () => {
	const source = await sourceHome();
	const target = await mkdtemp(join(tmpdir(), "gajaeway-migrate-held-"));
	await writeFile(join(target, "sentinel.json"), "keep these bytes");
	const holder = await acquireLease(target, "memory");
	try {
		await expect(migrate({ source, target })).rejects.toThrow(/lease_held name=memory .*; stop the gateway first/);

		expect(await readFile(join(target, "sentinel.json"), "utf8")).toBe("keep these bytes");
		expect((await readdir(target)).sort()).toEqual(["leases", "sentinel.json"]);
	} finally {
		await holder.release();
		await rm(source, { recursive: true, force: true });
		await rm(target, { recursive: true, force: true });
	}
});
test("the migrate command exits nonzero when the memory lease is held", async () => {
	const source = await sourceHome();
	const target = await mkdtemp(join(tmpdir(), "gajaeway-migrate-cli-held-"));
	await writeFile(join(target, "sentinel.json"), "unchanged");
	const holder = await acquireLease(target, "memory");
	const previousExitCode = process.exitCode;
	const originalError = console.error;
	const errors: string[] = [];
	try {
		console.error = (message: unknown) => errors.push(String(message));
		await main(["migrate", "--source", source, "--target", target]);
		expect(process.exitCode).toBe(1);
		expect(errors.join("\n")).toContain("stop the gateway first before importing memory data");
		expect(await readFile(join(target, "sentinel.json"), "utf8")).toBe("unchanged");
	} finally {
		console.error = originalError;
		process.exitCode = previousExitCode ?? 0;
		await holder.release();
		await rm(source, { recursive: true, force: true });
		await rm(target, { recursive: true, force: true });
	}
});

test("a free memory lease permits import and is released after success", async () => {
	const source = await sourceHome();
	const target = await mkdtemp(join(tmpdir(), "gajaeway-migrate-free-"));
	try {
		await migrate({ source, target });
		expect(JSON.parse(await readFile(join(target, "persona.json"), "utf8"))).toEqual({ name: "lease-test" });
		expect(await Bun.file(join(target, "memory", "axes.json")).exists()).toBe(true);
		expect(await leaseHeld(target, "memory")).toBe(false);
	} finally {
		await rm(source, { recursive: true, force: true });
		await rm(target, { recursive: true, force: true });
	}
});

test("migration releases the memory lease after an error partway through writing", async () => {
	const source = await sourceHome();
	const target = await mkdtemp(join(tmpdir(), "gajaeway-migrate-write-error-"));
	await writeFile(join(target, "memory"), "block the later axes merge");
	try {
		await expect(migrate({ source, target })).rejects.toThrow();
		expect(JSON.parse(await readFile(join(target, "persona.json"), "utf8"))).toEqual({ name: "lease-test" });
		expect(await leaseHeld(target, "memory")).toBe(false);
	} finally {
		await rm(source, { recursive: true, force: true });
		await rm(target, { recursive: true, force: true });
	}
});

test("dry-run takes no memory lease and writes nothing while another holder owns it", async () => {
	const source = await sourceHome();
	const target = await mkdtemp(join(tmpdir(), "gajaeway-migrate-dry-run-"));
	await writeFile(join(target, "sentinel.json"), "unchanged");
	const holder = await acquireLease(target, "memory");
	try {
		await migrate({ source, target, dryRun: true });
		expect(await leaseHeld(target, "memory")).toBe(true);
		expect(await readFile(join(target, "sentinel.json"), "utf8")).toBe("unchanged");
		expect((await readdir(target)).sort()).toEqual(["leases", "sentinel.json"]);
	} finally {
		await holder.release();
		await rm(source, { recursive: true, force: true });
		await rm(target, { recursive: true, force: true });
	}
});
