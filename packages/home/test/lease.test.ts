import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { chmod, link, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, homeLayout, LeaseError, LeaseHeldError, leaseHeld, readLeaseOwner } from "../src/index";

let home: string;
beforeEach(async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-home-"));
});
afterEach(async () => {
	await chmod(home, 0o700).catch(() => {});
	await rm(home, { recursive: true, force: true });
});

describe("acquireLease", () => {
	test("writes the owner record and refuses a second holder with that owner", async () => {
		const lease = await acquireLease(home, "gateway");
		const record = JSON.parse(await readFile(homeLayout(home).lease("gateway"), "utf8"));
		expect(record).toEqual({ pid: process.pid, token: lease.token, startedAt: expect.any(String) });

		const second = await acquireLease(home, "gateway").catch((error: unknown) => error);
		expect(second).toBeInstanceOf(LeaseHeldError);
		expect((second as LeaseHeldError).code).toBe("lease_held");
		expect((second as LeaseHeldError).owner?.token).toBe(lease.token);
		await lease.release();
	});

	test("release then re-acquire succeeds and leaves the file in place", async () => {
		const first = await acquireLease(home, "update");
		await first.release();
		await first.release();
		const second = await acquireLease(home, "update");
		expect(second.token).not.toBe(first.token);
		expect((await readLeaseOwner(home, "update"))?.token).toBe(second.token);
		await second[Symbol.asyncDispose]();
	});

	test("distinct names are independent", async () => {
		const gateway = await acquireLease(home, "gateway");
		const memory = await acquireLease(home, "memory");
		await memory.release();
		await gateway.release();
	});

	test("refuses a symlinked lease file", async () => {
		await mkdir(homeLayout(home).leasesDir, { recursive: true });
		await writeFile(join(home, "target"), "");
		await symlink(join(home, "target"), homeLayout(home).lease("broker"));
		const error = await acquireLease(home, "broker").catch((e: unknown) => e);
		expect(error).toBeInstanceOf(LeaseError);
		expect((error as LeaseError).code).toBe("lease_failed");
		expect((error as LeaseError).syscall).toBe("open");
	});

	test("refuses a non-regular file and a hard-linked file", async () => {
		const layout = homeLayout(home);
		await mkdir(layout.lease("memory"), { recursive: true });
		const directory = await acquireLease(home, "memory").catch((e: unknown) => e);
		expect(directory).toBeInstanceOf(LeaseError);

		await writeFile(join(home, "other"), "");
		await link(join(home, "other"), layout.lease("broker"));
		const linked = await acquireLease(home, "broker").catch((e: unknown) => e);
		expect(linked).toBeInstanceOf(LeaseError);
		expect((linked as LeaseError).errno).toBe("not_regular_file");
	});

	test("a non-contention failure is lease_failed with the original errno, not lease_held", async () => {
		await mkdir(homeLayout(home).leasesDir, { recursive: true });
		await chmod(homeLayout(home).leasesDir, 0o500);
		const error = await acquireLease(home, "gateway").catch((e: unknown) => e);
		expect(error).toBeInstanceOf(LeaseError);
		expect(error).not.toBeInstanceOf(LeaseHeldError);
		expect((error as LeaseError).errno).toBe("EACCES");
	});
});

describe("leaseHeld", () => {
	test("is false for a missing or free lease and true while held, without disturbing the holder", async () => {
		expect(await leaseHeld(home, "gateway")).toBe(false);
		const lease = await acquireLease(home, "gateway");
		expect(await leaseHeld(home, "gateway")).toBe(true);
		expect(await leaseHeld(home, "gateway")).toBe(true);
		expect(await acquireLease(home, "gateway").catch((e: unknown) => e)).toBeInstanceOf(LeaseHeldError);
		await lease.release();
		expect(await leaseHeld(home, "gateway")).toBe(false);
	});
});

describe("readLeaseOwner", () => {
	test("returns undefined for missing or malformed records", async () => {
		expect(await readLeaseOwner(home, "gateway")).toBeUndefined();
		await mkdir(homeLayout(home).leasesDir, { recursive: true });
		await writeFile(homeLayout(home).lease("gateway"), "{not json");
		expect(await readLeaseOwner(home, "gateway")).toBeUndefined();
	});
});
