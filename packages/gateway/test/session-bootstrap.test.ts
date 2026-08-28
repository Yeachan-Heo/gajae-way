import { afterEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { buildSessionBootstrap, SESSION_BOOTSTRAP_MAX_BYTES } from "../src/persona/bootstrap";

const ORIGIN = { platform: "discord", kind: "channel", conversationId: "c1" } as const;
let home = "";

afterEach(async () => {
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function setup(): Promise<GatewayConfig> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-bootstrap-"));
	await mkdir(join(home, "workspace"), { recursive: true });
	await mkdir(join(home, "memory", "channels"), { recursive: true });
	return {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		channels: { c1: { engagement: "open", debounceMs: 25 } },
		mentionAllowlist: ["owner-2", "owner-1"],
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "owner", peerId: "owner" } },
	};
}

async function build(config: GatewayConfig, epoch = 0) {
	return buildSessionBootstrap({
		home,
		origin: ORIGIN,
		epoch,
		config,
		engagement: {
			mentioned: true,
			authorId: "owner-1",
			authorName: "Bellman",
			authorHandle: "@bellman",
			channelLabel: "#general",
			serverLabel: "GAJAE",
		},
		now: new Date("2026-08-28T12:00:00.000Z"),
	});
}

describe("session bootstrap builder", () => {
	test("builds stable metadata identity without exposing a public owner DM id", async () => {
		const config = await setup();
		const first = await build(config, 2);
		const second = await build(config, 2);
		expect(first.marker).toBe(second.marker);
		expect(first.text).toContain("origin: discord/channel/c1");
		expect(first.text).toContain("engagement-gate: open");
		expect(first.text).toContain("known-participant-ids: owner-1, owner-2");
		expect(first.text).toContain("owner-target: configured discord/dm; same-origin=false");
		expect(first.text).not.toContain("owner-target: discord/dm/owner");
	});

	test("includes only the explicitly associated public-safe channel record", async () => {
		const config = await setup();
		await writeFile(
			join(home, "memory", "channels", "current.md"),
			"origin: discord/channel/c1\nbootstrap-safe: public\nCurrent safe channel facts.",
		);
		await writeFile(
			join(home, "memory", "channels", "other.md"),
			"origin: discord/channel/other\nbootstrap-safe: public\nOTHER PRIVATE BODY",
		);
		const result = await build(config);
		expect(result.text).toContain("Current safe channel facts.");
		expect(result.text).not.toContain("OTHER PRIVATE BODY");
	});

	test("public channel rejects an associated record without explicit public approval", async () => {
		const config = await setup();
		await writeFile(join(home, "memory", "channels", "private.md"), "origin: discord/channel/c1\nprivate channel body");
		const result = await build(config);
		expect(result.text).not.toContain("private channel body");
		expect(result.diagnostics).toContain("current channel record: no explicitly associated safe file");
	});

	test("public MEMORY navigation omits global and other-origin private pointers", async () => {
		const config = await setup();
		await mkdir(join(home, "memory", "people"), { recursive: true });
		await writeFile(join(home, "memory", "people", "owner.md"), "private owner notes");
		await writeFile(
			join(home, "memory", "channels", "other.md"),
			"origin-key: discord/channel/other\nbootstrap-safe: public\nother room",
		);
		await writeFile(
			join(home, "memory", "MEMORY.md"),
			"# Navigation\n- [Owner](people/owner.md)\n- [Other room](channels/other.md)",
		);
		const result = await build(config);
		expect(result.text).not.toContain("people/owner.md");
		expect(result.text).not.toContain("channels/other.md");
		expect(result.diagnostics).toContain("public MEMORY links omitted: 2");
	});

	test("daily navigation includes only exact current-origin entries from supported layouts", async () => {
		const config = await setup();
		await mkdir(join(home, "memory", "daily", "2026-08"), { recursive: true });
		await writeFile(
			join(home, "memory", "daily", "2026-08", "2026-08-28.md"),
			'## now\n- origin: discord/channel/c1\n- user: current body\n\n## other\n- origin: {"platform":"discord","kind":"dm","conversationId":"secret","peerId":"p"}\n- user: DM SECRET',
		);
		const result = await build(config);
		expect(result.text).toContain("current body");
		expect(result.text).not.toContain("DM SECRET");
	});

	test("malicious MEMORY traversal and escaped symlink are diagnosed and never read", async () => {
		const config = await setup();
		const outside = join(home, "outside.md");
		await writeFile(outside, "ESCAPED SECRET");
		await symlink(outside, join(home, "memory", "channels", "escape.md"));
		await writeFile(
			join(home, "memory", "MEMORY.md"),
			"# Map\n- [bad](%2e%2e/outside.md)\n- [escape](channels/escape.md)",
		);
		const result = await build(config);
		expect(result.text).not.toContain("ESCAPED SECRET");
		expect(result.text).not.toContain("%2e%2e/outside.md");
		expect(result.diagnostics.some((item) => item.includes("path_traversal") || item.includes("symlink_escape"))).toBe(
			true,
		);
	});

	test("a safe first MEMORY link cannot smuggle a trailing private link on the same line", async () => {
		const config = await setup();
		await writeFile(
			join(home, "memory", "channels", "c1.md"),
			"origin-key: discord/channel/c1\nbootstrap-safe: public\ncurrent",
		);
		await writeFile(join(home, "outside.md"), "outside");
		await writeFile(join(home, "memory", "MEMORY.md"), "# Map\n- [safe](channels/c1.md) and [private](../outside.md)");
		const result = await build(config);
		expect(result.text).toContain("[safe](channels/c1.md)");
		expect(result.text).not.toContain("../outside.md");
		expect(result.text).not.toContain("[private]");
	});

	test("ops rules index emits only individually decoded and confined pointers", async () => {
		const config = await setup();
		await mkdir(join(home, "memory", "ops", "rules"), { recursive: true });
		await writeFile(join(home, "memory", "ops", "rules", "safe.md"), "safe rule");
		await writeFile(
			join(home, "memory", "ops", "rules", "index.md"),
			"# Rules\n- [safe](safe.md) and [escape](%2e%2e/%2e%2e/outside.md)",
		);
		const result = await build(config);
		expect(result.text).toContain("[safe](ops/rules/safe.md)");
		expect(result.text).not.toContain("%2e%2e/outside.md");
		expect(result.text).not.toContain("[escape]");
		expect(result.diagnostics).toContain("ops/rules/index.md links rejected: 1");
	});

	test("navigation headings and labels redact credential shapes while preserving safe paths", async () => {
		const config = await setup();
		await writeFile(
			join(home, "memory", "channels", "c1.md"),
			"origin-key: discord/channel/c1\nbootstrap-safe: public\ncurrent",
		);
		await mkdir(join(home, "memory", "ops", "rules"), { recursive: true });
		await writeFile(join(home, "memory", "ops", "rules", "safe.md"), "safe rule");
		await writeFile(
			join(home, "memory", "MEMORY.md"),
			"# password=hunter2\n- [api_key=map-token](channels/c1.md) secret=adjacent-map",
		);
		await writeFile(
			join(home, "memory", "ops", "rules", "index.md"),
			"# Authorization: Bearer rules-token\n- [token=rules-token](safe.md) password=adjacent-rules",
		);
		const result = await build(config);
		expect(result.text).toContain("channels/c1.md");
		expect(result.text).toContain("ops/rules/safe.md");
		expect(result.text).toContain("[REDACTED]");
		for (const secret of ["hunter2", "map-token", "adjacent-map", "rules-token", "adjacent-rules"])
			expect(result.text).not.toContain(secret);
	});

	test("a configured memory-root symlink to a canonical directory is allowed", async () => {
		const config = await setup();
		await rm(join(home, "memory"), { recursive: true, force: true });
		const canonical = join(home, "canonical-memory");
		await mkdir(canonical, { recursive: true });
		await writeFile(join(canonical, "MEMORY.md"), "# Navigation\n- [Rules](ops/rules/index.md)");
		await symlink(canonical, join(home, "memory"));
		const result = await build(config);
		expect(result.includedSections).toContain("Memory navigation map");
	});

	test("uses whole sections at the deterministic 8 KiB boundary with multibyte UTF-8 diagnostics", async () => {
		const config = await setup();
		await writeFile(join(home, "memory", "MEMORY.md"), `# Navigation\n- [${"한글".repeat(3000)}](channels/c1.md)`);
		await writeFile(
			join(home, "memory", "channels", "c1.md"),
			`origin: discord/channel/c1\nbootstrap-safe: public\n${"🦞".repeat(1800)}`,
		);
		const first = await build(config);
		const second = await build(config);
		expect(first.byteCount).toBeLessThanOrEqual(SESSION_BOOTSTRAP_MAX_BYTES);
		expect(Buffer.byteLength(first.text, "utf8")).toBe(first.byteCount);
		expect(first.text).toBe(second.text);
		expect(first.truncated).toBe(true);
		expect(first.text).toMatch(/omitted sections \(\d+\):/);
		expect(first.text).not.toContain("�");
	});

	test("re-reads source edits for a later epoch", async () => {
		const config = await setup();
		const channel = join(home, "memory", "channels", "c1.md");
		await writeFile(channel, "origin: discord/channel/c1\nbootstrap-safe: public\nversion one");
		expect((await build(config, 0)).text).toContain("version one");
		await writeFile(channel, "origin: discord/channel/c1\nbootstrap-safe: public\nversion two");
		const next = await build(config, 1);
		expect(next.text).toContain("version two");
		expect(next.text).not.toContain("version one");
	});
});
