import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	GATEWAY_UNIT,
	installServices,
	restartStack,
	restartStackCommands,
	serviceSpecs,
	systemdUnitName,
} from "../src/services";

async function configuredHome(): Promise<{ home: string; cleanup: () => Promise<void> }> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-services-"));
	await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }), "utf8");
	return { home, cleanup: () => rm(home, { recursive: true, force: true }) };
}

async function generate(platform: "darwin" | "linux") {
	const { home, cleanup } = await configuredHome();
	const written = new Map<string, string>();
	const paths = await installServices({
		binDir: join(home, "bin"),
		platform,
		unitDir: join(home, "units"),
		launchAgentsDir: join(home, "agents"),
		env: { HOME: home, GAJAEWAY_HOME: home, SHELL: "/bin/sh" },
		loginPath: undefined,
		loginPathRunner: () => "/usr/bin:/bin",
		writeFile: (path, contents) => {
			written.set(path, contents);
		},
	} as Parameters<typeof installServices>[0]);
	return { home, paths, written, cleanup };
}

test("a linux install writes one bound systemd user unit per service", async () => {
	const { home, paths, written, cleanup } = await generate("linux");
	try {
		expect(paths).toHaveLength(serviceSpecs().length);
		for (const spec of serviceSpecs()) {
			const unitPath = join(home, "units", systemdUnitName(spec));
			expect(paths).toContain(unitPath);
			const unit = written.get(unitPath) ?? "";
			expect(unit).toContain("[Service]");
			expect(unit).toContain(`Environment="GAJAEWAY_HOME=${home}"`);
			if (spec.dependsOnGateway) {
				// A gateway restart has to take the adapter with it; this is the whole point of #251.
				expect(unit).toContain(`BindsTo=${GATEWAY_UNIT}`);
				expect(unit).toContain(`After=${GATEWAY_UNIT}`);
				expect(unit).toContain(`PartOf=${GATEWAY_UNIT}`);
				expect(unit).toContain(`WantedBy=${GATEWAY_UNIT}`);
				expect(unit).not.toContain("KillMode=process");
				expect(unit).not.toContain("TimeoutStopSec=");
			} else {
				expect(unit).not.toContain("BindsTo=");
				expect(unit).not.toContain("PartOf=");
				expect(unit).toContain("WantedBy=default.target");
				// GJC daemon and session hosts share the gateway cgroup.
				expect(unit).toContain("KillMode=process");
				// The gateway's own shutdown ceiling (25s) must fit inside the unit's stop window (#225).
				expect(unit).toContain("TimeoutStopSec=30s");
			}
		}
	} finally {
		await cleanup();
	}
});

test("a darwin install still writes launch agents, not units", async () => {
	const { home, paths, cleanup } = await generate("darwin");
	try {
		expect(paths).toHaveLength(serviceSpecs().length);
		for (const path of paths) {
			expect(path.startsWith(join(home, "agents"))).toBe(true);
			expect(path.endsWith(".plist")).toBe(true);
		}
	} finally {
		await cleanup();
	}
});

test("an unsupported platform is refused by name", async () => {
	const { home, cleanup } = await configuredHome();
	try {
		await expect(
			installServices({
				binDir: join(home, "bin"),
				env: { HOME: home, GAJAEWAY_HOME: home },
				platform: "plan9" as never,
				writeFile: () => {},
			}),
		).rejects.toThrow("unsupported service platform: plan9");
	} finally {
		await cleanup();
	}
});

test("restart-stack is one systemctl call on linux and an ordered kickstart chain on darwin", () => {
	expect(restartStackCommands("linux")).toEqual([["systemctl", "--user", "restart", GATEWAY_UNIT]]);

	const darwin = restartStackCommands("darwin", 501);
	expect(darwin[0]).toEqual(["launchctl", "kickstart", "-k", "gui/501/dev.gajaeway.gateway"]);
	expect(darwin).toHaveLength(serviceSpecs().length);
	for (const spec of serviceSpecs().filter((candidate) => candidate.dependsOnGateway))
		expect(darwin).toContainEqual(["launchctl", "kickstart", "-k", `gui/501/${spec.label}`]);
	// bootout removes the job and leaves the bot offline with no automatic recovery.
	expect(darwin.flat()).not.toContain("bootout");
});

test("restartStack stops at the first failing command and names it", async () => {
	const ran: string[][] = [];
	await expect(
		restartStack({
			platform: "darwin",
			uid: 501,
			runner: (command) => {
				ran.push([...command]);
				return command.includes("gui/501/dev.gajaeway.adapter-discord") ? 3 : 0;
			},
		}),
	).rejects.toThrow("restart-stack failed with status 3: launchctl kickstart -k gui/501/dev.gajaeway.adapter-discord");
	// The gateway ran, the failing adapter ran, and nothing after it did.
	expect(ran[0]).toEqual(["launchctl", "kickstart", "-k", "gui/501/dev.gajaeway.gateway"]);
	expect(ran).toHaveLength(2);
});

test("a successful restart returns every command it ran, in order", async () => {
	const commands = await restartStack({ platform: "linux", runner: () => 0 });
	expect(commands).toEqual([["systemctl", "--user", "restart", GATEWAY_UNIT]]);
});
