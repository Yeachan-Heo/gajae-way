import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BROKER_UNIT, GATEWAY_UNIT, installServices, restartStackCommands, serviceSpecs, systemdUnitName } from "../src/services";

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
			if (spec.id === "broker") {
				expect(unit).toContain(`ConditionPathExists=${join(home, "broker", "provision", "manifest.json")}\n`);
				expect(unit).toContain(`EnvironmentFile=-${join(home, "broker.env")}\n`);
				expect(unit).toContain("Restart=always");
				expect(unit).toContain("RestartSec=2s");
				expect(unit).toContain("KillMode=control-group");
				expect(unit).toContain("SuccessExitStatus=143\n");
				expect(unit).toContain("TimeoutStopSec=30s");
			} else if (spec.id === "gateway") {
				expect(unit).toContain(`Wants=${BROKER_UNIT}`);
				expect(unit).toContain(`After=${BROKER_UNIT}`);
				expect(unit).not.toContain("Requires=");
				expect(unit).toContain("KillMode=mixed");
				expect(unit).toContain("TimeoutStopSec=30s");
			} else {
				expect(unit).toContain(`BindsTo=${GATEWAY_UNIT}`);
				expect(unit).toContain(`After=${GATEWAY_UNIT}`);
				expect(unit).toContain(`PartOf=${GATEWAY_UNIT}`);
				expect(unit).toContain(`WantedBy=${GATEWAY_UNIT}`);
				expect(unit).not.toContain("KillMode=process");
				expect(unit).not.toContain("TimeoutStopSec=");
			}
		}
	} finally {
		await cleanup();
	}
});

test("a darwin install still writes launch agents, not units", async () => {
	const { home, paths, written, cleanup } = await generate("darwin");
	try {
		expect(paths).toHaveLength(serviceSpecs().length);
		for (const path of paths) {
			expect(path.startsWith(join(home, "agents"))).toBe(true);
			expect(path.endsWith(".plist")).toBe(true);
			const contents = [...written.values()].find((candidate) => candidate.includes(path.split("/").at(-1)?.replace(".plist", "") ?? ""));
			expect(contents).toBeDefined();
			expect(contents).toContain("<key>RunAtLoad</key><true/>");
			expect(contents).toContain("<key>KeepAlive</key><true/>");
			expect(contents).not.toContain("AbandonProcessGroup");
		}
	} finally {
		await cleanup();
	}
});

test("compareAndWrite leaves unchanged service definitions untouched", async () => {
	const { home, cleanup } = await configuredHome();
	const unitDir = join(home, "units");
	let writes = 0;
	const write = async (path: string, contents: string) => {
		writes++;
		await writeFile(path, contents, "utf8");
	};
	const options = {
		binDir: join(home, "bin"),
		unitDir,
		platform: "linux" as const,
		env: { HOME: home, GAJAEWAY_HOME: home },
		loginPathRunner: () => "/usr/bin:/bin",
		writeFile: write,
	};
	try {
		await installServices(options);
		expect(writes).toBe(serviceSpecs().length);
		writes = 0;
		await installServices({ ...options, compareAndWrite: true });
		expect(writes).toBe(0);
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
	expect(restartStackCommands("linux")).toEqual([
		["systemctl", "--user", "restart", BROKER_UNIT],
		["systemctl", "--user", "restart", GATEWAY_UNIT],
	]);

	const darwin = restartStackCommands("darwin", 501);
	expect(darwin[0]).toEqual(["launchctl", "kickstart", "-k", "gui/501/dev.gajaeway.broker"]);
	expect(darwin[1]).toEqual(["launchctl", "kickstart", "-k", "gui/501/dev.gajaeway.gateway"]);
	expect(darwin).toHaveLength(serviceSpecs().length);
	for (const spec of serviceSpecs().filter((candidate) => candidate.dependsOnGateway))
		expect(darwin).toContainEqual(["launchctl", "kickstart", "-k", `gui/501/${spec.label}`]);
	// bootout removes the job and leaves the bot offline with no automatic recovery.
	expect(darwin.flat()).not.toContain("bootout");
});
