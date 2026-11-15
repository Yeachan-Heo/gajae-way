// Covers the repo-root scripts/release-packages.ts. It lives under packages/ because
// CI runs `bun test packages/` and `biome ci packages/`; a test under scripts/ would
// never run. It lives in packages/conformance (the repo-wide gate package) rather than
// in a consumer package because sdk-boundary-dogfood forbids a consumer package from
// importing outside itself. The fixture copies the real script into a temp dir and
// shadows only the bun/npm subprocesses, so no test reaches a real registry.
import { describe, expect, test } from "bun:test";
import { chmod, copyFile, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, delimiter, join } from "node:path";
import { assertNoPrivateWorkspaceDeps, parseNpmViewResult, RELEASE_ORDER } from "../../../scripts/release-packages";

const RELEASE_PACKAGE_NAMES = new Set(["@gajae-gateway/protocol", "@gajae-gateway/images", "@gajae-gateway/sdk"]);
const RELEASE_PACKAGE_NAMES_WITH_PRIVATE = new Set([...RELEASE_PACKAGE_NAMES, "@gajae-gateway/home"]);

const WORKSPACE_PACKAGES = new Map([
	["@gajae-gateway/protocol", { name: "@gajae-gateway/protocol", private: false }],
	["@gajae-gateway/images", { name: "@gajae-gateway/images", private: false }],
	["@gajae-gateway/sdk", { name: "@gajae-gateway/sdk", private: false }],
	["@gajae-gateway/home", { name: "@gajae-gateway/home", private: true }],
	["@gajae-gateway/log", { name: "@gajae-gateway/log", private: false }],
]);

const PRIVATE_WORKSPACE_DEPENDENCY_FIXTURES = [
	{
		section: "dependencies",
		manifest: { name: "@gajae-gateway/cli", dependencies: { "@gajae-gateway/home": "workspace:*" } },
	},
	{
		section: "optionalDependencies",
		manifest: { name: "@gajae-gateway/cli", optionalDependencies: { "@gajae-gateway/home": "workspace:*" } },
	},
	{
		section: "peerDependencies",
		manifest: { name: "@gajae-gateway/cli", peerDependencies: { "@gajae-gateway/home": "workspace:*" } },
	},
] as const;

describe("release package workspace dependency guard", () => {
	test("releases images after protocol and before dependent packages", () => {
		expect(RELEASE_ORDER).toEqual(["protocol", "images", "sdk", "cli"]);
	});

	test("allows dependencies on released workspace packages and external packages", () => {
		const manifest = {
			name: "@gajae-gateway/cli",
			dependencies: {
				"@gajae-gateway/protocol": "0.1.8",
				"@gajae-gateway/images": "0.1.0",
				"@gajae-gateway/sdk": "0.1.8",
				kleur: "^4.1.5",
			},
			optionalDependencies: { "optional-external": "^1.0.0" },
			peerDependencies: { "peer-external": "^2.0.0" },
		};

		expect(() => assertNoPrivateWorkspaceDeps(manifest, WORKSPACE_PACKAGES, RELEASE_PACKAGE_NAMES)).not.toThrow();
	});

	for (const fixture of PRIVATE_WORKSPACE_DEPENDENCY_FIXTURES) {
		test(`rejects private workspace dependencies in ${fixture.section}`, () => {
			expect(() =>
				assertNoPrivateWorkspaceDeps(fixture.manifest, WORKSPACE_PACKAGES, RELEASE_PACKAGE_NAMES_WITH_PRIVATE),
			).toThrow("@gajae-gateway/home");
		});
	}

	test("rejects workspace packages missing from the release map even if they are not private", () => {
		const manifest = {
			name: "@gajae-gateway/cli",
			dependencies: { "@gajae-gateway/log": "0.1.0" },
		};

		expect(() => assertNoPrivateWorkspaceDeps(manifest, WORKSPACE_PACKAGES, RELEASE_PACKAGE_NAMES)).toThrow(
			"not in the release map",
		);
	});
});

describe("npm registry checks", () => {
	test("parseNpmViewResult detects already published version", () => {
		const result = parseNpmViewResult("0.5.0\n", "", 0, "0.5.0");
		expect(result).toEqual({ alreadyPublished: true });
	});

	test("parseNpmViewResult handles version not found (404)", () => {
		const result = parseNpmViewResult("", "404 Not Found", 1, "0.5.0");
		expect(result).toEqual({ alreadyPublished: false });
	});

	test("parseNpmViewResult reports other registry errors", () => {
		const result = parseNpmViewResult("", "ENEEDAUTH: no trusted publisher exists", 1, "0.5.0");
		expect(result).toEqual({
			alreadyPublished: false,
			error: "ENEEDAUTH: no trusted publisher exists",
		});
	});

	test("parseNpmViewResult reports unexpected version from npm", () => {
		const result = parseNpmViewResult("0.4.0\n", "", 0, "0.5.0");
		expect(result.alreadyPublished).toBe(false);
		expect(result.error).toContain("unexpected version");
	});

	test("parseNpmViewResult reports error when both stdout and stderr are present", () => {
		const result = parseNpmViewResult("some output", "E_TIMEOUT", 1, "0.5.0");
		expect(result).toEqual({
			alreadyPublished: false,
			error: "E_TIMEOUT",
		});
	});
});

const RELEASE_FIXTURES = [
	{ directory: "protocol", name: "@gajae-gateway/protocol", version: "1.2.3" },
	{ directory: "images", name: "@gajae-gateway/images", version: "2.3.4" },
	{ directory: "sdk", name: "@gajae-gateway/sdk", version: "3.4.5" },
	{ directory: "cli", name: "@gajae-gateway/cli", version: "4.5.6" },
] as const;

interface RecordedCommand {
	tool: string;
	args: string[];
	cwd: string;
}

// The child process runs a copy of the real release script. Only its bun and
// npm subprocesses are replaced; packing and reading the tarball use real tar.
const MOCK_COMMAND = `
import { appendFileSync, copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { spawnSync } from "node:child_process";

const [tool, ...args] = process.argv.slice(2);
appendFileSync(process.env.COMMAND_LOG, JSON.stringify({ tool, args, cwd: basename(process.cwd()) }) + "\\n");
if (tool === "bun") {
  if (args.join(" ") === "run build") process.exit(0);
  if (args[0] === "pm" && args[1] === "pack" && args[2] === "--filename" && args.length === 4) {
    const staging = join(process.env.FIXTURE_ROOT, "staging", basename(process.cwd()), "package");
    mkdirSync(staging, { recursive: true });
    copyFileSync("package.json", join(staging, "package.json"));
    const result = spawnSync("tar", ["-czf", args[3], "-C", join(staging, ".."), "package"], { stdio: "inherit" });
    process.exit(result.status ?? 1);
  }
} else if (tool === "npm") {
  if (args[0] === "view" && args.length === 3 && args[2] === "version") {
    if (args[1] === process.env.REGISTRY_ERROR) {
      console.error("simulated registry failure");
      process.exit(1);
    }
    const tags = JSON.parse(readFileSync(process.env.TAG_STATE, "utf8"));
    if (Object.hasOwn(tags, args[1])) {
      console.log(tags[args[1]]);
      process.exit(0);
    }
    if (JSON.parse(process.env.EXISTING_PACKAGES).includes(args[1])) {
      console.log(args[1].slice(args[1].lastIndexOf("@") + 1));
      process.exit(0);
    }
    console.error("404 Not Found");
    process.exit(1);
  }
  if (args.join(" ") === "config get tag") {
    if (process.env.CONFIG_ERROR) {
      console.error("simulated config failure");
      process.exit(1);
    }
    console.log(process.env.CONFIG_TAG);
    process.exit(0);
  }
  if (args[0] === "dist-tag" && args[1] === "add") {
    if (args[2] === process.env.DIST_TAG_ERROR) {
      console.error("simulated dist-tag failure");
      process.exit(1);
    }
    const tags = JSON.parse(readFileSync(process.env.TAG_STATE, "utf8"));
    const split = args[2].lastIndexOf("@");
    tags[args[2].slice(0, split) + "@" + args[3]] = process.env.INTERLEAVE_VERSION || args[2].slice(split + 1);
    writeFileSync(process.env.TAG_STATE, JSON.stringify(tags));
    process.exit(0);
  }
  if (args[0] === "publish") {
    if (process.env.PUBLISH_ERROR && args[1].endsWith(process.env.PUBLISH_ERROR)) {
      console.error("simulated publish failure");
      process.exit(1);
    }
    process.exit(0);
  }
}
console.error("unexpected mock command", tool, ...args);
process.exit(1);
`;

async function withReleaseFixture(
	options: {
		existing?: string[];
		registryError?: string;
		distTagError?: string;
		publishError?: string;
		configTag?: string;
		configError?: boolean;
		tags?: Record<string, string>;
		publishTags?: Record<string, string>;
		versions?: Record<string, string>;
		interleave?: string;
	},
	run: (fixture: {
		root: string;
		execute: (...args: string[]) => Promise<{ code: number; output: string; commands: RecordedCommand[] }>;
	}) => Promise<void>,
): Promise<void> {
	// Bun resolves symlinks in import.meta.url, so the copied script always reports the
	// canonical temp root. Canonicalize here too, or macOS (/var -> /private/var) compares
	// the script's realpath output against an unresolved expectation.
	const root = await realpath(await mkdtemp(join(tmpdir(), "release-packages-test-")));
	try {
		const bin = join(root, "bin");
		await mkdir(bin);
		await mkdir(join(root, "scripts"));
		await copyFile(
			new URL("../../../scripts/release-packages.ts", import.meta.url),
			join(root, "scripts", "release-packages.ts"),
		);
		await writeFile(join(root, "mock-command.js"), MOCK_COMMAND);
		for (const tool of ["bun", "npm"]) {
			const executable = join(bin, tool);
			await writeFile(executable, `#!/bin/sh\nexec "$REAL_BUN" "$FIXTURE_ROOT/mock-command.js" ${tool} "$@"\n`);
			await chmod(executable, 0o755);
		}
		for (const pkg of RELEASE_FIXTURES) {
			const dir = join(root, "packages", pkg.directory);
			await mkdir(dir, { recursive: true });
			await writeFile(
				join(dir, "package.json"),
				JSON.stringify({
					name: pkg.name,
					version: options.versions?.[pkg.name] ?? pkg.version,
					...(options.publishTags?.[pkg.name] ? { publishConfig: { tag: options.publishTags[pkg.name] } } : {}),
				}),
			);
		}
		const log = join(root, "commands.jsonl");
		await writeFile(log, "");
		const tagState = join(root, "tags.json");
		await writeFile(tagState, JSON.stringify(options.tags ?? {}));
		await run({
			root,
			execute: async (...args) => {
				const proc = Bun.spawn([process.execPath, join(root, "scripts", "release-packages.ts"), ...args], {
					cwd: root,
					env: {
						...process.env,
						PATH: `${bin}${delimiter}${process.env.PATH ?? ""}`,
						REAL_BUN: process.execPath,
						FIXTURE_ROOT: root,
						COMMAND_LOG: log,
						EXISTING_PACKAGES: JSON.stringify(options.existing ?? []),
						REGISTRY_ERROR: options.registryError ?? "",
						DIST_TAG_ERROR: options.distTagError ?? "",
						PUBLISH_ERROR: options.publishError ?? "",
						TAG_STATE: tagState,
						CONFIG_TAG: options.configTag ?? "latest",
						CONFIG_ERROR: options.configError ? "1" : "",
						INTERLEAVE_VERSION: options.interleave ?? "",
					},
					stdout: "pipe",
					stderr: "pipe",
				});
				const [stdout, stderr, code] = await Promise.all([
					new Response(proc.stdout).text(),
					new Response(proc.stderr).text(),
					proc.exited,
				]);
				const lines = (await readFile(log, "utf8")).trim();
				return {
					code,
					output: stdout + stderr,
					commands: lines ? lines.split("\n").map((line) => JSON.parse(line)) : [],
				};
			},
		});
	} finally {
		await rm(root, { recursive: true, force: true });
	}
}

function packageRef(pkg: (typeof RELEASE_FIXTURES)[number]): string {
	return `${pkg.name}@${pkg.version}`;
}

describe("release script subprocess behavior", () => {
	for (const tag of ["next", undefined]) {
		test(`resumes existing packages and publishes fresh packages with ${tag ?? "default latest"} tag`, async () => {
			const existing = [packageRef(RELEASE_FIXTURES[0]), packageRef(RELEASE_FIXTURES[2])];
			await withReleaseFixture({ existing }, async ({ root, execute }) => {
				const result = await execute("--publish", ...(tag ? ["--tag", tag] : []));
				expect(result.code).toBe(0);
				const expected: RecordedCommand[] = [];
				for (const pkg of RELEASE_FIXTURES) {
					const tarball = join(root, "dist-packed", `gajae-gateway-${pkg.directory}.tgz`);
					expected.push({ tool: "bun", args: ["run", "build"], cwd: pkg.directory });
					expected.push({ tool: "bun", args: ["pm", "pack", "--filename", tarball], cwd: pkg.directory });
					expected.push({ tool: "npm", args: ["view", packageRef(pkg), "version"], cwd: basename(root) });
					if (existing.includes(packageRef(pkg))) {
						if (!tag) expected.push({ tool: "npm", args: ["config", "get", "tag"], cwd: pkg.directory });
						expected.push({
							tool: "npm",
							args: ["view", `${pkg.name}@${tag ?? "latest"}`, "version"],
							cwd: pkg.directory,
						});
						expected.push({
							tool: "npm",
							args: ["dist-tag", "add", packageRef(pkg), tag ?? "latest"],
							cwd: pkg.directory,
						});
						expected.push({
							tool: "npm",
							args: ["view", `${pkg.name}@${tag ?? "latest"}`, "version"],
							cwd: pkg.directory,
						});
					} else {
						expected.push({
							tool: "npm",
							args: ["publish", tarball, "--access", "public", ...(tag ? ["--tag", tag] : [])],
							cwd: pkg.directory,
						});
					}
				}
				expect(result.commands).toEqual(expected);
			});
		}, 30_000);
	}

	test("dry-run builds and packs every package without calling npm", async () => {
		await withReleaseFixture({}, async ({ root, execute }) => {
			const result = await execute("--tag", "next");
			expect(result.code).toBe(0);
			expect(result.commands).toEqual(
				RELEASE_FIXTURES.flatMap((pkg) => [
					{ tool: "bun", args: ["run", "build"], cwd: pkg.directory },
					{
						tool: "bun",
						args: ["pm", "pack", "--filename", join(root, "dist-packed", `gajae-gateway-${pkg.directory}.tgz`)],
						cwd: pkg.directory,
					},
				]),
			);
		});
	}, 30_000);

	test("registry check failure stops the release before publishing that package", async () => {
		const failed = packageRef(RELEASE_FIXTURES[1]);
		await withReleaseFixture({ registryError: failed }, async ({ root, execute }) => {
			const result = await execute("--publish");
			expect(result.code).not.toBe(0);
			expect(result.output).toContain("simulated registry failure");
			expect(result.commands.filter((command) => command.tool === "npm").map((command) => command.args)).toEqual([
				["view", packageRef(RELEASE_FIXTURES[0]), "version"],
				["publish", join(root, "dist-packed", "gajae-gateway-protocol.tgz"), "--access", "public"],
				["view", failed, "version"],
			]);
			expect(result.commands.some((command) => command.cwd === "sdk")).toBe(false);
		});
	}, 30_000);

	test("dist-tag failure stops the release before subsequent packages", async () => {
		const failed = packageRef(RELEASE_FIXTURES[1]);
		await withReleaseFixture({ existing: [failed], distTagError: failed }, async ({ root, execute }) => {
			const result = await execute("--publish", "--tag", "next");
			expect(result.code).not.toBe(0);
			expect(result.output).toContain("simulated dist-tag failure");
			expect(result.commands.filter((command) => command.tool === "npm").map((command) => command.args)).toEqual([
				["view", packageRef(RELEASE_FIXTURES[0]), "version"],
				["publish", join(root, "dist-packed", "gajae-gateway-protocol.tgz"), "--access", "public", "--tag", "next"],
				["view", failed, "version"],
				["view", `${RELEASE_FIXTURES[1].name}@next`, "version"],
				["dist-tag", "add", failed, "next"],
			]);
			expect(result.output).toMatch(/OIDC|trusted publish/i);
			expect(result.output).toContain("Allow npm dist-tag");
			expect(result.output).toContain(failed);
			expect(result.commands.some((command) => command.cwd === "sdk")).toBe(false);
		});
	}, 30_000);

	test("publish failure stops the release before subsequent packages", async () => {
		await withReleaseFixture({ publishError: "gajae-gateway-images.tgz" }, async ({ root, execute }) => {
			const result = await execute("--publish", "--tag", "next");
			expect(result.code).not.toBe(0);
			expect(result.output).toContain("simulated publish failure");
			expect(result.commands.filter((command) => command.tool === "npm").map((command) => command.args)).toEqual([
				["view", packageRef(RELEASE_FIXTURES[0]), "version"],
				["publish", join(root, "dist-packed", "gajae-gateway-protocol.tgz"), "--access", "public", "--tag", "next"],
				["view", packageRef(RELEASE_FIXTURES[1]), "version"],
				["publish", join(root, "dist-packed", "gajae-gateway-images.tgz"), "--access", "public", "--tag", "next"],
			]);
			expect(result.commands.some((command) => command.cwd === "sdk")).toBe(false);
		});
	}, 30_000);

	test("uses npm's configured tag for resumed packages without forwarding it to fresh publishes", async () => {
		const existing = [packageRef(RELEASE_FIXTURES[0]), packageRef(RELEASE_FIXTURES[2])];
		await withReleaseFixture({ existing, configTag: "beta" }, async ({ execute }) => {
			const result = await execute("--publish");
			expect(result.code).toBe(0);
			const npm = result.commands.filter((command) => command.tool === "npm");
			expect(npm.filter((command) => command.args[0] === "config").map((command) => command.cwd)).toEqual([
				"protocol",
				"sdk",
			]);
			expect(npm.filter((command) => command.args[0] === "dist-tag").map((command) => command.args)).toEqual(
				existing.map((ref) => ["dist-tag", "add", ref, "beta"]),
			);
			expect(
				npm.filter((command) => command.args[0] === "publish").every((command) => !command.args.includes("--tag")),
			).toBe(true);
		});
	}, 30_000);

	test("packed publishConfig tag overrides npm config, and an explicit tag overrides both", async () => {
		const pkg = RELEASE_FIXTURES[0];
		for (const explicit of [false, true]) {
			await withReleaseFixture(
				{ existing: [packageRef(pkg)], configTag: "beta", publishTags: { [pkg.name]: "canary" }, configError: true },
				async ({ execute }) => {
					const result = await execute("--publish", ...(explicit ? ["--tag", "next"] : []));
					expect(result.code).toBe(0);
					const npm = result.commands.filter((command) => command.tool === "npm");
					expect(npm.some((command) => command.args[0] === "config")).toBe(false);
					expect(
						npm.some(
							(command) => command.args.join(" ") === `view ${pkg.name}@${explicit ? "next" : "canary"} version`,
						),
					).toBe(true);
					expect(
						npm.some(
							(command) => command.args.join(" ") === `dist-tag add ${packageRef(pkg)} ${explicit ? "next" : "canary"}`,
						),
					).toBe(true);
				},
			);
		}
	}, 30_000);

	for (const [label, target, shouldWrite] of [
		["equal", "1.2.3", false],
		["older", "1.2.2", true],
		["absent", undefined, true],
	] as const) {
		test(`${label} dist-tag ${shouldWrite ? "is updated" : "skips mutation"}`, async () => {
			const pkg = RELEASE_FIXTURES[0];
			await withReleaseFixture(
				{ existing: [packageRef(pkg)], tags: target ? { [`${pkg.name}@next`]: target } : {} },
				async ({ execute }) => {
					const result = await execute("--publish", "--tag", "next");
					expect(result.code).toBe(0);
					expect(
						result.commands.filter((command) => command.args[0] === "dist-tag").map((command) => command.args),
					).toEqual(shouldWrite ? [["dist-tag", "add", packageRef(pkg), "next"]] : []);
				},
			);
		}, 30_000);
	}

	test("a repeated release observes the tags written by the first release", async () => {
		await withReleaseFixture({ existing: RELEASE_FIXTURES.map(packageRef) }, async ({ execute }) => {
			const first = await execute("--publish", "--tag", "next");
			expect(first.code).toBe(0);
			expect(first.commands.filter((command) => command.args[0] === "dist-tag")).toHaveLength(4);
			const second = await execute("--publish", "--tag", "next");
			expect(second.code).toBe(0);
			expect(
				second.commands.slice(first.commands.length).filter((command) => command.args[0] === "dist-tag"),
			).toHaveLength(0);
		});
	}, 30_000);

	test("rejects a newer tag without mutation or publishing subsequent packages", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{ existing: [packageRef(pkg)], tags: { [`${pkg.name}@next`]: "1.3.0" } },
			async ({ execute }) => {
				const result = await execute("--publish", "--tag", "next");
				expect(result.code).not.toBe(0);
				expect(result.output).toContain("1.3.0");
				expect(
					result.commands.some(
						(command) => command.args[0] === "dist-tag" || command.args[0] === "publish" || command.cwd === "images",
					),
				).toBe(false);
			},
		);
	}, 30_000);

	for (const explicit of [false, true]) {
		test(`prerelease ${explicit ? "explicitly promotes latest" : "rejects implicit latest"}`, async () => {
			const pkg = RELEASE_FIXTURES[0];
			const ref = `${pkg.name}@1.2.3-rc.1`;
			await withReleaseFixture({ existing: [ref], versions: { [pkg.name]: "1.2.3-rc.1" } }, async ({ execute }) => {
				const result = await execute("--publish", ...(explicit ? ["--tag", "latest"] : []));
				expect(result.code === 0).toBe(explicit);
				const mutations = result.commands.filter((command) => command.args[0] === "dist-tag");
				expect(mutations.map((command) => command.args)).toEqual(explicit ? [["dist-tag", "add", ref, "latest"]] : []);
				if (!explicit) expect(result.output).toMatch(/prerelease/i);
			});
		}, 30_000);
	}

	test("a prerelease that already holds latest is a no-op, not a refusal", async () => {
		const pkg = RELEASE_FIXTURES[0];
		const ref = `${pkg.name}@1.2.3-rc.1`;
		await withReleaseFixture(
			{ existing: [ref], versions: { [pkg.name]: "1.2.3-rc.1" }, tags: { [`${pkg.name}@latest`]: "1.2.3-rc.1" } },
			async ({ execute }) => {
				const result = await execute("--publish");
				expect(result.code).toBe(0);
				expect(result.output).toContain("already has dist-tag latest");
				expect(result.commands.filter((command) => command.args[0] === "dist-tag")).toEqual([]);
			},
		);
	}, 30_000);

	test("fails when a concurrent release moved the tag this run just wrote", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture({ existing: [packageRef(pkg)], interleave: "1.2.4" }, async ({ execute }) => {
			const result = await execute("--publish", "--tag", "next");
			expect(result.code).not.toBe(0);
			expect(result.output).toContain("concurrent release");
			expect(result.output).toContain("1.2.4");
			expect(result.commands.some((command) => command.cwd === "images" || command.args[0] === "publish")).toBe(false);
		});
	}, 30_000);

	test("refuses an option-shaped publishConfig tag instead of passing it to npm", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{ existing: [packageRef(pkg)], publishTags: { [pkg.name]: "--force" } },
			async ({ execute }) => {
				const result = await execute("--publish");
				expect(result.code).not.toBe(0);
				expect(result.output).toContain("would parse it as an option");
				expect(result.commands.some((command) => command.args[0] === "dist-tag" || command.cwd === "images")).toBe(
					false,
				);
			},
		);
	}, 30_000);

	for (const failure of ["tag lookup", "config"] as const) {
		test(`${failure} error fails without tag mutation`, async () => {
			const pkg = RELEASE_FIXTURES[0];
			await withReleaseFixture(
				{
					existing: [packageRef(pkg)],
					...(failure === "config" ? { configError: true } : { registryError: `${pkg.name}@next` }),
				},
				async ({ execute }) => {
					const result = await execute("--publish", ...(failure === "tag lookup" ? ["--tag", "next"] : []));
					expect(result.code).not.toBe(0);
					expect(result.output).toContain(
						failure === "config" ? "simulated config failure" : "simulated registry failure",
					);
					expect(
						result.commands.some(
							(command) => command.args[0] === "dist-tag" || command.args[0] === "publish" || command.cwd === "images",
						),
					).toBe(false);
				},
			);
		}, 30_000);
	}
});
