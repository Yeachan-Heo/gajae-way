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
import {
	assertNoPrivateWorkspaceDeps,
	highestPublishedStableVersion,
	parseNpmViewResult,
	RELEASE_ORDER,
} from "../../../scripts/release-packages";

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

describe("latest ordering", () => {
	test("selects the highest stable published version by semver order", () => {
		expect(highestPublishedStableVersion(["1.2.3", "1.10.0", "1.9.9"])).toBe("1.10.0");
	});

	test("ignores prereleases and build metadata when selecting latest", () => {
		expect(highestPublishedStableVersion(["1.2.3", "1.3.0-rc.1", "2.0.0-beta.2"])).toBe("1.2.3");
		expect(highestPublishedStableVersion(["1.3.0+build.5", "1.2.9"])).toBe("1.3.0+build.5");
	});

	test("reports no candidate when only prereleases are published", () => {
		expect(highestPublishedStableVersion(["1.0.0-rc.1"])).toBeUndefined();
		expect(highestPublishedStableVersion([])).toBeUndefined();
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
import { appendFileSync, copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
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
  if (args[0] === "view" && args[2] === "versions" && args[3] === "--json") {
    if (args[1] === process.env.VERSIONS_ERROR) {
      console.error("simulated version-list failure");
      process.exit(1);
    }
    const overrides = JSON.parse(process.env.PUBLISHED_VERSIONS);
    const afterPublish = JSON.parse(process.env.PUBLISHED_VERSIONS_AFTER_PUBLISH);
    const afterWrite = JSON.parse(process.env.PUBLISHED_VERSIONS_AFTER_WRITE);
    const name = args[1];
    if (existsSync(process.env.WRITTEN_MARKER) && Array.isArray(afterWrite[name])) {
      console.log(JSON.stringify(afterWrite[name]));
      process.exit(0);
    }
    if (existsSync(process.env.PUBLISHED_MARKER) && Array.isArray(afterPublish[name])) {
      console.log(JSON.stringify(afterPublish[name]));
      process.exit(0);
    }
    const versions = overrides[name] ?? JSON.parse(process.env.EXISTING_PACKAGES)
      .filter((ref) => ref.startsWith(name + "@"))
      .map((ref) => ref.slice(name.length + 1));
    if (versions.length === 0) {
      console.error("404 Not Found");
      process.exit(1);
    }
    console.log(JSON.stringify(versions));
    process.exit(0);
  }
  if (args[0] === "view" && args[2] === "dist-tags" && args[3] === "--json") {
    if (args[1] === process.env.TAGS_EMPTY) process.exit(0);
    if (args[1] === process.env.TAGS_ERROR) {
      console.error("simulated tag-map failure");
      process.exit(1);
    }
    const state = JSON.parse(readFileSync(process.env.TAG_STATE, "utf8"));
    const prefix = args[1] + "@";
    const tags = {};
    let known = false;
    for (const [key, value] of Object.entries(state)) {
      if (key.startsWith(prefix)) {
        tags[key.slice(prefix.length)] = value;
        known = true;
      }
    }
    if (!known && !JSON.parse(process.env.EXISTING_PACKAGES).some((ref) => ref.startsWith(prefix))) {
      console.error("404 Not Found");
      process.exit(1);
    }
    console.log(JSON.stringify(tags));
    process.exit(0);
  }
  if (args[0] === "view" && args.length === 3 && args[2] === "version") {
    if (args[1] === process.env.REGISTRY_ERROR) {
      console.error("simulated registry failure");
      process.exit(1);
    }
    const tags = JSON.parse(readFileSync(process.env.TAG_STATE, "utf8"));
    if (Object.hasOwn(tags, args[1])) {
      const stale = JSON.parse(readFileSync(process.env.STALE_STATE, "utf8"));
      if (Object.hasOwn(stale, args[1])) {
        const value = stale[args[1]];
        delete stale[args[1]];
        writeFileSync(process.env.STALE_STATE, JSON.stringify(stale));
        if (value === "__404__") {
          console.error("404 Not Found");
          process.exit(1);
        }
        console.log(value);
        process.exit(0);
      }
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
    writeFileSync(process.env.WRITTEN_MARKER, "1");
    process.exit(0);
  }
  if (args[0] === "dist-tag" && args[1] === "rm") {
    const tags = JSON.parse(readFileSync(process.env.TAG_STATE, "utf8"));
    delete tags[args[2] + "@" + args[3]];
    writeFileSync(process.env.TAG_STATE, JSON.stringify(tags));
    process.exit(0);
  }
  if (args[0] === "publish") {
    if (process.env.PUBLISH_ERROR && args[1].endsWith(process.env.PUBLISH_ERROR)) {
      console.error("simulated publish failure");
      process.exit(1);
    }
    // Real npm publish writes the dist-tag it was given, so the fixture must too or
    // the post-publish reads would see a pointer the real registry already moved.
    const tagIndex = args.indexOf("--tag");
    if (tagIndex !== -1 && args[tagIndex + 1] !== undefined) {
      const packed = JSON.parse(spawnSync("tar", ["-xOzf", args[1], "package/package.json"], { encoding: "utf8" }).stdout);
      const tags = JSON.parse(readFileSync(process.env.TAG_STATE, "utf8"));
      tags[packed.name + "@" + args[tagIndex + 1]] = packed.version;
      writeFileSync(process.env.TAG_STATE, JSON.stringify(tags));
      writeFileSync(process.env.PUBLISHED_MARKER, "1");
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
		staleVerification?: Record<string, string>;
		publishedVersions?: Record<string, string[]>;
		publishedVersionsAfterPublish?: Record<string, string[]>;
		publishedVersionsAfterWrite?: Record<string, string[]>;
		versionsError?: string;
		tagsError?: string;
		tagsEmpty?: string;
	},
	run: (fixture: {
		root: string;
		execute: (...args: string[]) => Promise<{ code: number; output: string; commands: RecordedCommand[] }>;
	}) => Promise<void>,
): Promise<void> {
	// Bun resolves symlinks in import.meta.url, so the copied script always reports the
	// canonical temp root. Canonicalize here too, or macOS (/var -> /private/var) compares
	// the script's realpath output against an unresolved expectation.
	//
	// The prefix contains a space and `#` on purpose: `new URL("..", import.meta.url)`
	// percent-encodes both, so every test in this file fails if the script uses the URL
	// pathname instead of decoding it with `fileURLToPath`.
	const root = await realpath(await mkdtemp(join(tmpdir(), "release packages #")));
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
		const publishedMarker = join(root, "published.marker");
		const writtenMarker = join(root, "written.marker");
		const staleState = join(root, "stale.json");
		await writeFile(staleState, JSON.stringify(options.staleVerification ?? {}));
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
						STALE_STATE: staleState,
						CONFIG_TAG: options.configTag ?? "latest",
						CONFIG_ERROR: options.configError ? "1" : "",
						INTERLEAVE_VERSION: options.interleave ?? "",
						PUBLISHED_VERSIONS: JSON.stringify(options.publishedVersions ?? {}),
						PUBLISHED_VERSIONS_AFTER_PUBLISH: JSON.stringify(options.publishedVersionsAfterPublish ?? {}),
						PUBLISHED_MARKER: publishedMarker,
						PUBLISHED_VERSIONS_AFTER_WRITE: JSON.stringify(options.publishedVersionsAfterWrite ?? {}),
						WRITTEN_MARKER: writtenMarker,
						VERSIONS_ERROR: options.versionsError ?? "",
						TAGS_ERROR: options.tagsError ?? "",
						TAGS_EMPTY: options.tagsEmpty ?? "",
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
							args: ["view", pkg.name, "dist-tags", "--json"],
							cwd: pkg.directory,
						});
						if (tag === undefined) {
							expected.push({
								tool: "npm",
								args: ["view", pkg.name, "versions", "--json"],
								cwd: pkg.directory,
							});
						}
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
						if (tag === undefined) {
							// `latest` is re-derived from the version list after the write.
							expected.push({
								tool: "npm",
								args: ["view", pkg.name, "versions", "--json"],
								cwd: pkg.directory,
							});
							expected.push({
								tool: "npm",
								args: ["view", pkg.name, "dist-tags", "--json"],
								cwd: pkg.directory,
							});
						}
					} else {
						// The publish path resolves the tag once and orders it before publishing, so a
						// fresh package reads npm's configured tag (only when --tag is absent) and, for
						// `latest`, the published version list.
						if (!tag) expected.push({ tool: "npm", args: ["config", "get", "tag"], cwd: pkg.directory });
						// The publish path reads the live pointer, and for `latest` the published version
						// list, before deciding which tag `npm publish` must write.
						expected.push({
							tool: "npm",
							args: ["view", pkg.name, "dist-tags", "--json"],
							cwd: pkg.directory,
						});
						if (tag === undefined) {
							expected.push({
								tool: "npm",
								args: ["view", pkg.name, "versions", "--json"],
								cwd: pkg.directory,
							});
						}
						expected.push({
							tool: "npm",
							args: ["publish", tarball, "--access", "public", "--tag", tag ?? "latest"],
							cwd: pkg.directory,
						});
						// `npm publish` writes the tag itself; only `latest` is reconciled afterwards, from
						// the version list, so a release that landed during the upload cannot leave it
						// behind. A named tag is ordered by its pointer before the publish.
						if (tag === undefined) {
							expected.push({
								tool: "npm",
								args: ["view", pkg.name, "versions", "--json"],
								cwd: pkg.directory,
							});
							expected.push({
								tool: "npm",
								args: ["view", pkg.name, "dist-tags", "--json"],
								cwd: pkg.directory,
							});
						}
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
				["config", "get", "tag"],
				["view", RELEASE_FIXTURES[0].name, "dist-tags", "--json"],
				["view", RELEASE_FIXTURES[0].name, "versions", "--json"],
				["publish", join(root, "dist-packed", "gajae-gateway-protocol.tgz"), "--access", "public", "--tag", "latest"],
				["view", RELEASE_FIXTURES[0].name, "versions", "--json"],
				["view", RELEASE_FIXTURES[0].name, "dist-tags", "--json"],
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
				["view", RELEASE_FIXTURES[0].name, "dist-tags", "--json"],
				["publish", join(root, "dist-packed", "gajae-gateway-protocol.tgz"), "--access", "public", "--tag", "next"],
				["view", failed, "version"],
				["view", RELEASE_FIXTURES[1].name, "dist-tags", "--json"],
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
				["view", RELEASE_FIXTURES[0].name, "dist-tags", "--json"],
				["publish", join(root, "dist-packed", "gajae-gateway-protocol.tgz"), "--access", "public", "--tag", "next"],
				["view", packageRef(RELEASE_FIXTURES[1]), "version"],
				["view", RELEASE_FIXTURES[1].name, "dist-tags", "--json"],
				["publish", join(root, "dist-packed", "gajae-gateway-images.tgz"), "--access", "public", "--tag", "next"],
			]);
			expect(result.commands.some((command) => command.cwd === "sdk")).toBe(false);
		});
	}, 30_000);

	test("forwards npm's configured tag to fresh publishes and resumes alike", async () => {
		const existing = [packageRef(RELEASE_FIXTURES[0]), packageRef(RELEASE_FIXTURES[2])];
		await withReleaseFixture({ existing, configTag: "beta" }, async ({ execute }) => {
			const result = await execute("--publish");
			expect(result.code).toBe(0);
			const npm = result.commands.filter((command) => command.tool === "npm");
			// Every package resolves the same configured tag, because the publish path shares
			// the resume path's precedence instead of letting npm pick an untracked default.
			expect(new Set(npm.filter((command) => command.args[0] === "config").map((command) => command.cwd))).toEqual(
				new Set(RELEASE_FIXTURES.map((pkg) => pkg.directory)),
			);
			expect(npm.filter((command) => command.args[0] === "dist-tag").map((command) => command.args)).toEqual(
				existing.map((ref) => ["dist-tag", "add", ref, "beta"]),
			);
			expect(
				npm
					.filter((command) => command.args[0] === "publish")
					.every((command) => command.args[command.args.length - 1] === "beta"),
			).toBe(true);
		});
	}, 30_000);

	test("packed publishConfig tag overrides npm config, and an explicit tag overrides both", async () => {
		const pkg = RELEASE_FIXTURES[0];
		const existing = RELEASE_FIXTURES.map(packageRef);
		for (const explicit of [false, true]) {
			await withReleaseFixture(
				{ existing, configTag: "beta", publishTags: { [pkg.name]: "canary" } },
				async ({ execute }) => {
					const result = await execute("--publish", ...(explicit ? ["--tag", "next"] : []));
					expect(result.code).toBe(0);
					const npm = result.commands.filter((command) => command.tool === "npm");
					// An explicit --tag skips npm config altogether, and the packed publishConfig tag
					// wins over it for the package that declares one.
					if (explicit) {
						expect(npm.some((command) => command.args[0] === "config")).toBe(false);
					} else {
						expect(npm.some((command) => command.cwd === pkg.directory && command.args[0] === "config")).toBe(false);
						expect(npm.some((command) => command.cwd !== pkg.directory && command.args[0] === "config")).toBe(true);
					}
					expect(npm.filter((command) => command.args[0] === "dist-tag").map((command) => command.args[3])).toEqual(
						explicit ? ["next", "next", "next", "next"] : ["canary", "beta", "beta", "beta"],
					);
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

	// A newer release already moved this tag. Moving it back is never allowed, but
	// failing would strand this tag's published version and skip its GitHub release, so
	// the run leaves the pointer alone and finishes the remaining packages.
	test("leaves a tag that a newer release already moved and finishes the release", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{ existing: [packageRef(pkg)], tags: { [`${pkg.name}@next`]: "1.3.0" } },
			async ({ execute }) => {
				const result = await execute("--publish", "--tag", "next");
				expect(result.code).toBe(0);
				expect(result.output).toContain("leaving it instead of moving it back");
				expect(result.commands.filter((command) => command.args[0] === "dist-tag")).toEqual([]);
				expect(result.commands.some((command) => command.cwd === "images")).toBe(true);
			},
		);
	}, 30_000);

	// A rerun of an older release can read a cached dist-tag that still points at the
	// previous version while a newer release has already published 1.3.0. The published
	// version list orders the write, so the pointer is converged forward and the older
	// version is never written to latest.
	test("converges a stale latest read forward to the newest published stable version", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{
				existing: [packageRef(pkg)],
				tags: { [`${pkg.name}@latest`]: "1.2.2" },
				publishedVersions: { [pkg.name]: ["1.2.2", "1.2.3", "1.3.0"] },
			},
			async ({ execute }) => {
				const result = await execute("--publish");
				expect(result.code).toBe(0);
				expect(result.output).toContain("stays with 1.3.0");
				expect(
					result.commands.filter((command) => command.args[0] === "dist-tag").map((command) => command.args),
				).toEqual([["dist-tag", "add", `${pkg.name}@1.3.0`, "latest"]]);
				expect(result.commands.some((command) => command.cwd === "images")).toBe(true);
			},
		);
	}, 30_000);

	test("does not treat a published prerelease as blocking latest", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{
				existing: [packageRef(pkg)],
				tags: { [`${pkg.name}@latest`]: "1.2.2" },
				publishedVersions: { [pkg.name]: ["1.2.2", "1.3.0-rc.1"] },
			},
			async ({ execute }) => {
				const result = await execute("--publish");
				expect(result.code).toBe(0);
				expect(
					result.commands.filter((command) => command.args[0] === "dist-tag").map((command) => command.args),
				).toEqual([["dist-tag", "add", packageRef(pkg), "latest"]]);
			},
		);
	}, 30_000);

	// Publishing immutable content must not be blocked by the pointer: the older version is
	// still published, under a scratch tag, so `latest` never moves backwards at all.
	test("publishes an older version under a scratch tag and converges latest forward", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture({ publishedVersions: { [pkg.name]: ["1.2.2", "1.3.0"] } }, async ({ execute }) => {
			const result = await execute("--publish", "--tag", "latest");
			expect(result.code).toBe(0);
			expect(result.output).toContain("stays with 1.3.0");
			const npm = result.commands.filter((command) => command.tool === "npm");
			const publish = npm.filter((command) => command.args[0] === "publish" && command.cwd === pkg.directory);
			expect(publish.map((command) => command.args[command.args.length - 1])).toEqual(["gajae-pending-1-2-3"]);
			expect(npm.filter((command) => command.args[0] === "dist-tag").map((command) => command.args)).toEqual([
				["dist-tag", "rm", pkg.name, "gajae-pending-1-2-3"],
				["dist-tag", "add", `${pkg.name}@1.3.0`, "latest"],
			]);
		});
	}, 30_000);

	// A resumed release can read the version list before a newer release publishes and
	// then write its own older version. The post-write reconciliation re-derives `latest`
	// from the list, so the stale write is repaired by the same run.
	test("repairs a resumed latest write when a newer release publishes in between", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{
				existing: [packageRef(pkg)],
				tags: { [`${pkg.name}@latest`]: "1.2.2" },
				publishedVersions: { [pkg.name]: ["1.2.2", "1.2.3"] },
				publishedVersionsAfterWrite: { [pkg.name]: ["1.2.2", "1.2.3", "1.3.0"] },
			},
			async ({ execute }) => {
				const result = await execute("--publish");
				expect(result.code).toBe(0);
				expect(
					result.commands.filter((command) => command.args[0] === "dist-tag").map((command) => command.args),
				).toEqual([
					["dist-tag", "add", packageRef(pkg), "latest"],
					["dist-tag", "add", `${pkg.name}@1.3.0`, "latest"],
				]);
				expect(result.output).toContain("stays with 1.3.0");
			},
		);
	}, 30_000);

	// A release that lands while this run uploads is the documented read-to-write window:
	// `npm publish` has already written `latest` onto this run's older version when the
	// newer version shows up in the list, so the post-publish read converges forward.
	test("converges latest when a newer release publishes during the upload", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{
				publishedVersions: { [pkg.name]: ["1.2.2"] },
				publishedVersionsAfterPublish: { [pkg.name]: ["1.2.2", "1.3.0"] },
			},
			async ({ execute }) => {
				const result = await execute("--publish", "--tag", "latest");
				expect(result.code).toBe(0);
				const npm = result.commands.filter((command) => command.tool === "npm");
				expect(
					npm
						.filter((command) => command.args[0] === "publish" && command.cwd === pkg.directory)
						.map((command) => command.args[command.args.length - 1]),
				).toEqual(["latest"]);
				expect(npm.filter((command) => command.args[0] === "dist-tag").map((command) => command.args)).toEqual([
					["dist-tag", "add", `${pkg.name}@1.3.0`, "latest"],
				]);
				expect(result.output).toContain("stays with 1.3.0");
			},
		);
	}, 30_000);

	// An explicitly promoted prerelease keeps `latest`; a resumed stable release must not
	// demote it just because it is the highest published stable version.
	test("does not demote a latest pointer that holds a newer prerelease", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{
				existing: [packageRef(pkg)],
				tags: { [`${pkg.name}@latest`]: "1.3.0-rc.1" },
				publishedVersions: { [pkg.name]: ["1.2.3", "1.3.0-rc.1"] },
			},
			async ({ execute }) => {
				const result = await execute("--publish");
				expect(result.code).toBe(0);
				expect(result.output).toContain("stays with 1.3.0-rc.1");
				expect(result.commands.filter((command) => command.args[0] === "dist-tag")).toEqual([]);
				expect(result.commands.some((command) => command.cwd === "images")).toBe(true);
			},
		);
	}, 30_000);

	test("publishes an older version under a scratch tag instead of demoting a newer prerelease", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{ tags: { [`${pkg.name}@latest`]: "1.3.0-rc.1" }, publishedVersions: { [pkg.name]: ["1.2.3", "1.3.0-rc.1"] } },
			async ({ execute }) => {
				const result = await execute("--publish", "--tag", "latest");
				expect(result.code).toBe(0);
				expect(result.output).toContain("stays with 1.3.0-rc.1");
				const publish = result.commands.filter(
					(command) => command.args[0] === "publish" && command.cwd === pkg.directory,
				);
				expect(publish.map((command) => command.args[command.args.length - 1])).toEqual(["gajae-pending-1-2-3"]);
				expect(
					result.commands.filter((command) => command.args[0] === "dist-tag").map((command) => command.args),
				).toEqual([["dist-tag", "rm", pkg.name, "gajae-pending-1-2-3"]]);
			},
		);
	}, 30_000);

	// A pointer equal to the resumed version does not prove it is still the newest one.
	test("converges a latest pointer that already sits on the resumed version", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{
				existing: [packageRef(pkg)],
				tags: { [`${pkg.name}@latest`]: "1.2.3" },
				publishedVersions: { [pkg.name]: ["1.2.3", "1.3.0"] },
			},
			async ({ execute }) => {
				const result = await execute("--publish");
				expect(result.code).toBe(0);
				expect(result.output).toContain("stays with 1.3.0");
				expect(
					result.commands.filter((command) => command.args[0] === "dist-tag").map((command) => command.args),
				).toEqual([["dist-tag", "add", `${pkg.name}@1.3.0`, "latest"]]);
			},
		);
	}, 30_000);

	test("refuses to publish a prerelease without an explicit tag instead of letting npm default to latest", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture({ versions: { [pkg.name]: "1.2.3-rc.1" } }, async ({ execute }) => {
			const result = await execute("--publish");
			expect(result.code).not.toBe(0);
			expect(result.output).toMatch(/prerelease .* as latest/);
			expect(result.commands.some((command) => command.args[0] === "publish")).toBe(false);
		});
	}, 30_000);

	// The first stable release after prerelease-only publishes has no `latest` tag to read.
	// A tag-map read reports that as an absent entry, not as an error that aborts the release.
	test("publishes the first stable version when only a prerelease tag exists", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{
				existing: [`${pkg.name}@1.2.3-rc.1`],
				tags: { [`${pkg.name}@next`]: "1.2.3-rc.1" },
				publishedVersions: { [pkg.name]: ["1.2.3-rc.1"] },
			},
			async ({ execute }) => {
				const result = await execute("--publish", "--tag", "latest");
				expect(result.code).toBe(0);
				const publish = result.commands.filter(
					(command) => command.args[0] === "publish" && command.cwd === pkg.directory,
				);
				expect(publish.map((command) => command.args[command.args.length - 1])).toEqual(["latest"]);
				expect(result.output).not.toContain("dist-tag lookup failed");
			},
		);
	}, 30_000);

	test("treats a 404 version list as a package that has never been published", async () => {
		await withReleaseFixture({}, async ({ execute }) => {
			const result = await execute("--publish", "--tag", "latest");
			expect(result.code).toBe(0);
			expect(result.output).not.toContain("published-version lookup failed");
			expect(result.commands.filter((command) => command.args[0] === "publish")).toHaveLength(RELEASE_FIXTURES.length);
			expect(result.commands.filter((command) => command.args[0] === "dist-tag")).toEqual([]);
		});
	}, 30_000);

	test("passes an explicit latest through to npm publish when this version is the highest", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture({ publishedVersions: { [pkg.name]: ["1.2.2"] } }, async ({ execute }) => {
			const result = await execute("--publish", "--tag", "latest");
			expect(result.code).toBe(0);
			const publish = result.commands.filter(
				(command) => command.args[0] === "publish" && command.cwd === pkg.directory,
			);
			expect(publish.map((command) => command.args[command.args.length - 1])).toEqual(["latest"]);
			expect(result.commands.filter((command) => command.args[0] === "dist-tag")).toEqual([]);
		});
	}, 30_000);

	// An exit-0 read with no body is not evidence that no tag exists, so it must not
	// authorize a write decision.
	test("fails closed when the tag map comes back empty", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture({ existing: [packageRef(pkg)], tagsEmpty: pkg.name }, async ({ execute }) => {
			const result = await execute("--publish", "--tag", "next");
			expect(result.code).not.toBe(0);
			expect(result.output).toContain("invalid JSON");
			expect(result.commands.some((command) => command.args[0] === "dist-tag" || command.cwd === "images")).toBe(false);
		});
	}, 30_000);

	test("fails closed when the published version list cannot be read", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{ existing: [packageRef(pkg)], tags: { [`${pkg.name}@latest`]: "1.2.2" }, versionsError: pkg.name },
			async ({ execute }) => {
				const result = await execute("--publish");
				expect(result.code).not.toBe(0);
				expect(result.output).toContain("simulated version-list failure");
				expect(result.commands.some((command) => command.args[0] === "dist-tag" || command.cwd === "images")).toBe(
					false,
				);
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

	// A concurrent release winning forward is the enforced ordering, so it must not fail
	// this run and strand the packages and GitHub release still to come.
	test("keeps a tag a concurrent release moved forward instead of failing the release", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture({ existing: [packageRef(pkg)], interleave: "1.2.4" }, async ({ execute }) => {
			const result = await execute("--publish", "--tag", "next");
			expect(result.code).toBe(0);
			expect(result.output).toContain("moved forward to 1.2.4");
			expect(result.commands.some((command) => command.cwd === "images")).toBe(true);
			// Detection only: never a second unconditional write.
			expect(result.commands.filter((command) => command.args[0] === "dist-tag")).toHaveLength(1);
		});
	}, 30_000);

	test("fails when a concurrent release moved the tag this run just wrote backwards", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture({ existing: [packageRef(pkg)], interleave: "1.2.2" }, async ({ execute }) => {
			const result = await execute("--publish", "--tag", "next");
			expect(result.code).not.toBe(0);
			expect(result.output).toContain("moved it backwards");
			expect(result.commands.some((command) => command.cwd === "images" || command.args[0] === "publish")).toBe(false);
		});
	}, 30_000);

	test("re-reads a lagging registry instead of calling it a concurrent move", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{ existing: [packageRef(pkg)], staleVerification: { [`${pkg.name}@next`]: "1.2.2" } },
			async ({ execute }) => {
				const result = await execute("--publish", "--tag", "next");
				expect(result.code).toBe(0);
				expect(result.output).not.toContain("concurrent release");
				// The retry re-reads; it must never issue a second unconditional write.
				expect(result.commands.filter((command) => command.args[0] === "dist-tag")).toHaveLength(1);
				// The pointer is read as a tag map now, so only the verification re-reads
				// `pkg@next` itself: the stale value once, then the value this run wrote.
				expect(
					result.commands.filter((command) => command.args[0] === "view" && command.args[1] === `${pkg.name}@next`),
				).toHaveLength(2);
			},
		);
	}, 30_000);

	test("retries a transient not-found read after the tag write", async () => {
		const pkg = RELEASE_FIXTURES[0];
		await withReleaseFixture(
			{ existing: [packageRef(pkg)], staleVerification: { [`${pkg.name}@next`]: "__404__" } },
			async ({ execute }) => {
				const result = await execute("--publish", "--tag", "next");
				expect(result.code).toBe(0);
				expect(result.output).not.toContain("verification failed");
				// Retried reads must never turn into a second write.
				expect(result.commands.filter((command) => command.args[0] === "dist-tag")).toHaveLength(1);
			},
		);
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
					...(failure === "config" ? { configError: true } : { tagsError: pkg.name }),
				},
				async ({ execute }) => {
					const result = await execute("--publish", ...(failure === "tag lookup" ? ["--tag", "next"] : []));
					expect(result.code).not.toBe(0);
					expect(result.output).toContain(
						failure === "config" ? "simulated config failure" : "simulated tag-map failure",
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
