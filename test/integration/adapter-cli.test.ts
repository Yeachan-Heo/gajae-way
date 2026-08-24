import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { runUnifiedAdapter } from "../../src/adapter/main";

/**
 * The unified adapter is launched by the runbooks and the shipped systemd units
 * with --state-dir and --profile. A launcher that resolves configuration from the
 * environment alone silently ignores those flags and loads the wrong profile,
 * which is exactly how the live deployment broke: the compiled adapter reported
 * "Discord token is missing" while the supplied profile declared a valid
 * token_file. These tests exercise the real argv path, not a prebuilt config.
 */
function writeDeployment(): { readonly dir: string; readonly profilePath: string } {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-adapter-cli-"));
	fs.writeFileSync(path.join(dir, "discord-token"), "cli-regression-token", { mode: 0o600 });
	const profilePath = path.join(dir, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${dir}"\nworkspace = "${dir}"\n\n[injection]\nfiles = []\n\n[main_session]\nsession_id = "cli-regression-session"\n\n[surfaces.owner]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "main"\n\n[adapter.discord]\ntoken_file = "discord-token"\nchannel_id = "1468535438498336923"\nsurface_id = "discord:owner-dm"\n`,
	);
	return { dir, profilePath };
}

test("the unified adapter resolves its channel config from --state-dir and --profile", async () => {
	const { dir, profilePath } = writeDeployment();
	try {
		let checked = false;
		await runUnifiedAdapter(["--check", "--state-dir", path.join(dir, "state"), "--profile", profilePath], {
			environment: { PATH: process.env.PATH ?? "" } as NodeJS.ProcessEnv,
			fetch: (async (input: string | URL | Request) => {
				const url = String(input);
				if (url.endsWith("/users/@me")) {
					checked = true;
					return new Response(JSON.stringify({ id: "bot-cli", username: "cli", bot: true }), { status: 200 });
				}
				return new Response("{}", { status: 200 });
			}) as unknown as typeof fetch,
			async rpcConnect() {
				return {
					async request(method: string) {
						if (method === "way.health") return { result: { status: "healthy", state: "running" } };
						return { result: {} };
					},
					close() {},
				} as never;
			},
		});
		// Reaching the Discord identity probe proves the profile's token_file was
		// resolved from the supplied --profile rather than from the environment.
		expect(checked).toBe(true);
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});

test("the unified adapter rejects an unknown argument instead of ignoring it", async () => {
	const { dir, profilePath } = writeDeployment();
	try {
		await expect(
			runUnifiedAdapter(["--check", "--profile", profilePath, "--not-a-flag"], {
				environment: { PATH: process.env.PATH ?? "" } as NodeJS.ProcessEnv,
			}),
		).rejects.toThrow(/Unknown argument/);
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});
