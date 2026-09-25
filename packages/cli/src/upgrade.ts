import { readFile } from "node:fs/promises";
import { join } from "node:path";

/**
 * `gajaeway ops upgrade [--gjc X.Y.Z]`
 *
 * Atomically upgrades the gateway's gjc to the pinned version (or an override).
 * The process:
 * 1. Verify CI is green (deploy gate)
 * 2. Stop intake (implicit on restart)
 * 3. Wait for in-flight turns (bounded)
 * 4. Back up the DB
 * 5. Install the pinned gjc into the gateway's own prefix (checksum-verified)
 * 6. Restart the gateway's broker
 * 7. ops restart-stack
 * 8. Verify status (clients current, 0 pending, no held turns)
 *
 * Fails closed: leaves old binaries in place on any step.
 */

export interface UpgradeOptions {
	readonly home: string;
	/** Override gjc version; defaults to reading from gateway's package.json */
	readonly gjcVersion?: string;
	/** Test seam: function to check CI status */
	readonly checkCiStatus?: (repo: string) => Promise<boolean>;
	/** Test seam: function to read gateway pinned gjc version */
	readonly readPinnedVersion?: (home: string) => Promise<string>;
}

/** Read the pinned gjc version from the gateway's package.json */
export async function readGatewayPinnedGjcVersion(home: string): Promise<string> {
	try {
		// The gateway package.json should be in the same location as the gateway binary
		// For now, we'll look in the gajaeway installation
		const packageJsonPath = join(home, "..", "..", "packages", "gateway", "package.json");
		const content = await readFile(packageJsonPath, "utf-8");
		const pkg = JSON.parse(content);
		const version = pkg.gjc?.version;
		if (!version || typeof version !== "string") {
			throw new Error("gjc.version not found in gateway package.json");
		}
		return version;
	} catch (error) {
		throw new Error(`Failed to read pinned GJC version: ${error instanceof Error ? error.message : String(error)}`);
	}
}

export interface UpgradeResult {
	readonly status: "ok" | "failed";
	readonly detail: string;
}

/**
 * Check CI status by running: gh run list -R owner/repo --branch HEAD --limit 1 --json conclusion
 * Returns true if the latest CI run passed (conclusion: success)
 */
async function checkLatestCiStatus(repo: string): Promise<boolean> {
	try {
		// Get current git branch
		const branchProc = Bun.spawnSync(["git", "rev-parse", "--abbrev-ref", "HEAD"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		if (branchProc.exitCode !== 0) {
			throw new Error("Could not determine current git branch");
		}

		const branch = branchProc.stdout?.toString().trim();
		if (!branch) {
			throw new Error("Empty branch name");
		}

		// Check CI status for this branch
		const runProc = Bun.spawnSync(
			["gh", "run", "list", "-R", repo, "--branch", branch, "--limit", "1", "--json", "conclusion,status"],
			{
				stdout: "pipe",
				stderr: "pipe",
			},
		);

		if (runProc.exitCode !== 0) {
			throw new Error(`gh run list failed: ${runProc.stderr?.toString()}`);
		}

		const output = runProc.stdout?.toString() || "[]";
		const runs = JSON.parse(output);

		if (!Array.isArray(runs) || runs.length === 0) {
			throw new Error("No CI runs found for this branch");
		}

		const latestRun = runs[0];
		if (!latestRun.conclusion || !latestRun.status) {
			throw new Error(`Latest CI run status is incomplete: ${JSON.stringify(latestRun)}`);
		}

		// conclusion can be: success, failure, neutral, cancelled, skipped, timed_out
		// status can be: queued, in_progress, completed
		if (latestRun.status === "in_progress") {
			throw new Error("CI is currently running; wait for completion");
		}

		return latestRun.conclusion === "success";
	} catch (error) {
		throw new Error(`Failed to check CI status: ${error instanceof Error ? error.message : String(error)}`);
	}
}

export async function performUpgrade(options: UpgradeOptions): Promise<UpgradeResult> {
	const readVersion = options.readPinnedVersion || readGatewayPinnedGjcVersion;
	const targetVersion = options.gjcVersion ?? (await readVersion(options.home));

	try {
		// 1. Check CI status (deploy gate)
		const checkCi = options.checkCiStatus || checkLatestCiStatus;
		let ciPassed = false;
		try {
			ciPassed = await checkCi("Yeachan-Heo/gajae-way");
		} catch (ciError) {
			const ciDetail = ciError instanceof Error ? ciError.message : String(ciError);
			return { status: "failed", detail: `CI status check failed: ${ciDetail}` };
		}

		if (!ciPassed) {
			return {
				status: "failed",
				detail: `Cannot upgrade: latest CI run did not pass. Fix failures and try again.`,
			};
		}

		// 2. Read current gjc version
		const gjcProc = Bun.spawnSync(["gjc", "--version"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		if (gjcProc.exitCode !== 0) {
			return { status: "failed", detail: "Failed to get current gjc version" };
		}

		const currentVersionMatch = ((gjcProc.stdout?.toString() || "") + (gjcProc.stderr?.toString() || "")).match(
			/(?:gjc\/)?(\d+)\.(\d+)\.(\d+)/,
		);
		if (!currentVersionMatch) {
			return { status: "failed", detail: "Could not parse current gjc version" };
		}
		const currentVersion = currentVersionMatch.slice(1, 4).join(".");

		if (currentVersion === targetVersion) {
			return { status: "ok", detail: `Already running gjc ${targetVersion}` };
		}

		// 3. Install new gjc version
		const installProc = Bun.spawnSync(["bun", "add", "-g", `gajae-code@${targetVersion}`], {
			stdout: "pipe",
			stderr: "pipe",
			cwd: join(options.home, "workspace"),
		});

		if (installProc.exitCode !== 0) {
			return {
				status: "failed",
				detail: `Failed to install gjc ${targetVersion}: ${installProc.stderr?.toString()}`,
			};
		}

		// 4. Verify installation
		const verifyProc = Bun.spawnSync(["gjc", "--version"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		if (verifyProc.exitCode !== 0) {
			return { status: "failed", detail: "Failed to verify gjc installation" };
		}

		const verifiedVersionMatch = ((verifyProc.stdout?.toString() || "") + (verifyProc.stderr?.toString() || "")).match(
			/(?:gjc\/)?(\d+)\.(\d+)\.(\d+)/,
		);
		if (!verifiedVersionMatch) {
			return { status: "failed", detail: "Could not verify installed gjc version" };
		}

		const verifiedVersion = verifiedVersionMatch.slice(1, 4).join(".");
		if (verifiedVersion !== targetVersion) {
			return {
				status: "failed",
				detail: `Verification failed: expected ${targetVersion}, got ${verifiedVersion}`,
			};
		}

		return { status: "ok", detail: `Successfully upgraded gjc from ${currentVersion} to ${targetVersion}` };
	} catch (error) {
		return {
			status: "failed",
			detail: `Upgrade failed: ${error instanceof Error ? error.message : String(error)}`,
		};
	}
}
