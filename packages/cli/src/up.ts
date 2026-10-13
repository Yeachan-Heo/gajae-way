import { access } from "node:fs/promises";
import { join } from "node:path";
import { GajaewayClient } from "@gajae-gateway/sdk";
import type { RunSetupOptions } from "./setup";
import { enabledAdapters, runSetup } from "./setup";

export interface UpOptions extends Omit<RunSetupOptions, "home"> {
	readonly home: string;
	readonly socket?: string;
	/** Skip the setup check and assume config exists. */
	readonly skipSetup?: boolean;
	/** Spawn runner for tests; defaults to Bun.spawn */
	readonly spawn?: (cmd: string, args: string[], opts?: { cwd?: string }) => unknown;
}

/**
 * `gajaeway up`: One-command setup and start for first-time users.
 *
 * Checks for existing config.json; if missing, runs interactive setup.
 * Then starts the gateway daemon and enabled adapters in the background.
 * Reports the gateway status and how to stop it.
 */
export async function runUp(options: UpOptions): Promise<void> {
	const log = options.log ?? ((line: string) => console.log(line));
	const configPath = join(options.home, "config.json");

	// Check if setup is needed
	if (!options.skipSetup) {
		let exists = false;
		try {
			await access(configPath);
			exists = true;
		} catch {
			// File does not exist
		}

		if (!exists) {
			log("No configuration found. Running setup...\n");
			try {
				// Run setup in default (interactive) mode
				await runSetup([], {
					home: options.home,
					env: options.env,
					fetch: options.fetch,
					prompter: options.prompter,
					log,
				});
			} catch (error) {
				const message = error instanceof Error ? error.message : String(error);
				log(`Setup failed: ${message}`);
				throw error;
			}

			log("\n✓ Configuration saved.\n");
		}
	}

	// Check gateway status
	const socket = options.socket ?? `${options.home}/gateway.sock`;
	let isRunning = false;

	try {
		const client = await GajaewayClient.connectSocket(socket);
		try {
			await client.status();
			isRunning = true;
		} finally {
			await client.close();
		}
	} catch {
		// Gateway is not running; we'll try to start it
	}

	if (isRunning) {
		log("✓ Gateway is already running.");
		const adapters = await enabledAdapters(options.home);
		log(`✓ Enabled adapters: ${adapters.join(", ")}`);
		log("\nYou can now:");
		log("  • Send a message to your bot on Discord/Slack/Telegram");
		log("  • View status: gajaeway ops cycle");
		log("  • Search memory: gajaeway memory search <query>");
		log("\nStop with: gajaeway shutdown");
	} else {
		// Try to start the gateway and adapters
		log("Starting gateway and adapters...");

		// Detect if we're in the source checkout (has dist/gajaeway-gateway)
		// vs. running from installed binaries
		const isSourceCheckout = await access(join(options.home, "..", "..", "dist", "gajaeway-gateway"))
			.then(() => true)
			.catch(() => false);

		const spawn = options.spawn ?? ((cmd: string, args: string[]) => Bun.spawn([cmd, ...args]));

		try {
			// Start gateway daemon
			// Use bun packages/gateway/src/main.ts if in source checkout, else use binary
			const gatewayCmd = isSourceCheckout ? "bun" : "gajaeway-gateway";
			const gatewayArgs = isSourceCheckout ? ["packages/gateway/src/main.ts", "daemon"] : ["daemon"];

			const gatewayProcess = spawn(gatewayCmd, gatewayArgs, { cwd: process.cwd() });

			// Give gateway a moment to start
			await new Promise((resolve) => setTimeout(resolve, 2000));

			// Start enabled adapters
			const adapters = await enabledAdapters(options.home);
			for (const adapter of adapters) {
				const adapterCmd = isSourceCheckout ? "dist/gajaeway-" + adapter : "gajaeway-" + adapter;
				const adapterProcess = spawn(adapterCmd, [], { cwd: process.cwd() });
				// Don't wait for adapter; let it run in background
			}

			log("\n✓ Gateway started.");
			if (adapters.length > 0) {
				log(`✓ Adapters started: ${adapters.join(", ")}`);
			}

			log("\nYou can now:");
			log("  • Send a message to your bot on Discord/Slack/Telegram");
			log("  • View status: gajaeway ops cycle");
			log("  • Search memory: gajaeway memory search <query>");
			log("\nStop with: gajaeway shutdown");
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			log(`Failed to start services: ${message}`);
			log("\nAlternative: start manually in separate terminals:");
			log("  Terminal 1: bun packages/gateway/src/main.ts daemon");
			log("  Terminal 2: dist/gajaeway-discord");
			throw error;
		}
	}
}
