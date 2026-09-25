import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { detectHermes, readHermes } from "./migrate/hermes";
import { detectOpenClaw, readOpenClaw } from "./migrate/openclaw";
import {
	emitChannels,
	emitImportedAxis,
	emitMonitors,
	emptyChannelPlan,
	type MigrationSource,
	type SourceExtract,
} from "./migrate/plan";
import { expandHome, SourceTree } from "./migrate/source-fs";

export interface MigrateOptions {
	source?: string;
	target?: string;
	dryRun?: boolean;
}

/**
 * Main migration entry point. Detects the source type (Hermes or OpenClaw),
 * reads its configuration, and writes the gajae-way-compatible output.
 */
export async function migrate(options: MigrateOptions): Promise<void> {
	const sourceRoot = expandHome(options.source ?? ".");
	const targetHome = expandHome(options.target ?? `${process.env.HOME ?? "~"}/.gajaeway`);

	// Detect source type
	let sourceType: MigrationSource | undefined;
	if (await detectHermes(sourceRoot)) {
		sourceType = "hermes";
	} else if (await detectOpenClaw(sourceRoot)) {
		sourceType = "openclaw";
	} else {
		throw new Error(
			`No Hermes or OpenClaw configuration detected at ${sourceRoot}. Expected .hermes.config.json or config.json`,
		);
	}

	console.log(`Detected ${sourceType} configuration at ${sourceRoot}`);

	// Create source tree for tracking consumed files
	const tree = new SourceTree(sourceRoot);

	// Read source configuration
	let extract: SourceExtract;
	if (sourceType === "hermes") {
		extract = await readHermes(tree, targetHome);
	} else {
		extract = await readOpenClaw(tree, targetHome);
	}

	// Report warnings
	for (const warning of extract.warnings) {
		console.warn(`Warning: ${warning}`);
	}

	// Report unmapped entries
	if (extract.unmapped.length > 0) {
		console.log("\nUnmapped entries (these will not be migrated):");
		const byCategory = new Map<string, typeof extract.unmapped>();
		for (const entry of extract.unmapped) {
			if (!byCategory.has(entry.category)) {
				byCategory.set(entry.category, []);
			}
			const cat = byCategory.get(entry.category);
			if (cat) {
				cat.push(entry);
			}
		}

		for (const [category, entries] of byCategory) {
			console.log(`  ${category}:`);
			for (const entry of entries) {
				console.log(`    - ${entry.source}: ${entry.reason}`);
			}
		}
	}

	// Report inventory
	if (extract.inventory.persona.length > 0 || extract.inventory.schedules.length > 0) {
		console.log("\nMigration summary:");
		console.log(`  Personas: ${extract.inventory.persona.length}`);
		console.log(`  Schedules: ${extract.inventory.schedules.length}`);
		console.log(`  Memory entries: ${extract.inventory.memory.length}`);
		console.log(`  Skills: ${extract.inventory.skills.length}`);
		console.log(`  Channels: ${extract.inventory.channels.length}`);
	}

	if (options.dryRun) {
		console.log("\n[DRY RUN] Output would be written to:");
		for (const file of extract.files) {
			console.log(`  ${file.target} (${file.category})`);
		}
		return;
	}

	// Write files
	await mkdir(targetHome, { recursive: true });

	// Create channel plan and emit configurations
	const channels = emptyChannelPlan();
	emitChannels(extract, channels, targetHome, sourceType);
	emitMonitors(extract);
	emitImportedAxis(extract);

	// Write source files
	for (const file of extract.files) {
		const filePath = join(targetHome, file.target);
		await mkdir(join(filePath, ".."), { recursive: true });
		await writeFile(filePath, file.content);
	}

	// Write merged JSON files
	for (const merge of extract.merges) {
		const filePath = join(targetHome, merge.target);
		await mkdir(join(filePath, ".."), { recursive: true });
		let existing: Record<string, unknown> | undefined;
		try {
			const text = await readFile(filePath, "utf-8");
			existing = JSON.parse(text) as Record<string, unknown>;
		} catch {
			// File doesn't exist or is not valid JSON, start fresh
		}
		const merged = merge.merge(existing);
		await writeFile(filePath, JSON.stringify(merged, null, 2));
	}

	console.log(`\nMigration complete. Output written to ${targetHome}`);
}

/**
 * Parse command-line arguments for the migrate command.
 */
export function parseMigrateArgs(args: readonly string[]): MigrateOptions {
	const options: MigrateOptions = {};

	for (let i = 0; i < args.length; i++) {
		const arg = args[i];

		if (arg === "--source" && args[i + 1]) {
			options.source = args[++i];
		} else if (arg === "--target" && args[i + 1]) {
			options.target = args[++i];
		} else if (arg === "--dry-run") {
			options.dryRun = true;
		} else if (arg.startsWith("--")) {
			throw new Error(`Unknown option: ${arg}`);
		} else if (!options.source) {
			options.source = arg;
		} else {
			throw new Error(`Unexpected argument: ${arg}`);
		}
	}

	return options;
}
