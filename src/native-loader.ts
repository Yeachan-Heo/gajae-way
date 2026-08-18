import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createRequire } from "node:module";
import { embeddedAddon } from "../native/embedded-addon";

export interface HealthInfo {
	version: string;
	bootEpoch: number;
}

export interface WayCoreHandle {
	readonly stateDir: string;
}

export interface WayCoreConstructor {
	open(stateDir: string): WayCoreHandle;
}

export interface WayCoreBindings {
	healthInfo(): HealthInfo;
	WayCore: WayCoreConstructor;
}

const requiredExports = ["healthInfo", "WayCore"] as const;

function platformTag(): string {
	return `${process.platform}-${process.arch}`;
}

function localAddonPath(): string {
	return path.join(import.meta.dir, "..", "native", `way_core.${platformTag()}.node`);
}

function materializeEmbeddedAddon(): string {
	if (!embeddedAddon) return localAddonPath();
	if (embeddedAddon.platformTag !== platformTag()) {
		throw new Error(`Embedded ${embeddedAddon.filename} cannot run on ${platformTag()}.`);
	}

	const cacheDir = path.join(os.tmpdir(), "gajae-way", "native", embeddedAddon.version, embeddedAddon.platformTag);
	const targetPath = path.join(cacheDir, embeddedAddon.filename);
	const sourceSize = fs.statSync(embeddedAddon.filePath).size;
	try {
		if (fs.statSync(targetPath).size === sourceSize) return targetPath;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}

	fs.mkdirSync(cacheDir, { recursive: true });
	const temporaryPath = `${targetPath}.tmp.${process.pid}`;
	fs.writeFileSync(temporaryPath, fs.readFileSync(embeddedAddon.filePath));
	try {
		fs.renameSync(temporaryPath, targetPath);
	} catch (renameError) {
		try {
			fs.rmSync(targetPath, { force: true });
			fs.renameSync(temporaryPath, targetPath);
		} catch (fallbackError) {
			fs.rmSync(temporaryPath, { force: true });
			throw new Error(
				`Could not materialize embedded addon ${embeddedAddon.filename}: ${
					fallbackError instanceof Error ? fallbackError.message : String(renameError)
				}`,
			);
		}
	}
	return targetPath;
}

function assertWayCoreBindings(
	bindings: Record<string, unknown>,
	addonPath: string,
): asserts bindings is WayCoreBindings & Record<string, unknown> {
	const missing = requiredExports.filter(symbol => typeof bindings[symbol] !== "function");
	if (missing.length > 0) {
		throw new Error(`Native addon ${addonPath} is missing required exports: ${missing.join(", ")}.`);
	}
}

/** Loads either the development addon or the addon embedded into a Bun standalone executable. */
export function loadWayCore(): WayCoreBindings {
	const addonPath = materializeEmbeddedAddon();
	const bindings = createRequire(import.meta.url)(addonPath) as Record<string, unknown>;
	assertWayCoreBindings(bindings, addonPath);
	return bindings;
}
