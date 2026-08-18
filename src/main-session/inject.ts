import * as fs from "node:fs";
import * as path from "node:path";
import type { SessionKind, WayProfile } from "../profile";

export interface ContextFile {
	readonly path: string;
	readonly content: string;
}

export interface InjectionPlan {
	readonly files: readonly string[];
}

export interface InjectionLogEntry {
	readonly kind: "missing" | "restricted";
	readonly sessionKind: SessionKind;
	readonly path: string;
}

export interface AssembleInjectionOptions {
	readonly sessionKind?: SessionKind;
	readonly now?: Date;
	readonly onLog?: (entry: InjectionLogEntry) => void;
	readonly readFile?: (filePath: string) => string;
}

function localDate(date: Date): string {
	const year = date.getFullYear();
	const month = String(date.getMonth() + 1).padStart(2, "0");
	const day = String(date.getDate()).padStart(2, "0");
	return `${year}-${month}-${day}`;
}

function dayBefore(date: Date, days: number): Date {
	const result = new Date(date.getTime());
	result.setDate(result.getDate() - days);
	return result;
}

function dateCandidates(configuredPath: string, now: Date): string[] {
	if (!configuredPath.includes("{date}")) return [configuredPath];
	return [0, 1, 2].map(days => configuredPath.replaceAll("{date}", localDate(dayBefore(now, days))));
}

function corpusFilePath(corpusPath: string, configuredPath: string): string {
	const resolved = path.resolve(corpusPath, configuredPath);
	const relative = path.relative(corpusPath, resolved);
	if (relative === "" || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
		throw new Error(`Injection path escapes corpus path: ${configuredPath}`);
	}
	return resolved;
}

function isRestricted(configuredPath: string, rules: readonly string[]): boolean {
	const normalized = configuredPath.replaceAll("\\", "/");
	const name = path.posix.basename(normalized);
	return rules.some(rule => rule === normalized || rule === name);
}

/**
 * Resolves the profile's ordered injection list. `{date}` expands to today then
 * the prior two local calendar days; missing daily files are observable skips,
 * while all other filesystem failures remain hard errors.
 */
export function assembleInjection(profile: WayProfile, options: AssembleInjectionOptions = {}): ContextFile[] {
	const sessionKind = options.sessionKind ?? "main";
	const now = options.now ?? new Date();
	const log = options.onLog ?? (() => undefined);
	const readFile = options.readFile ?? (filePath => fs.readFileSync(filePath, "utf8"));
	const contextFiles: ContextFile[] = [];
	for (const configuredPath of profile.injection.files) {
		for (const candidate of dateCandidates(configuredPath, now)) {
			if (isRestricted(candidate, profile.restrictedFiles[sessionKind])) {
				log({ kind: "restricted", sessionKind, path: candidate });
				continue;
			}
			const filePath = corpusFilePath(profile.corpusPath, candidate);
			try {
				contextFiles.push({ path: candidate, content: readFile(filePath) });
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code === "ENOENT") {
					log({ kind: "missing", sessionKind, path: candidate });
					continue;
				}
				throw error;
			}
		}
	}
	return contextFiles;
}

export function injectionPlan(profile: WayProfile, options: Pick<AssembleInjectionOptions, "now"> = {}): InjectionPlan {
	const now = options.now ?? new Date();
	return { files: profile.injection.files.flatMap(file => dateCandidates(file, now)) };
}