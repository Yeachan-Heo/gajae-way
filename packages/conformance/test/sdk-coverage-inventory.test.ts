import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { EVENTS_V01, VERBS_V01 } from "@gajaeway/protocol";

/**
 * Action/event coverage inventory (spec AC-4, plan driver 1).
 *
 * Generated from source, not hand-maintained: every verb the gateway server
 * dispatches and every event it emits must exist in the protocol catalog, and
 * every catalog entry must be implemented by the gateway. An internal action
 * with no typed SDK surface fails this test the phase it is born.
 */

const ROOT = join(import.meta.dir, "..", "..", "..");
const GATEWAY_SRC = join(ROOT, "packages/gateway/src");

function collectSourceFiles(dir: string, out: string[] = []): string[] {
	let entries: string[];
	try {
		entries = readdirSync(dir);
	} catch {
		return out;
	}
	for (const entry of entries) {
		if (entry === "node_modules" || entry.startsWith(".")) continue;
		const full = join(dir, entry);
		if (statSync(full).isDirectory()) collectSourceFiles(full, out);
		else if (/\.ts$/.test(entry)) out.push(full);
	}
	return out;
}

/** Verb/event literals referenced in gateway source. */
function harvest(pattern: RegExp): Set<string> {
	const found = new Set<string>();
	for (const file of collectSourceFiles(GATEWAY_SRC)) {
		// The gjc vendor adapter speaks gjc's own SDK protocol (session.create
		// ops etc.), not the gajaeway profile; it is outside the inventory.
		if (file.endsWith("gjc-client.ts")) continue;
		// Monitor EVENT TYPE names (memory.canonicalize etc.) are data flowing through
		// the monitor pipeline, not protocol verbs/events; the seeded defaults and the
		// per-type authoring guidance reference them as plain strings.
		if (file.endsWith("monitors/defaults.ts") || file.endsWith("monitors/propagate.ts")) continue;
		const text = readFileSync(file, "utf8");
		for (const match of text.matchAll(pattern)) {
			const name = match[1];
			if (name && !NON_VERB_SUFFIX_RE.test(name)) found.add(name);
		}
	}
	return found;
}

// Namespaced dotted literals in quotes: "gateway.status", "chat.message", ...
// File-ish literals (gateway.sock, gateway.db, *.json, ...) are not verbs.
const VERB_LITERAL_RE = /["'`]((?:gateway|chat|session|memory|monitor|delivery|engagement|ops|work)\.[a-zA-Z0-9_.]+)["'`]/g;
const NON_VERB_SUFFIX_RE = /\.(sock|db|json|jsonl|sqlite|md|ts|js|log|lock|pid)$/;

describe("sdk-coverage-inventory", () => {
	test("every catalog verb is implemented by the gateway", () => {
		const referenced = harvest(VERB_LITERAL_RE);
		const missing = VERBS_V01.filter((verb) => !referenced.has(verb));
		expect(missing).toEqual([]);
	});

	test("every catalog event is emitted by the gateway", () => {
		const referenced = harvest(VERB_LITERAL_RE);
		const missing = EVENTS_V01.filter((event) => !referenced.has(event));
		expect(missing).toEqual([]);
	});

	test("gateway references no verb/event missing from the catalog", () => {
		const catalog = new Set<string>([...VERBS_V01, ...EVENTS_V01]);
		const referenced = harvest(VERB_LITERAL_RE);
		const uncatalogued = [...referenced].filter((name) => !catalog.has(name));
		expect(uncatalogued).toEqual([]);
	});
});
