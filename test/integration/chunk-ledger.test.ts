import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { ChunkLedgerError, FileChunkLedger, MemoryChunkLedger } from "../../src/adapter/chunk-ledger";

/**
 * A multi-chunk reply commits its journal event only after every chunk is sent,
 * so an interruption mid-set replays the whole set. Deterministic nonces let
 * Discord suppress a PROMPT duplicate, but that deduplication is time-bounded:
 * after a long outage the earlier chunks would post again. These tests cover the
 * durable record that makes the skip decision independent of elapsed time.
 */
test("a durable chunk ledger survives a restart so a delayed replay skips confirmed chunks", () => {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-chunk-ledger-"));
	try {
		const first = new FileChunkLedger(dir, "discord-chunk-ledger.json");
		expect(first.recorded("event:7:chunk:0")).toBeUndefined();
		first.record("event:7:chunk:0", "platform-msg-1");

		// A brand-new instance models the adapter restarting long after Discord's
		// nonce window has expired; the confirmed chunk must still be known.
		const restarted = new FileChunkLedger(dir, "discord-chunk-ledger.json");
		expect(restarted.recorded("event:7:chunk:0")).toBe("platform-msg-1");
		expect(restarted.recorded("event:7:chunk:1")).toBeUndefined();
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});

test("a corrupt chunk ledger fails closed instead of silently losing duplicate protection", () => {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-chunk-ledger-bad-"));
	try {
		const file = path.join(dir, "discord-chunk-ledger.json");
		fs.writeFileSync(file, "{not json");
		expect(() => new FileChunkLedger(dir, "discord-chunk-ledger.json")).toThrow(ChunkLedgerError);

		fs.writeFileSync(file, JSON.stringify({ "event:1:chunk:0": 42 }));
		expect(() => new FileChunkLedger(dir, "discord-chunk-ledger.json")).toThrow(/malformed entry/);

		// An ABSENT ledger remains a legitimate fresh start.
		fs.rmSync(file);
		expect(() => new FileChunkLedger(dir, "discord-chunk-ledger.json")).not.toThrow();
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});

test("the in-memory chunk ledger stays bounded", () => {
	const ledger = new MemoryChunkLedger();
	for (let index = 0; index < 5_050; index += 1) ledger.record(`k:${index}`, `m:${index}`);
	// The oldest entries are evicted; the newest are retained.
	expect(ledger.recorded("k:5049")).toBe("m:5049");
	expect(ledger.recorded("k:0")).toBeUndefined();
});
