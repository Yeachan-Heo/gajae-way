import { describe, expect, test } from "bun:test";
import { DEFAULT_ALLOWLIST } from "../src/gate";
import { TurnTracker } from "../src/turns";
import { renderIndex } from "../src/ui";
import { buildSnapshot, type ConsoleSnapshot } from "../src/view";
import { FIXED_NOW, MONITOR, monitorEvent, SESSIONS, STATUS } from "./fixture";

async function state(monitors = [MONITOR]): Promise<ConsoleSnapshot> {
	return buildSnapshot({
		request: async (method, params) => {
			switch (method) {
				case "gateway.status":
					return STATUS;
				case "session.list":
					return SESSIONS;
				case "monitor.list":
					return { monitors };
				case "monitor.inspect": {
					const monitorId = (params as { monitorId: string }).monitorId;
					const monitor = monitors.find((candidate) => candidate.monitorId === monitorId);
					if (!monitor) throw new Error("unknown monitorId");
					return { monitor, recentEvents: [monitorEvent({ monitorId })] };
				}
				default:
					throw new Error(`unexpected verb ${method}`);
			}
		},
		turns: new TurnTracker(() => FIXED_NOW.getTime()),
		now: () => FIXED_NOW,
	});
}

async function html(monitors?: (typeof MONITOR)[]): Promise<string> {
	return renderIndex(await state(monitors), DEFAULT_ALLOWLIST);
}

/** Everything between `<main>` and `</main>`: the primary surfaces. */
function main(document: string): string {
	return document.slice(document.indexOf("<main>"), document.indexOf("</main>"));
}

describe("document shape", () => {
	test("is one self-contained document with no external asset of any kind", async () => {
		const document = await html();
		expect(document).toStartWith("<!doctype html>");
		expect(document).not.toContain("<link");
		expect(document).not.toContain("src=");
		expect(document).not.toContain("@import");
		expect(document).not.toContain("//fonts.");
		expect(document).not.toContain("url(");
	});

	test("declares both schemes and a phone-first viewport", async () => {
		const document = await html();
		expect(document).toContain("color-scheme: dark light");
		expect(document).toContain('content="width=device-width, initial-scale=1"');
	});

	test("exposes a typographic and a spacing scale as custom properties", async () => {
		const document = await html();
		for (const token of ["--text-xs:", "--text-sm:", "--text-md:", "--text-lg:", "--text-xl:"]) {
			expect(document).toContain(token);
		}
		for (const token of ["--space-1:", "--space-2:", "--space-3:", "--space-4:", "--space-5:", "--space-6:"]) {
			expect(document).toContain(token);
		}
	});

	test("reserves colour for status semantics and derives it from light-dark()", async () => {
		const document = await html();
		for (const token of ["--ok:", "--warn:", "--danger:", "--active:"]) {
			expect(document).toContain(token);
		}
		expect(document).toContain("light-dark(");
	});

	test("sizes every interactive control to a real tap target", async () => {
		const document = await html();
		expect(document).toContain("--tap: 2.75rem");
		expect(document).toContain("min-height: var(--tap)");
	});

	test("goes multi-column only above a phone width", async () => {
		const document = await html();
		expect(document).toContain("@media (min-width: 56rem)");
		expect(document).toContain("repeat(auto-fit, minmax(24rem, 1fr))");
	});
});

describe("no raw JSON on a primary surface", () => {
	test("main carries no serialised protocol object", async () => {
		const surface = main(await html());
		expect(surface).not.toContain("schemaVersion");
		expect(surface).not.toContain("profileVersion");
		expect(surface).not.toContain('"monitorId"');
		expect(surface).not.toContain("4242");
		expect(surface).not.toContain("<pre");
	});

	test("raw results exist exactly once, behind an explicit disclosure in the footer", async () => {
		const document = await html();
		expect(document.match(/<pre id="raw-json">/g)).toHaveLength(1);
		const footer = document.slice(document.indexOf("<footer>"));
		expect(footer).toContain('<details class="raw">');
		expect(footer).toContain("<summary>Raw protocol results</summary>");
		expect(footer).toContain("schemaVersion");
	});
});

describe("panels", () => {
	test("the IA follows the specified home ordering", async () => {
		const surface = main(await html());
		const order = [
			"panel-attention",
			"panel-live",
			"panel-conversation",
			"panel-sessions",
			"panel-monitors",
			"panel-ops",
		];
		let cursor = -1;
		for (const id of order) {
			const at = surface.indexOf(id);
			expect(at).toBeGreaterThan(cursor);
			cursor = at;
		}
	});

	test("the first paint is complete without JavaScript: rows are server-rendered", async () => {
		const document = await html();
		const surface = main(document);
		expect(surface).toContain("weekday-review");
		expect(surface).toContain("weekdays 08:30");
		expect(surface).toContain("discord channel · 1493…5762");
		// The status bar is a sticky header above main, and it is server-rendered too.
		expect(document.slice(0, document.indexOf("<main>"))).toContain("alive 4d 6h");
	});

	test("every reconciled panel ships the row markup once, as a template", async () => {
		const document = await html();
		for (const id of ["attention", "live", "sessions", "monitors", "audit"]) {
			expect(document).toContain(`<template data-template="${id}">`);
		}
	});

	test("a blocked panel names its gap instead of showing an empty box", async () => {
		const surface = main(await html());
		expect(surface).toContain("The conversation projection needs a transcript verb");
		expect(surface).toContain("session.transcript");
		expect(surface).toContain("Not shown here (1)");
	});

	test("the audit trail opens in a loading state rather than a false empty one", async () => {
		const document = await html();
		expect(document).toContain('id="panel-audit" data-panel="audit" data-state="loading"');
		expect(document).toContain("Reading the audit trail…");
	});

	test("a row without a meter hides it, rather than leaving an empty bar", async () => {
		const document = await html();
		// display:block on .row__meter would otherwise beat the UA's [hidden] rule.
		expect(document).toContain(".row__meter[hidden]{ display:none; }");
	});
});

describe("the confirmation is not habituated", () => {
	test("no input asks for the operation id, and no option row hands it over", async () => {
		const document = await html();
		expect(document).not.toContain("type the operation id");
		expect(document).not.toContain('<option value="monitor.remove">monitor.remove');
		expect(document).not.toContain("params (json)");
	});

	test("the destructive option is labelled by consequence, not by verb name", async () => {
		const document = await html();
		expect(document).toContain('<option value="monitor.remove">Remove a monitor (destructive)</option>');
	});

	test("a deployment with mutations disabled says so on the panel", async () => {
		const document = renderIndex(await state(), DEFAULT_ALLOWLIST, false);
		expect(document).toContain("Mutations are disabled for this deployment");
	});

	test("the console states that chat.send cannot be reached from it", async () => {
		const document = await html();
		expect(document).toContain("<code>chat.send</code> is not on the mutation allowlist");
	});
});

describe("escaping", () => {
	test("hostile gateway data cannot become markup", async () => {
		const document = await html([{ ...MONITOR, name: `<img src=x onerror="alert(1)">`, monitorId: "mon-evil" }]);
		expect(document).not.toContain("<img src=x");
		expect(document).toContain("&lt;img src=x onerror=&quot;alert(1)&quot;&gt;");
	});

	test("the inlined snapshot cannot close its own script element", async () => {
		const document = await html([{ ...MONITOR, name: "</script><script>alert(1)</script>", monitorId: "mon-evil" }]);
		const bootstrap = document.slice(document.indexOf('id="bootstrap"'));
		expect(bootstrap).not.toContain("</script><script>alert(1)");
		expect(bootstrap).toContain("\\u003c/script");
	});
});
