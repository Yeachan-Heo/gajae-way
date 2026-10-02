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

type UiEvent = { readonly data?: string };
type UiListener = (event: UiEvent) => void;

class UiNode {
	textContent = "";
	value = "";
	hidden = false;
	readonly dataset: Record<string, string> = {};
	readonly attributes = new Map<string, string>();
	readonly children = new Map<string, UiNode>();
	readonly listeners = new Map<string, UiListener>();
	firstElementChild: UiNode | null = null;
	content = { firstElementChild: null as UiNode | null };

	constructor(state = "") {
		if (state) this.attributes.set("data-state", state);
	}

	addEventListener(name: string, listener: UiListener): void {
		this.listeners.set(name, listener);
	}

	getAttribute(name: string): string | null {
		return this.attributes.get(name) ?? null;
	}

	hasAttribute(name: string): boolean {
		return this.attributes.has(name);
	}

	setAttribute(name: string, value: string): void {
		this.attributes.set(name, value);
	}

	removeAttribute(name: string): void {
		this.attributes.delete(name);
	}

	querySelector(selector: string): UiNode | null {
		return this.children.get(selector) ?? null;
	}

	querySelectorAll(_selector: string): UiNode[] {
		return [];
	}

	replaceChildren(): void {
		this.firstElementChild = null;
	}

	appendChild(child: UiNode): void {
		if (!this.firstElementChild) this.firstElementChild = child;
	}
}

function runInlineHealthScript(markup: string, snapshot: ConsoleSnapshot) {
	const scriptStart = markup.indexOf("<script>");
	if (scriptStart < 0) throw new Error("inline console script is missing");
	const bodyStart = scriptStart + "<script>".length;
	const bodyEnd = markup.indexOf("</script>", bodyStart);
	if (bodyEnd < 0) throw new Error("inline console script is unterminated");
	const streamState = new UiNode();
	const statusBar = new UiNode();
	const livePanel = new UiNode("ready");
	const sections = new Map<string, UiNode>();
	for (const name of ["attention", "live", "sessions", "monitors", "audit"]) {
		const section = new UiNode("ready");
		section.children.set("[data-note]", new UiNode());
		section.children.set("[data-count]", new UiNode());
		// This harness exercises stream health, not keyed row reconciliation.
		// Omit row containers so the real reconciliation guard skips that surface.
		sections.set(name, section);
	}
	const nodes = new Map<string, UiNode>([
		["bootstrap", Object.assign(new UiNode(), { textContent: JSON.stringify(snapshot) })],
		["ops-meta", Object.assign(new UiNode(), { textContent: JSON.stringify({ operations: [] }) })],
		["stream-state", streamState],
		["statusbar", statusBar],
		["raw-json", new UiNode()],
		["ops", new UiNode()],
		["ops-op", new UiNode()],
		["ops-fields", new UiNode()],
		["ops-summary", new UiNode()],
		...["ops-next", "ops-cancel", "ops-again", "ops-run", "ops-actor"].map((id) => [id, new UiNode()] as const),
	]);
	const document = {
		getElementById: (id: string) => nodes.get(id) ?? null,
		querySelector: (selector: string) => {
			const match = selector.match(/^\[data-panel="([^"]+)"\]$/);
			return match ? (sections.get(match[1]!) ?? null) : null;
		},
		querySelectorAll: (selector: string) => (selector === '.panel[data-live="1"]' ? [livePanel] : []),
	};
	const eventListeners = new Map<string, UiListener>();
	const source = {
		addEventListener: (name: string, listener: UiListener) => eventListeners.set(name, listener),
	};
	const EventSource = function (_url: string) {
		return source;
	};
	const fetch = () => new Promise<{ json: () => Promise<unknown> }>(() => {});
	const setInterval = (_callback: () => void, _delay: number): number => 0;
	new Function("document", "EventSource", "fetch", "setInterval", markup.slice(bodyStart, bodyEnd))(
		document,
		EventSource,
		fetch,
		setInterval,
	);
	return {
		streamState,
		livePanel,
		emit(name: string, data?: unknown) {
			eventListeners.get(name)?.({ data: JSON.stringify(data) });
		},
	};
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

describe("stream health", () => {
	test("inline handlers distinguish gateway loss and wait for a recovery snapshot", async () => {
		const snapshot = await state();
		const page = runInlineHealthScript(await html(), snapshot);
		const outagePage = runInlineHealthScript(await html(), {
			...snapshot,
			gateway: { reachable: false, error: "gateway disconnected" },
		});
		expect(outagePage.streamState.textContent).toContain("gateway disconnected");

		page.emit("open");
		expect(page.streamState.textContent).toMatch(/^stream .+ data /);
		page.emit("error");
		expect(page.streamState.textContent).toContain("console stream lost");
		expect(page.streamState.textContent).not.toContain("gateway disconnected");

		page.emit("open");
		page.emit("gateway.stopping");
		expect(page.streamState.textContent).toContain("gateway is stopping");
		page.emit("gateway.connection", { connected: false });
		expect(page.streamState.textContent).toContain("gateway disconnected");
		expect(page.livePanel.getAttribute("data-stale")).toBe("1");

		// A successful response already in flight when the local socket dropped is stale.
		page.emit("snapshot", snapshot);
		expect(page.streamState.textContent).toContain("gateway disconnected");

		// Recovery while the browser is offline arrives only in its next full snapshot.
		page.emit("error");
		page.emit("open");
		expect(page.streamState.textContent).toContain("waiting for a fresh snapshot");
		page.emit("snapshot", snapshot);
		expect(page.streamState.textContent).toMatch(/^stream .+ data /);

		page.emit("gateway.connection", { connected: false });
		expect(page.streamState.textContent).toContain("gateway disconnected");

		page.emit("gateway.connection", { connected: true });
		expect(page.streamState.textContent).toContain("waiting for a fresh snapshot");
		expect(page.streamState.textContent).not.toContain("gateway is stopping");
		expect(page.streamState.textContent).not.toMatch(/^stream .+ data /);

		page.emit("snapshot", snapshot);
		expect(page.streamState.textContent).toMatch(/^stream .+ data /);
		expect(page.livePanel.getAttribute("data-stale")).toBeNull();
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
