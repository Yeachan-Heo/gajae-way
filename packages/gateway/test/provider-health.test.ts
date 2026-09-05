/**
 * I7b acceptance (Q2): passive + active fusion. Injected 403 (auth) and 301
 * (redirect, the live http->https incident) set the gate on their own; a 200
 * from /models never clears a sustained inference failure; the gate clears only
 * when the active probe is ok AND the passive signal has recovered.
 */
import { expect, test } from "bun:test";
import { ProviderHealth, isProviderFailureCode, probeProvider } from "../src/provider/probe";

test("provider failure codes: provider_* family, prompt_failed, agent_error", () => {
	for (const code of [
		"provider_rejected",
		"provider_http_502",
		"provider_down",
		"provider_unavailable",
		"prompt_failed",
		"agent_error",
	])
		expect(isProviderFailureCode(code)).toBe(true);
	for (const code of ["invalid_input", "session_unavailable", undefined, "operation_lost"])
		expect(isProviderFailureCode(code)).toBe(false);
});

test("passive: two consecutive provider-class failures within 10 min set the gate; a lone failure does not", () => {
	let now = 0;
	const logs: string[] = [];
	const health = new ProviderHealth({ now: () => now, log: (l) => logs.push(l) });
	health.recordTurn({ ok: false, code: "provider_rejected" });
	expect(health.gated).toBe(false);
	now += 11 * 60_000;
	health.recordTurn({ ok: false, code: "provider_rejected" });
	expect(health.gated).toBe(false); // window reset: consecutive count restarted
	now += 1_000;
	health.recordTurn({ ok: false, code: "provider_rejected" });
	expect(health.gated).toBe(true);
	expect(health.status().provenance.setBy).toBe("passive");
	expect(logs.some((l) => l.startsWith("provider_failing set_by=passive"))).toBe(true);
});

test("active auth (403) and redirect (301) set the gate by themselves; server/unreachable do not", () => {
	for (const [cls, expectGate] of [
		["auth", true],
		["redirect", true],
		["server", false],
		["unreachable", false],
	] as const) {
		const health = new ProviderHealth({ log: () => {} });
		health.recordActive({ at: 0, httpStatus: 0, class: cls });
		expect([cls, health.gated]).toEqual([cls, expectGate]);
		if (expectGate) expect(health.status().provenance.setBy).toBe("active");
	}
});

test("disagreement: /models 200 never clears a sustained inference failure; clear needs active ok AND passive recovery", () => {
	let now = 0;
	const health = new ProviderHealth({ now: () => now, log: () => {} });
	health.recordTurn({ ok: false, code: "provider_rejected" });
	health.recordTurn({ ok: false, code: "provider_rejected" });
	expect(health.gated).toBe(true);
	health.recordActive({ at: now, httpStatus: 200, class: "ok" });
	expect(health.gated).toBe(true);
	// Still failing turns keep it gated even with active ok.
	now += 60_000;
	health.recordTurn({ ok: false, code: "prompt_failed" });
	health.recordActive({ at: now, httpStatus: 200, class: "ok" });
	expect(health.gated).toBe(true);
	// A passive success after the last failure + active ok clears, provenance recorded.
	now += 60_000;
	health.recordTurn({ ok: true });
	expect(health.gated).toBe(false);
	expect(health.status().provenance.clearedBy).toBe("active");
	// Passive success alone (active not ok) does not clear.
	const other = new ProviderHealth({ now: () => now, log: () => {} });
	other.recordActive({ at: now, httpStatus: 403, class: "auth" });
	other.recordTurn({ ok: true });
	expect(other.gated).toBe(true);
	// No passive failure in 10 min + active ok clears an active-set gate.
	now += 11 * 60_000;
	other.recordActive({ at: now, httpStatus: 200, class: "ok" });
	expect(other.gated).toBe(false);
});

test("cadence is 60 s while gated and 5 min otherwise", () => {
	const health = new ProviderHealth({ log: () => {} });
	expect(health.nextProbeDelayMs()).toBe(5 * 60_000);
	health.recordActive({ at: 0, httpStatus: 403, class: "auth" });
	expect(health.nextProbeDelayMs()).toBe(60_000);
});

test("probeProvider: manual redirect is a finding with the Location host only; 403 is auth; no body or key leaks", async () => {
	const seen: Array<{ url: string; auth: string | null; redirect: string | undefined }> = [];
	const fetchImpl = (async (url: string | URL | Request, init?: RequestInit) => {
		const target = String(url);
		seen.push({
			url: target,
			auth: (init?.headers as Record<string, string>)?.Authorization ?? null,
			redirect: init?.redirect,
		});
		if (target.startsWith("http://redirect.test"))
			return new Response("moved", {
				status: 301,
				headers: { location: "https://api.example.test/v1/models?token=secret" },
			});
		if (target.startsWith("https://auth.test")) return new Response("forbidden body with sk-secret", { status: 403 });
		if (target.startsWith("https://ok.test")) return new Response('{"data":[]}', { status: 200 });
		return new Response("boom", { status: 503 });
	}) as unknown as typeof fetch;
	const redirect = await probeProvider({ baseUrl: "http://redirect.test/v1", apiKey: "sk-live-key", fetchImpl });
	expect(redirect).toMatchObject({
		class: "redirect",
		httpStatus: 301,
		locationHost: "api.example.test",
		detail: "provider_redirect",
	});
	expect(JSON.stringify(redirect)).not.toContain("secret");
	expect(seen[0]).toMatchObject({
		url: "http://redirect.test/v1/models",
		auth: "Bearer sk-live-key",
		redirect: "manual",
	});
	expect(await probeProvider({ baseUrl: "https://auth.test/v1/", apiKey: "k", fetchImpl })).toMatchObject({
		class: "auth",
		httpStatus: 403,
	});
	expect(await probeProvider({ baseUrl: "https://ok.test/v1", apiKey: "k", fetchImpl })).toMatchObject({
		class: "ok",
		httpStatus: 200,
	});
	expect(await probeProvider({ baseUrl: "https://down.test/v1", apiKey: "k", fetchImpl })).toMatchObject({
		class: "server",
		httpStatus: 503,
	});
	expect(await probeProvider({ baseUrl: undefined, apiKey: "k", fetchImpl })).toMatchObject({ class: "unreachable" });
	for (const entry of seen) expect(JSON.stringify(entry)).not.toContain("forbidden body");
});
