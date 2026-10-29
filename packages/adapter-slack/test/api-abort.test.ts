import { expect, test } from "bun:test";
import { SlackWebApi } from "../src/api";

test("Slack call with AbortSignal passes the signal to fetch and respects abort", async () => {
	const signals: AbortSignal[] = [];
	const api = new SlackWebApi("xoxb-secret", async (_input, init) => {
		if (init?.signal) signals.push(init.signal);
		// Simulate a hanging request that is abortable
		return new Promise((_resolve, reject) => {
			if (init?.signal) {
				init.signal.addEventListener("abort", () => {
					reject(new DOMException("Aborted", "AbortError"));
				});
			}
		});
	});

	const controller = new AbortController();
	const promise = api.connectionsOpen("xapp-secret", controller.signal);

	// Give the request time to be captured
	await Promise.resolve();
	await Promise.resolve();

	expect(signals).toHaveLength(1);
	const signal = signals[0];
	expect(signal?.aborted).toBe(false);

	// Abort the controller to simulate timeout
	controller.abort();
	await Promise.resolve();
	expect(signal?.aborted).toBe(true);

	// The promise should be rejected
	let rejected = false;
	await promise.catch(() => {
		rejected = true;
	});
	expect(rejected).toBe(true);
});

test("Slack connectionsOpen without signal works normally", async () => {
	const api = new SlackWebApi("xoxb-secret", async (_input, _init) => {
		return Response.json({ ok: true, url: "wss://slack.test" });
	});

	const result = await api.connectionsOpen("xapp-secret");
	expect(result.url).toBe("wss://slack.test");
});
