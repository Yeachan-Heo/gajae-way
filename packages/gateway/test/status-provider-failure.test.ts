/** a81bb27; fixture fake-gjc version 1: status:failed:provider_rejected, probe HTTP 403 plus disagreement HTTP 200.
 * Failing assertions: expect(status).toMatchObject({ provider: { passive: { lastCode: "provider_rejected" } } }); expect(cycle.gates).toContain("provider_failing"); expect(notice).toMatch(/^\[turn failed\] provider_rejected:/).
 * HEAD: no provider key, gates [], notice "[turn failed] provider_rejected" (no colon); post-fix: passive code, failing gate, coded notice with colon.
 * Matched log: [turn failed] provider_rejected while status healthy. S11's generic notice is not reproduced by this coded fixture.
 * The local HTTP probe is independent evidence only: HEAD exposes no provider-probe integration seam.
 */
import { expect, test } from "bun:test";
import { startUnixServer } from "../src/server/server";
import { ScriptedSessionPort } from "./session-port.fake";
import { createFakeGjc } from "./fixtures/fake-gjc.mjs";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { eventually, harness } from "./red-first-harness";

test("red 6: repeated provider failures remain visible when an active probe disagrees", async () => {
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => scripted.fail(input.opRef, "Prompt submission failed."),
	});
	const h = await harness(port);
	const fake = createFakeGjc({ modes: "status:failed:provider_rejected" });
	const run = async (args: readonly string[]) => (await fake(args))!;
	const real = new BrokerSessionPort({
		database: h.database,
		cli: run,
		instanceId: "red-provider",
		tailRunner: new TailRunner({ run, repo: h.repo }),
	});
	port.status = real.status.bind(real);
	let probeStatus = 403;
	const probe = Bun.serve({ port: 0, fetch: () => new Response("fixture provider", { status: probeStatus }) });
	const server = await startUnixServer({
		database: h.database,
		sessionPort: port,
		config: {
			schemaVersion: 1,
			home: h.home,
			configPath: `${h.home}/config.json`,
			socketPath: `${h.home}/gateway.sock`,
			dbPath: `${h.home}/gateway.db`,
			logVerbosity: "info",
			dmPolicy: "open",
		},
	});
	const frames: Array<{ id?: string; result?: any; payload?: { text?: string } }> = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: `${h.home}/gateway.sock`,
		socket: {
			data(_socket, data) {
				buffered += new TextDecoder().decode(data);
				let newline: number;
				while ((newline = buffered.indexOf("\n")) >= 0) {
					frames.push(JSON.parse(buffered.slice(0, newline)));
					buffered = buffered.slice(newline + 1);
				}
			},
		},
	});
	const send = (frame: unknown) => socket.write(`${JSON.stringify(frame)}\n`);
	try {
		send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await eventually(() => frames.length > 0, "negotiation failed");
		for (let i = 0; i < 2; i++) {
			send({
				v: "0.1",
				type: "request",
				id: `turn-${i}`,
				verb: "chat.send",
				params: {
					origin: { platform: "discord", kind: "dm", conversationId: "red-provider", peerId: "fixture" },
					text: `fail-${i}`,
					engagement: { mentioned: false, group: false, authorId: "fixture" },
				},
			});
			await eventually(
				() => frames.filter((f) => f.payload?.text?.startsWith("[turn failed]")).length >= i + 1,
				"failure notice missing",
			);
		}
		expect((await fetch(probe.url)).status).toBe(403);
		probeStatus = 200;
		expect((await fetch(probe.url)).status).toBe(200);
		send({ v: "0.1", type: "request", id: "status", verb: "gateway.status" });
		send({ v: "0.1", type: "request", id: "cycle", verb: "ops.cycle" });
		await eventually(() => frames.some((f) => f.id === "cycle"), "cycle missing");
		const status = frames.find((f) => f.id === "status")!.result;
		const cycle = frames.find((f) => f.id === "cycle")!.result;
		const notice = frames.find((f) => f.payload?.text?.startsWith("[turn failed]"))!.payload!.text!;
		console.info("red6", { status, gates: cycle.gates, notice });
		expect(status).toMatchObject({ provider: { passive: { lastCode: "provider_rejected" } } });
		expect(cycle.gates).toContain("provider_failing");
		expect(notice).toMatch(/^\[turn failed\] provider_rejected:/);
	} finally {
		socket.end();
		await server.stop();
		probe.stop(true);
		await h.close();
	}
});
