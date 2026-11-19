import { describe, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ProtocolError } from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import { main } from "../src/main";

// Exercise protocol serialization, real SDK decoding, and the CLI process catch.
async function runRefusal(command: string, detail: unknown, message = "work send rejected", code = "verb_failed") {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-refusal-"));
	const path = join(home, "gateway.sock");
	const requests: string[] = [];
	const payload = new ProtocolError("verb_failed", message, detail).toPayload();
	const listener = Bun.listen<{ buffer: string }>({
		unix: path,
		socket: {
			open(socket) {
				socket.data = { buffer: "" };
			},
			data(socket, data) {
				socket.data.buffer += Buffer.from(data).toString("utf8");
				let newline = socket.data.buffer.indexOf("\n");
				while (newline >= 0) {
					const frame = JSON.parse(socket.data.buffer.slice(0, newline));
					socket.data.buffer = socket.data.buffer.slice(newline + 1);
					if (frame.type === "hello")
						socket.write(`${JSON.stringify({ v: "0.1", type: "negotiated", payload: { profileVersion: "v0.1" } })}\n`);
					else if (frame.type === "request") {
						requests.push(frame.verb);
						socket.write(`${JSON.stringify({ v: "0.1", type: "error", id: frame.id, error: { ...payload, code } })}\n`);
					}
					newline = socket.data.buffer.indexOf("\n");
				}
			},
		},
	});
	try {
		const child = Bun.spawn(
			[
				process.execPath,
				join(import.meta.dir, "../src/main.ts"),
				"--socket",
				path,
				"work",
				command,
				"fix",
				...(command === "status" ? [] : ["task"]),
			],
			{
				stdin: "ignore",
				stdout: "pipe",
				stderr: "pipe",
				env: { ...process.env, GAJAEWAY_HOME: home },
			},
		);
		const [stdout, stderr, exitCode] = await Promise.all([
			new Response(child.stdout).text(),
			new Response(child.stderr).text(),
			child.exited,
		]);
		return { stdout, stderr, exitCode, requests };
	} finally {
		listener.stop(true);
		await rm(home, { recursive: true, force: true });
	}
}

const safeCauses = [
	"busy",
	"steer_refused",
	"invalid_params",
	"not_running",
	"no_active_turn",
	"client_ref_conflict",
	"session_not_found",
	"sdk_refused",
];

describe("work refusal CLI protocol boundary", () => {
	for (const command of ["run", "start"]) {
		for (const cause of safeCauses) {
			test(`${command} renders only allowlisted ${cause}`, async () => {
				const result = await runRefusal(command, {
					reasonCode: "send_rejected",
					cause,
					diagnostic: "SECRET",
					jobId: "SECRET",
					sessionId: "SECRET",
				});
				expect(result).toEqual({
					stdout: "",
					stderr: `work send rejected cause=${cause}\n`,
					exitCode: 1,
					requests: [`work.${command}`],
				});
			});
		}
	}
	for (const cause of [undefined, "SECRET", "busy\nSECRET", { code: "busy", message: "SECRET" }, ["busy"], null, 1]) {
		test(`omits absent, unrecognized or malformed cause ${JSON.stringify(cause)}`, async () => {
			const result = await runRefusal("run", { reasonCode: "send_rejected", cause, message: "SECRET" });
			expect(result.stderr).toBe("work send rejected\n");
			expect(result.exitCode).toBe(1);
		});
	}
	test("does not print arbitrary refusal message or detail", async () => {
		const result = await runRefusal("run", { reasonCode: "send_rejected", cause: "busy", message: "SECRET" }, "SECRET");
		expect(result.stderr).toBe("work send rejected cause=busy\n");
	});
	for (const detail of [undefined, null, [], "busy", { cause: "busy" }, { reasonCode: "sdk_failed", cause: "busy" }]) {
		test(`ignores unrelated detail ${JSON.stringify(detail)}`, async () => {
			const result = await runRefusal("run", detail);
			expect(result.stderr).toBe("work send rejected\n");
		});
	}
	test("renders and redacts refusal errors from a separately loaded protocol package", async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-nested-protocol-"));
		const copy = join(directory, "errors.ts");
		await Bun.write(copy, Bun.file(join(import.meta.dir, "../../protocol/src/errors.ts")));
		const { ProtocolError: NestedProtocolError } = await import(copy);
		const client: GajaewayClient = Object.create(GajaewayClient.prototype);
		let failure: Error = new Error("unset");
		client.request = async () => {
			throw failure;
		};
		client.close = async () => {};
		const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(client);
		const errors: string[] = [];
		const logged = spyOn(console, "error").mockImplementation((line) => errors.push(String(line)));
		const previousExit = process.exitCode;
		try {
			for (const command of ["run", "start"]) {
				for (const [cause, expected] of [
					["busy", " cause=busy"],
					["sdk_refused", " cause=sdk_refused"],
					["SECRET", ""],
				]) {
					failure = new NestedProtocolError("verb_failed", "SECRET remote message", {
						reasonCode: "send_rejected",
						cause,
						diagnostic: "SECRET detail",
					});
					expect(failure).not.toBeInstanceOf(ProtocolError);
					await main(["--socket", "/unused", "work", command, "fix", "task"]);
					expect(errors.pop()).toBe(`work send rejected${expected}`);
					expect(process.exitCode).toBe(1);
				}
			}
			failure = new NestedProtocolError("invalid_params", "unrelated error", {
				reasonCode: "send_rejected",
				cause: "busy",
			});
			await main(["--socket", "/unused", "work", "run", "fix", "task"]);
			expect(errors.pop()).toBe("unrelated error");
		} finally {
			connect.mockRestore();
			logged.mockRestore();
			process.exitCode = previousExit ?? 0;
			await rm(directory, { recursive: true, force: true });
		}
	});
	test("does not reinterpret other verbs or error codes", async () => {
		for (const [command, code] of [
			["status", "verb_failed"],
			["run", "invalid_params"],
		]) {
			const result = await runRefusal(
				command as string,
				{ reasonCode: "send_rejected", cause: "busy" },
				"original error",
				code,
			);
			expect(result.stderr).toBe("original error\n");
		}
	});
});
