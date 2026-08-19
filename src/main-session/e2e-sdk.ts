import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { bootstrapNonceMarker, type HostedSdkGate, type HostedSdkGateResolution, type HostedSdkSession, type MainSessionSdk } from "./sdk";
import { fingerprintSessionFile, sameFingerprint, type SessionFingerprint } from "./state";

const E2E_SESSION_DIRECTORY = ".gajaeway-e2e-sessions";

/**
 * Deterministic, file-backed SDK used only by the portable compiled-binary
 * restart drill. `main.ts` enables it only with NODE_ENV=test and an explicit
 * GAJAEWAY_E2E_FILE_SDK=1 opt-in; deployed services always use the published SDK.
 */
export function createE2eFileSdk(): MainSessionSdk {
	return {
		async createNew(input) {
			const directory = sessionDirectory(input.workspace);
			fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
			return new E2eFileSession(path.join(directory, `${crypto.randomUUID()}.jsonl`), undefined);
		},
		async openExistingStrict(input) {
			const observed = fingerprintSessionFile(input.identity.canonicalPath);
			if (!sameFingerprint(observed, input.identity)) throw new Error("e2e SDK strict identity mismatch");
			return new E2eFileSession(observed.canonicalPath, observed.sessionId);
		},
		async findBootstrapNonceCandidates(workspace, nonce) {
			const directory = sessionDirectory(workspace);
			try {
				return fs
					.readdirSync(directory, { withFileTypes: true })
					.filter(entry => entry.isFile() && entry.name.endsWith(".jsonl"))
					.map(entry => path.join(directory, entry.name))
					.filter(candidate => fs.readFileSync(candidate, "utf8").includes(bootstrapNonceMarker(nonce)));
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
				throw error;
			}
		},
	};
}

function sessionDirectory(workspace: string): string {
	return path.join(workspace, E2E_SESSION_DIRECTORY);
}

class E2eFileSession implements HostedSdkSession {
	readonly sessionFile: string;
	readonly sessionId: string;
	readonly #listeners = new Set<(event: unknown) => void>();
	readonly #gateListeners = new Set<(gate: HostedSdkGate) => void>();
	#followUpQueueDepth = 0;
	#disposed = false;

	constructor(sessionFile: string, sessionId: string | undefined) {
		this.sessionFile = sessionFile;
		this.sessionId = sessionId ?? path.basename(sessionFile, ".jsonl");
	}

	subscribe(listener: (event: unknown) => void): () => void {
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}

	subscribeGates(listener: (gate: HostedSdkGate) => void): () => void {
		this.#gateListeners.add(listener);
		return () => this.#gateListeners.delete(listener);
	}

	async prompt(text: string): Promise<void> {
		await this.respond(text, "prompt");
	}

	async steer(text: string): Promise<void> {
		await this.respond(text, "steer");
	}

	async followUp(_text: string): Promise<void> {
		this.assertLive();
		this.#followUpQueueDepth += 1;
	}

	followUpQueueDepth(): number {
		return this.#followUpQueueDepth;
	}

	async answerGate(_gateId: string, _answer: unknown, _idempotencyKey: string): Promise<HostedSdkGateResolution> {
		this.assertLive();
		return "not_found";
	}

	async sendBootstrapMessage(nonce: string): Promise<void> {
		this.assertLive();
		fs.mkdirSync(path.dirname(this.sessionFile), { recursive: true, mode: 0o700 });
		const transcript = [
			JSON.stringify({ type: "session", id: this.sessionId }),
			JSON.stringify({ type: "message", role: "user", content: bootstrapNonceMarker(nonce) }),
			JSON.stringify({ type: "message", role: "assistant", content: "e2e bootstrap persisted" }),
		].join("\n");
		fs.writeFileSync(this.sessionFile, `${transcript}\n`, { encoding: "utf8", mode: 0o600, flag: "wx" });
	}

	async dispose(): Promise<void> {
		this.#disposed = true;
		this.#listeners.clear();
		this.#gateListeners.clear();
	}

	private async respond(text: string, delivery: "prompt" | "steer"): Promise<void> {
		this.assertLive();
		this.emit({ type: "turn_start" });
		this.append({ type: "message", role: "user", content: text, delivery });
		const reply = `fixture reply: ${text}`;
		const message = {
			role: "assistant",
			content: [{ type: "text", text: reply }],
			responseId: crypto.randomUUID(),
			timestamp: Date.now(),
		};
		this.append({ type: "message", role: "assistant", content: reply });
		this.emit({ type: "message_update", message, assistantMessageEvent: { type: "text_delta", delta: reply } });
		this.emit({ type: "message_end", message });
		this.emit({ type: "turn_end", message });
	}

	private append(value: unknown): void {
		fs.appendFileSync(this.sessionFile, `${JSON.stringify(value)}\n`, { encoding: "utf8", mode: 0o600 });
	}

	private emit(event: unknown): void {
		for (const listener of [...this.#listeners]) listener(event);
	}

	private assertLive(): void {
		if (this.#disposed) throw new Error("e2e SDK session is disposed");
	}
}
