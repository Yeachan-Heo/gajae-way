#!/usr/bin/env node
import fs from "node:fs";

const statePath = process.env.GAJAEWAY_BROKER_FIXTURE_STATE;
if (!statePath) {
	console.error("GAJAEWAY_BROKER_FIXTURE_STATE is required");
	process.exitCode = 2;
} else {
	const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
	const args = process.argv.slice(2);
	if (!state.sessions) {
		function advanceLegacyLiveness() {
			const result = state.list?.result;
			if (!result || typeof result !== "object" || !Array.isArray(result.sessions)) return;
			let heartbeatRows = 0;
			for (const row of result.sessions) {
				if (!row || typeof row !== "object" || row.live !== true) continue;
				let advanced = false;
				if (row.activity && typeof row.activity === "object" && typeof row.activity.at === "number") {
					row.activity.at += 1;
					advanced = true;
				}
				if (typeof row.lastHeartbeatAt === "number") {
					row.lastHeartbeatAt += 1;
					advanced = true;
				}
				if (advanced) heartbeatRows += 1;
			}
			if (typeof result.indexSeq === "number") result.indexSeq += heartbeatRows;
		}
		if (args.length === 3 && args[0] === "sdk" && args[1] === "session" && args[2] === "list") {
			process.stdout.write(`${JSON.stringify(state.list)}\n`);
			advanceLegacyLiveness();
			fs.writeFileSync(statePath, `${JSON.stringify(state)}\n`);
		} else if (
			args[0] === "sdk" &&
			args[1] === "session" &&
			args[2] === "raw" &&
			args[3] === "query" &&
			args[args.indexOf("--query") + 1] === "session.metadata"
		) {
			const sessionId = args[4];
			fs.appendFileSync(`${statePath}.queries`, `${sessionId}\n`);
			const metadata = state.metadata?.[sessionId];
			if (!metadata || metadata.unavailable === true) {
				process.stdout.write(
					`${JSON.stringify({ ok: false, error: { code: "session_unavailable", message: "fixture unavailable" } })}\n`,
				);
				process.exitCode = 1;
			} else {
				process.stdout.write(`${JSON.stringify({ ok: true, result: metadata })}\n`);
			}
		} else {
			console.error(`unexpected fake broker argv: ${JSON.stringify(args)}`);
			process.exitCode = 2;
		}
	} else {
		const sessions = state.sessions ?? {};

		function writeState() {
			fs.writeFileSync(statePath, `${JSON.stringify(state)}\n`);
		}

		function fail(code, message) {
			process.stdout.write(`${JSON.stringify({ ok: false, error: { code, message } })}\n`);
			process.exitCode = 1;
		}

		function success(result) {
			process.stdout.write(`${JSON.stringify({ ok: true, result })}\n`);
		}

		function queryResponse(item) {
			process.stdout.write(
				`${JSON.stringify({
					type: "query_response",
					id: "fixture-query",
					ok: true,
					page: { items: [item], complete: true, revision: "fixture-revision" },
				})}\n`,
			);
		}

		function pause(milliseconds) {
			return new Promise((resolve) => setTimeout(resolve, milliseconds));
		}

		function session(sessionId) {
			const value = sessions[sessionId];
			if (!value) {
				fail("session_unavailable", `fixture session ${sessionId} is unavailable`);
				return undefined;
			}
			return value;
		}

		function listRows() {
			return Object.values(sessions).map((value) => value.row);
		}

		function listEnvelope() {
			return {
				ok: true,
				result: {
					version: 1,
					source: "broker",
					indexSeq: state.indexSeq ?? 1,
					sessions: listRows(),
					warnings: [],
				},
			};
		}

		function advanceListLiveness() {
			let heartbeatRows = 0;
			for (const value of Object.values(sessions)) {
				const row = value.row;
				if (!row || row.live !== true) continue;
				let advanced = false;
				if (row.activity && typeof row.activity.at === "number") {
					row.activity.at += 1;
					advanced = true;
				}
				if (typeof row.lastHeartbeatAt === "number") {
					row.lastHeartbeatAt += 1;
					advanced = true;
				}
				if (advanced) heartbeatRows += 1;
			}
			state.indexSeq = (state.indexSeq ?? 1) + heartbeatRows;
		}

		function compareTailCoordinates(left, right) {
			if (left.generation !== right.generation) return left.generation < right.generation ? -1 : 1;
			if (left.seq !== right.seq) return left.seq < right.seq ? -1 : 1;
			return 0;
		}

		function eventIsAfter(event, checkpoint) {
			return (
				typeof event.generation !== "number" ||
				typeof event.seq !== "number" ||
				event.generation > checkpoint.generation ||
				(event.generation === checkpoint.generation && event.seq > checkpoint.seq)
			);
		}

		function nextSequence(value, generation) {
			value.nextSequenceByGeneration ??= {};
			const key = String(generation);
			const sequence = (value.nextSequenceByGeneration[key] ?? 0) + 1;
			value.nextSequenceByGeneration[key] = sequence;
			return sequence;
		}

		function appendEvent(value, kind, payload) {
			const scopedGeneration = payload?.scope?.generation;
			const generation = Number.isSafeInteger(scopedGeneration) ? scopedGeneration : value.tailCheckpoint.generation;
			const seq = nextSequence(value, generation);
			value.nextEventId = (value.nextEventId ?? 0) + 1;
			value.tailCheckpoint = { generation, seq };
			value.events ??= [];
			value.events.push({
				kind,
				id: `${value.row.sessionId}:${generation}:${seq}:${value.nextEventId}`,
				generation,
				seq,
				payload,
			});
		}

		function appendTranscript(value, payload) {
			value.transcript ??= [];
			const id = `${value.row.sessionId}:transcript:${value.nextTranscriptId ?? value.transcript.length}`;
			value.nextTranscriptId = (value.nextTranscriptId ?? value.transcript.length) + 1;
			value.transcript.push({ id, payload });
		}

		function brokerTurnId(value, opRef) {
			return value.tailUsesBrokerTurnId === true ? `broker-turn-${opRef}` : `turn:${opRef}`;
		}

		function operationScope(value, operation) {
			return {
				attemptId:
					value.tailUsesBrokerTurnId === true
						? brokerTurnId(value, operation.opRef)
						: `${value.row.sessionId}:${operation.opRef}`,
				generation: operation.generation ?? 1,
				lineage: "main",
			};
		}

		function settleQueuedFollowUps(value) {
			for (const candidate of Object.values(value.operations ?? {})) {
				if (candidate.operation !== "turn.follow_up" || candidate.completed) continue;
				const scope = operationScope(value, candidate);
				value.context.isStreaming = true;
				appendEvent(value, "agent_start", { type: "agent_start", sessionId: value.row.sessionId, scope });
				appendEvent(value, "turn_start", { type: "turn_start", sessionId: value.row.sessionId, scope });
				appendEvent(value, "agent_start", { type: "agent_start", sessionId: value.row.sessionId, scope });
				completeOperation(value, candidate);
			}
		}

		function completeOperation(value, operation) {
			if (!operation || operation.completed) return;
			operation.completed = true;
			value.context ??= { isStreaming: false, followupQueueDepth: 0 };
			const scope = operationScope(value, operation);
			const responseId = `${value.row.sessionId}:assistant:${operation.opRef}`;
			const text = operation.failure
				? ""
				: (operation.responseText ?? (operation.operation === "turn.steer" ? "steered" : "ack"));
			if (operation.operation !== "turn.follow_up") {
				appendTranscript(value, {
					type: "message",
					role: "user",
					content: operation.text,
					delivery: operation.operation,
				});
			}
			if (operation.failure) {
				appendEvent(value, "agent_failed", {
					type: "agent_failed",
					sessionId: value.row.sessionId,
					error: { code: "fixture_failure", message: "fixture injected turn failure" },
					scope,
				});
				settleQueuedFollowUps(value);
				value.context.isStreaming = false;
				value.context.followupQueueDepth = 0;
				return;
			}
			const message = {
				role: "assistant",
				content: [{ type: "text", text }],
				responseId,
				turnId: brokerTurnId(value, operation.opRef),
				timestamp: operation.timestamp ?? 1_700_000_000_000 + (value.nextEventId ?? 0),
			};
			appendTranscript(value, {
				type: "message",
				role: "assistant",
				content: text,
				responseId,
				turnId: brokerTurnId(value, operation.opRef),
				timestamp: message.timestamp,
			});
			appendEvent(value, "turn_end", { type: "turn_end", message, toolResults: [], scope });
			appendEvent(value, "agent_end", { type: "agent_end", messages: [message], stopReason: "completed", scope });
			settleQueuedFollowUps(value);
			value.context.isStreaming = false;
			value.context.followupQueueDepth = 0;
		}

		function startOperation(value, operation, hold) {
			value.operations ??= {};
			value.operations[operation.opRef] = operation;
			value.context ??= { isStreaming: false, followupQueueDepth: 0 };
			const scope = operationScope(value, operation);
			if (operation.operation === "turn.follow_up") {
				value.context.followupQueueDepth = (value.context.followupQueueDepth ?? 0) + 1;
				if (!hold && value.context.isStreaming !== true) {
					value.context.isStreaming = true;
					appendEvent(value, "agent_start", { type: "agent_start", sessionId: value.row.sessionId, scope });
					appendEvent(value, "turn_start", { type: "turn_start", sessionId: value.row.sessionId, scope });
					appendEvent(value, "agent_start", { type: "agent_start", sessionId: value.row.sessionId, scope });
					completeOperation(value, operation);
				}
			} else {
				value.context.isStreaming = true;
				// The SDK event ring can contain overlapping lifecycle publications from
				// both the outer agent and provider turn source. Emit duplicates on purpose.
				appendEvent(value, "agent_start", { type: "agent_start", sessionId: value.row.sessionId, scope });
				appendEvent(value, "turn_start", { type: "turn_start", sessionId: value.row.sessionId, scope });
				appendEvent(value, "agent_start", { type: "agent_start", sessionId: value.row.sessionId, scope });
				if (!hold) completeOperation(value, operation);
			}
		}

		function operationReceipt(value, operation, opRef) {
			// Mirrors the REAL broker receipt: {commandId, turnId, accepted, clientRef}.
			// The real broker does NOT echo sessionId or operation (observed 2026-08-20).
			return {
				accepted: true,
				clientRef: opRef,
				commandId: `command:${opRef}`,
				turnId: brokerTurnId(value, opRef),
			};
		}

		function suppressAcceptedReceipt(value) {
			if (value.suppressNextReceipt !== true) return false;
			delete value.suppressNextReceipt;
			writeState();
			console.error("fixture: accepted operation receipt suppressed");
			process.exitCode = 1;
			return true;
		}

		async function suppressReceiptAfterTerminalTail(value, opRef) {
			delete value.suppressNextReceiptAfterTerminalTail;
			value.receiptSuppressionWaitingForTerminalTail = opRef;
			delete value.receiptSuppressionTerminalTailObserved;
			writeState();
			const deadline = Date.now() + 1_500;
			while (Date.now() < deadline) {
				await pause(10);
				const latest = JSON.parse(fs.readFileSync(statePath, "utf8")).sessions?.[value.row.sessionId];
				if (latest?.receiptSuppressionTerminalTailObserved !== opRef) continue;
				// Let the host process the already-emitted terminal envelope before the
				// waiting send command fails without its receipt.
				await pause(50);
				console.error("fixture: accepted operation receipt suppressed after terminal tail");
				process.exitCode = 1;
				return true;
			}
			console.error("fixture: terminal tail was not observed before receipt suppression timed out");
			process.exitCode = 1;
			return true;
		}

		function handleOperation(sessionId, operation, text, opRef) {
			const value = session(sessionId);
			if (!value) return;
			if (value.row.live !== true || value.row.deleted === true)
				return fail("session_unavailable", "fixture session is not live");
			if (!text || !text.trim() || !opRef || !opRef.trim())
				return fail("invalid_input", "text and operation ref are required");
			value.admissionAttemptLog ??= [];
			value.admissionAttemptLog.push({ operation, text, opRef });
			const rejection = value.rejectNext;
			if (rejection) {
				delete value.rejectNext;
				writeState();
				return fail(rejection.code, rejection.message);
			}
			const existing = value.operations?.[opRef];
			if (!existing) {
				const hold = value.holdOperations?.includes(opRef) === true || value.holdNext === true;
				if (value.holdNext === true) value.holdNext = false;
				const operationState = {
					opRef,
					operation,
					text,
					failure: value.failOperations?.includes(opRef) === true || value.failNext === true,
					responseText: value.responseText,
					generation: value.nextGeneration ?? 1,
				};
				value.nextGeneration = (value.nextGeneration ?? 1) + 1;
				if (value.failNext === true) value.failNext = false;
				startOperation(value, operationState, hold);
			}
			value.commandLog ??= [];
			value.commandLog.push({ operation, text, opRef });
			writeState();
			return value;
		}

		function applyRequestedCompletions() {
			let changed = false;
			for (const value of Object.values(sessions)) {
				for (const operation of Object.values(value.operations ?? {})) {
					const requested = operation.completionRequested;
					if (!requested || operation.completed) continue;
					delete operation.completionRequested;
					if (requested.failure) operation.failure = true;
					if (requested.text !== undefined) operation.responseText = requested.text;
					completeOperation(value, operation);
					if (value.rotateRingDuringNextCompletion === true) {
						delete value.rotateRingDuringNextCompletion;
						const priorFloor = value.retentionFloor;
						const checkpoint = value.tailCheckpoint;
						const floor = !priorFloor || compareTailCoordinates(checkpoint, priorFloor) > 0 ? checkpoint : priorFloor;
						value.retentionFloor = floor;
						value.clearRetentionGapAfterTail = true;
						value.events = (value.events ?? []).filter((event) => eventIsAfter(event, floor));
						value.gap = undefined;
					}
					changed = true;
				}
			}
			if (changed) writeState();
		}

		applyRequestedCompletions();

		if (args.length === 3 && args[0] === "sdk" && args[1] === "session" && args[2] === "list") {
			const output = `${JSON.stringify(listEnvelope())}\n`;
			advanceListLiveness();
			writeState();
			process.stdout.write(output);
		} else if (args.length === 4 && args[0] === "sdk" && args[1] === "session" && args[2] === "inspect") {
			const value = session(args[3]);
			if (value) success({ version: 1, source: "broker", session: value.row });
		} else if (args[0] === "sdk" && args[1] === "session" && args[2] === "send") {
			const sessionId = args[3];
			const textIndex = args.indexOf("--text");
			const refIndex = args.indexOf("--op-ref");
			const value = handleOperation(sessionId, "turn.prompt", args[textIndex + 1], args[refIndex + 1]);
			if (value) {
				const opRef = args[refIndex + 1];
				const suppress =
					value.suppressNextReceiptAfterTerminalTail === true
						? await suppressReceiptAfterTerminalTail(value, opRef)
						: suppressAcceptedReceipt(value);
				if (!suppress)
					success({
						version: 1,
						operationRef: opRef,
						status: "accepted",
						receipt: operationReceipt(value, "turn.prompt", opRef),
					});
			}
		} else if (args[0] === "sdk" && args[1] === "session" && args[2] === "status") {
			const value = session(args[3]);
			const opRef = args[4];
			if (value) {
				if (value.operationStatusUnavailable === true) {
					fail("unavailable", "fixture operation status is unavailable");
				} else {
					const operation = value.operations?.[opRef];
					const status =
						operation?.failure && operation.completed
							? "failed"
							: operation?.completed
								? "terminal_ok"
								: operation
									? "in_flight"
									: "unknown";
					success({
						version: 1,
						operationRef: opRef,
						status: { status },
						summary: { completed: status === "failed" || status === "terminal_ok" },
					});
				}
			}
		} else if (args[0] === "sdk" && args[1] === "session" && args[2] === "tail") {
			const value = session(args[3]);
			if (value && typeof value.timeoutNextTailCount === "number" && value.timeoutNextTailCount > 0) {
				value.timeoutNextTailCount -= 1;
				fs.writeFileSync(statePath, JSON.stringify(state, null, 1));
				fail("tail_timeout", "fixture: injected broker tail timeout");
				process.exit(1);
			}
			if (value && typeof value.crashTailAfterCount === "number" && value.crashTailAfterCount > 0) {
				value.crashTailAfterCount -= 1;
				fs.writeFileSync(statePath, JSON.stringify(state, null, 1));
			} else if (value && typeof value.crashNextTailCount === "number" && value.crashNextTailCount > 0) {
				// Transient transport failure injection: consume one crash budget and
				// exit nonzero WITHOUT an envelope, like a CLI dying under load.
				value.crashNextTailCount -= 1;
				fs.writeFileSync(statePath, JSON.stringify(state, null, 1));
				console.error("fixture: injected transient tail crash");
				process.exit(1);
			}
			if (value && value.tailTimeoutWhileBusy === true && value.context?.isStreaming === true) {
				fail("tail_timeout", "fixture: busy session has no tail envelope before a terminal boundary");
			} else if (value) {
				const cursorIndex = args.indexOf("--cursor");
				if (cursorIndex !== -1) {
					// The real credential-free CLI redacts the signed checkpoint token it
					// would need here. Do not accept an invented record-shaped cursor.
					fail("invalid_cursor", "fixture tail cannot validate an unavailable checkpoint token");
				} else {
					const retentionFloor = value.retentionFloor;
					const gap =
						value.gap ??
						(retentionFloor
							? {
									code: "retention_gap",
									missing: { from: 0, to: retentionFloor.seq },
									resync: {
										revision: value.transcript?.length ?? 0,
										generation: retentionFloor.generation,
										seq: retentionFloor.seq,
									},
								}
							: undefined);
					if (gap && args.includes("--strict")) {
						fail("retention_gap", "fixture strict tail encountered retained-history loss");
					} else {
						const items = [
							...(value.transcript ?? []).map((entry) => ({
								kind: "transcript",
								id: entry.id,
								payload: entry.payload,
							})),
							...(value.events ?? []).filter(
								(event) => retentionFloor === undefined || eventIsAfter(event, retentionFloor),
							),
						];
						const terminal =
							typeof value.tailTerminalOverride === "boolean"
								? value.tailTerminalOverride
								: value.context?.isStreaming !== true && (value.context?.followupQueueDepth ?? 0) === 0;
						success({
							version: 1,
							source: "session",
							session: value.row,
							checkpoint: {
								revision: value.transcript?.length ?? 0,
								generation: value.tailCheckpoint.generation,
								seq: value.tailCheckpoint.seq,
							},
							...(gap === undefined ? {} : { gap }),
							items,
							terminal,
						});
						if (value.receiptSuppressionWaitingForTerminalTail && terminal) {
							value.receiptSuppressionTerminalTailObserved = value.receiptSuppressionWaitingForTerminalTail;
							writeState();
						}
						if (gap && value.clearRetentionGapAfterTail === true) {
							delete value.clearRetentionGapAfterTail;
							delete value.retentionFloor;
							writeState();
						}
					}
				}
			}
		} else if (args[0] === "sdk" && args[1] === "session" && args[2] === "raw" && args[3] === "query") {
			const sessionId = args[4];
			const queryIndex = args.indexOf("--query");
			const query = args[queryIndex + 1];
			const value = session(sessionId);
			if (value) {
				if (value.unavailableQueries?.includes(query)) {
					fail("unavailable", `fixture query ${query} is unavailable`);
				} else if (query === "session.metadata") {
					fs.appendFileSync(`${statePath}.queries`, `${sessionId}\n`);
					if (!value.metadata || value.metadata.unavailable === true)
						fail("session_unavailable", "fixture unavailable");
					else queryResponse(value.metadata);
				} else if (query === "session.checkpoint") {
					// Real broker returns the RESULT-form envelope for this query, but these
					// coordinates are foreign to the event ring and must never seed its watermark.
					process.stdout.write(
						`${JSON.stringify({
							type: "query_response",
							id: "fixture-query",
							ok: true,
							result: {
								checkpoint: { revision: value.transcript?.length ?? 0, generation: 0, seq: 0 },
								revisionId: "fixture-revision-id",
								issuedAt: 1700000000000,
								expiresAt: 1700000900000,
							},
						})}\n`,
					);
				} else if (query === "context.get") {
					const context = value.context ?? { isStreaming: false, followupQueueDepth: 0 };
					queryResponse({
						isStreaming: context.isStreaming === true,
						followupQueueDepth: context.followupQueueDepth ?? 0,
					});
				} else if (query === "workflow.gates.list") {
					success(value.gates ?? []);
				} else {
					fail("unavailable", `fixture query ${query} is unavailable`);
				}
			}
		} else if (args[0] === "sdk" && args[1] === "session" && args[2] === "raw" && args[3] === "control") {
			const sessionId = args[4];
			const operationIndex = args.indexOf("--op");
			const inputIndex = args.indexOf("--json-input");
			let input;
			try {
				input = JSON.parse(args[inputIndex + 1] ?? "{}");
			} catch {
				fail("invalid_input", "fixture received invalid JSON input");
			}
			const operation = args[operationIndex + 1];
			if (!input) {
				// `fail` set a non-zero status above; do not attempt a second response.
			} else if (operation !== "turn.steer" && operation !== "turn.follow_up") {
				fail("operation_not_supported", `fixture control ${operation} is unsupported`);
			} else {
				const value = handleOperation(sessionId, operation, input.text, input.clientRef);
				if (value) {
					const suppress =
						value.suppressNextReceiptAfterTerminalTail === true
							? await suppressReceiptAfterTerminalTail(value, input.clientRef)
							: suppressAcceptedReceipt(value);
					if (!suppress) success(operationReceipt(value, operation, input.clientRef));
				}
			}
		} else {
			console.error(`unexpected fake broker argv: ${JSON.stringify(args)}`);
			process.exitCode = 2;
		}
	}
	// End external-session fixture branch.
}
