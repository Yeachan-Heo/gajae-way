import { acquireLease, type Lease, LeaseHeldError } from "@gajae-gateway/home";
import type { GatewayConfig } from "../config";
import { resolveBrokerAgentSource } from "./allowlist";
import { readBrokerAgentDrift } from "./drift";
import { BrokerAgentError, type BrokerProvisionFault, provisionBrokerAgent } from "./provision";

export interface BrokerAgentCommandDependencies {
	readonly fault?: (point: BrokerProvisionFault) => void | Promise<void>;
}

export async function runBrokerAgentCommand(
	args: readonly string[],
	config: GatewayConfig,
	dependencies: BrokerAgentCommandDependencies = {},
): Promise<number> {
	const [verb, ...options] = args;
	if (verb === "status") return runStatus(options, config);
	if (verb !== "provision") {
		console.error(
			"usage: broker-agent provision --mode force|update [--from DIR] [--lease-wait-ms N] | broker-agent status [--json]",
		);
		return 2;
	}
	const parsed = parseProvisionArgs(options);
	if (!parsed) {
		console.error("usage: broker-agent provision --mode force|update [--from DIR] [--lease-wait-ms N]");
		return 2;
	}
	let lease: Lease;
	try {
		lease = await acquireBrokerLease(config.home, parsed.leaseWaitMs);
		console.info(`lease_acquired name=broker pid=${process.pid}`);
	} catch (error) {
		if (error instanceof LeaseHeldError) {
			console.error(error.message);
			return 1;
		}
		console.error(`lease_failed name=broker detail=${error instanceof Error ? error.message : String(error)}`);
		console.error(`broker-agent provision refused: ${error instanceof Error ? error.message : String(error)}`);
		return 1;
	}
	try {
		const result = await provisionBrokerAgent({
			home: config.home,
			agentDir: config.gjc.agentDir,
			source: resolveBrokerAgentSource(parsed.from),
			mode: parsed.mode,
			lease,
			fault: dependencies.fault,
		});
		console.log(`broker_agent_provision status=${result.status}${result.genId ? ` genId=${result.genId}` : ""}`);
		for (const entry of result.entries)
			console.log(`broker_agent_entry name=${entry.name} kind=${entry.kind} state=${entry.state}`);
		return 0;
	} catch (error) {
		if (error instanceof BrokerAgentError) {
			console.error(`broker_agent_provision ${error.message}`);
			if (error.code === "broker_agent_unrecoverable") return 4;
			if (error.code === "broker_agent_failed_restored") return 3;
			return 1;
		}
		console.error(`broker_agent_provision failed: ${error instanceof Error ? error.message : String(error)}`);
		return 1;
	} finally {
		await lease.release();
	}
}

async function runStatus(args: readonly string[], config: GatewayConfig): Promise<number> {
	if (args.some((arg) => arg !== "--json")) {
		console.error("usage: broker-agent status [--json]");
		return 2;
	}
	const drift = readBrokerAgentDrift({
		home: config.home,
		agentDir: config.gjc.agentDir,
	});
	if (args.includes("--json")) {
		console.log(JSON.stringify(drift, null, 2));
		return 0;
	}
	console.log(
		`broker_agent_status status=${drift.status} genId=${drift.genId ?? "none"} provisionedAt=${drift.provisionedAt ?? "none"} source=${drift.source ?? "none"}`,
	);
	for (const entry of drift.entries)
		console.log(`broker_agent_entry name=${entry.name} kind=${entry.kind} state=${entry.state}`);
	return 0;
}

function parseProvisionArgs(
	args: readonly string[],
): { readonly mode: "force" | "update"; readonly from?: string; readonly leaseWaitMs: number } | undefined {
	let mode: "force" | "update" | undefined;
	let from: string | undefined;
	let leaseWaitMs = 0;
	for (let index = 0; index < args.length; index++) {
		const arg = args[index];
		if (arg === "--mode") {
			const value = args[++index];
			if (value !== "force" && value !== "update") return undefined;
			if (mode !== undefined) return undefined;
			mode = value;
		} else if (arg === "--from") {
			const value = args[++index];
			if (!value || from !== undefined) return undefined;
			from = value;
		} else if (arg === "--lease-wait-ms") {
			const value = args[++index];
			if (!value || !/^\d+$/.test(value)) return undefined;
			leaseWaitMs = Number(value);
			if (!Number.isSafeInteger(leaseWaitMs)) return undefined;
		} else return undefined;
	}
	return mode === undefined ? undefined : { mode, ...(from === undefined ? {} : { from }), leaseWaitMs };
}

async function acquireBrokerLease(home: string, waitMs: number): Promise<Lease> {
	const deadline = Date.now() + waitMs;
	for (;;) {
		try {
			return await acquireLease(home, "broker");
		} catch (error) {
			if (!(error instanceof LeaseHeldError) || Date.now() >= deadline) throw error;
			await new Promise((resolve) => setTimeout(resolve, Math.min(50, Math.max(1, deadline - Date.now()))));
		}
	}
}
