import * as fs from "node:fs/promises";
import * as path from "node:path";

export const requiredGeneratedBindingSymbols = [
	"BridgeRequest",
	"GatewayMetaReadOutput",
	"GatewayMetaTransactionInput",
	"HealthInfo",
	"RpcBridgeStats",
	"WayCore",
	"healthInfo",
	"sdNotifyStatus",
	"setRpcHealth",
	"setMainSessionStatus",
	"setJournalDegraded",
	"lockDrainRevocations",
	"lockFencingValid",
	"openWithTestHardCap",
	"processIdentity",
	"RegistryRowOutput",
	"registryApplyBrokerSnapshot",
	"registryList",
	"registryGet",
	"registryAnnotate",
	"registryApplyMetadata",
	"registryMarkMetadataUnavailable",
	"registryConfigureSurfaces",
	"registryBindSurface",
	"registryRegisterGatewaySession",
	"surfaceResolve",
	"setReconcileStatus",
] as const;

export function validateGeneratedBindingSource(bindings: string): void {
	for (const symbol of requiredGeneratedBindingSymbols) {
		if (!bindings.includes(symbol)) {
			throw new Error(`napi build did not generate the required binding: ${symbol}`);
		}
	}
}

export async function validateGeneratedBindings(nativeDir = path.join(import.meta.dir, "..", "native")): Promise<void> {
	const declarations = await fs.readFile(path.join(nativeDir, "index.d.ts"), "utf8");
	validateGeneratedBindingSource(declarations);
}

if (import.meta.main) {
	await validateGeneratedBindings();
	console.log("Generated native bindings contain the required RPC exports.");
}
