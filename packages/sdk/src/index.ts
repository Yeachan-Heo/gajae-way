export type { GajaewayClientOptions, StdioTransport } from "./client";
export { GajaewayClient, LOOPBACK_ORIGIN } from "./client";

// Grok provider with dynamic version resolution and HTTP 426 handling
export {
	getLatestGrokCliVersion,
	parseVersionFrom426,
	updateVersionFrom426,
	clearVersionCache,
} from "./providers/grok/version-resolver";
export {
	createGrokHttpClient,
	streamToGrokV1,
	type GrokHttpClientOptions,
	type GrokApiResponse,
} from "./providers/grok/stream";
