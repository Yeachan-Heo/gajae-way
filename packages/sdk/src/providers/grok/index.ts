/**
 * Grok provider with dynamic version resolution and HTTP 426 handling.
 */

export {
	getLatestGrokCliVersion,
	parseVersionFrom426,
	updateVersionFrom426,
	clearVersionCache,
} from "./version-resolver.js";

export {
	createGrokHttpClient,
	streamToGrokV1,
	type GrokHttpClientOptions,
	type GrokApiResponse,
} from "./stream.js";
