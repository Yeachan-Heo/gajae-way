import type { CliResult } from "@gajaeway/subsession";
export const FIXTURE_VERSION: number;
export function parseFakeModes(value?: string): string[];
export function createFakeGjc(options?: {
	modes?: string;
	connectionId?: string;
	env?: Record<string, string | undefined>;
}): (args: readonly string[]) => Promise<CliResult | undefined>;
export function runFakeGjc(args: readonly string[], command?: ReturnType<typeof createFakeGjc>): Promise<CliResult>;
export function startCapacityExhaustedBroker(options?: { token?: string }): {
	url: string;
	token: string;
	requests: unknown[];
	stop(): void;
};
