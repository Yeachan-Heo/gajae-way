import { afterEach } from "bun:test";
import { ReconnectingGateway } from "../src/main";

const live = new Set<ReconnectingGateway>();

/**
 * A test adapter whose client fails keeps rescheduling reconnects (and logging
 * them) for the rest of the `bun test` process, leaking into every later test
 * that captures console.log. Each one built here is stopped after its test.
 */
export function liveGateway(...args: ConstructorParameters<typeof ReconnectingGateway>): ReconnectingGateway {
	const gateway = new ReconnectingGateway(...args);
	live.add(gateway);
	return gateway;
}

/** Registers the per-file hook that stops every adapter built by `liveGateway`. */
export function stopLiveGatewaysAfterEach(): void {
	afterEach(() => {
		for (const gateway of live) gateway.stop();
		live.clear();
	});
}
