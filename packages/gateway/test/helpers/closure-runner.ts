import { join } from "node:path";
import { MemoryClosureQueue } from "../../src/memory/closure";
import { GatewayDatabase } from "../../src/store/db";

const home = process.env.GAJAEWAY_HOME;
if (!home) throw new Error("GAJAEWAY_HOME is required");
const database = await GatewayDatabase.open(join(home, "gateway.db"));
const queue = new MemoryClosureQueue(database, home);
await queue.initialize();
if (process.env.GAJAEWAY_MEMORY_RUNNER_MODE !== "recover") {
	queue.enqueue({
		kind: "daily_capture",
		originRefJson: JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: "crash-matrix" }),
		userText: "crash matrix user",
		replyText: "crash matrix reply",
	});
}
await queue.drain();
database.close();
