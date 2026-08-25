import { expect, test } from "bun:test";
import { KeyedQueue } from "../src/server/keyed-queue";

test("same-key tasks run strictly FIFO, never concurrently", async () => {
	const queue = new KeyedQueue();
	const events: string[] = [];
	let running = 0;
	let maxRunning = 0;
	const task = (label: string, ms: number) => async () => {
		running++;
		maxRunning = Math.max(maxRunning, running);
		events.push(`start:${label}`);
		await Bun.sleep(ms);
		events.push(`end:${label}`);
		running--;
		return label;
	};
	const results = await Promise.all([
		queue.run("origin-a", task("first", 20)),
		queue.run("origin-a", task("second", 5)),
		queue.run("origin-a", task("third", 1)),
	]);
	expect(results).toEqual(["first", "second", "third"]);
	expect(maxRunning).toBe(1);
	expect(events).toEqual(["start:first", "end:first", "start:second", "end:second", "start:third", "end:third"]);
});

test("distinct keys run concurrently", async () => {
	const queue = new KeyedQueue();
	let running = 0;
	let maxRunning = 0;
	const task = () => async () => {
		running++;
		maxRunning = Math.max(maxRunning, running);
		await Bun.sleep(15);
		running--;
	};
	await Promise.all([queue.run("origin-a", task()), queue.run("origin-b", task())]);
	expect(maxRunning).toBe(2);
});

test("a failed task rejects its caller but never wedges the key", async () => {
	const queue = new KeyedQueue();
	const failing = queue.run("origin-a", async () => {
		throw new Error("turn exploded");
	});
	const following = queue.run("origin-a", async () => "recovered");
	expect(failing).rejects.toThrow("turn exploded");
	expect(await following).toBe("recovered");
	// Queue map cleans up after the tail settles so idle origins hold no state.
	await Bun.sleep(0);
	expect(await queue.run("origin-a", async () => "fresh")).toBe("fresh");
});
