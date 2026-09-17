import { expect, test } from "bun:test";
import {
	BOT_AUDIENCE_TURN_COOLDOWN_MS,
	BotAudienceTurnGuard,
	type BotAudienceTurnStore,
} from "../src/engagement/policy";

const ORIGIN = "discord/channel/1508831529856663612";
const OTHER = "discord/channel/999";

function memoryStore(seed: Record<string, string> = {}): BotAudienceTurnStore & { readonly rows: Map<string, string> } {
	const rows = new Map<string, string>(Object.entries(seed));
	return {
		rows,
		metaGet: (key) => rows.get(key),
		metaSet: (key, value) => {
			rows.set(key, value);
		},
		metaDelete: (key) => {
			rows.delete(key);
		},
	};
}

function clock(start = 1_700_000_000_000) {
	const state = { now: start };
	return { state, read: () => state.now };
}

test("a scheduled driver bot keeps its cadence once the cooldown has elapsed", () => {
	const { state, read } = clock();
	const guard = new BotAudienceTurnGuard(undefined, read);
	// The live regression: a 25-minute follow-up cron used to lose every tick
	// after the first until a human happened to speak.
	for (let tick = 0; tick < 5; tick++) {
		expect(guard.canAdmit(ORIGIN)).toBe(true);
		guard.recordBotAdmission(ORIGIN);
		state.now += 25 * 60_000;
	}
});

test("a second bot trigger inside the cooldown is declined, and admitted at the boundary", () => {
	const { state, read } = clock();
	const guard = new BotAudienceTurnGuard(undefined, read);
	guard.recordBotAdmission(ORIGIN);
	state.now += BOT_AUDIENCE_TURN_COOLDOWN_MS - 1;
	expect(guard.canAdmit(ORIGIN)).toBe(false);
	state.now += 1;
	expect(guard.canAdmit(ORIGIN)).toBe(true);
});

test("the charge is per conversation", () => {
	const guard = new BotAudienceTurnGuard();
	guard.recordBotAdmission(ORIGIN);
	expect(guard.canAdmit(ORIGIN)).toBe(false);
	expect(guard.canAdmit(OTHER)).toBe(true);
});

test("a turn that settled without an answer refunds its admission immediately", () => {
	const store = memoryStore();
	const { read } = clock();
	const guard = new BotAudienceTurnGuard(store, read);
	guard.recordBotAdmission(ORIGIN);
	expect(guard.canAdmit(ORIGIN)).toBe(false);
	guard.releaseBotAdmission(ORIGIN);
	expect(guard.canAdmit(ORIGIN)).toBe(true);
	expect([...store.rows.keys()]).toEqual([]);
});

test("a restart cannot refund a live charge, and stops declining once it expires", () => {
	const store = memoryStore();
	const { state, read } = clock();
	new BotAudienceTurnGuard(store, read).recordBotAdmission(ORIGIN);
	state.now += BOT_AUDIENCE_TURN_COOLDOWN_MS / 2;
	expect(new BotAudienceTurnGuard(store, read).canAdmit(ORIGIN)).toBe(false);
	state.now += BOT_AUDIENCE_TURN_COOLDOWN_MS / 2;
	expect(new BotAudienceTurnGuard(store, read).canAdmit(ORIGIN)).toBe(true);
});

test("a human message clears the durable charge too", () => {
	const store = memoryStore();
	const { read } = clock();
	new BotAudienceTurnGuard(store, read).recordBotAdmission(ORIGIN);
	const restarted = new BotAudienceTurnGuard(store, read);
	restarted.recordHumanMessage(ORIGIN);
	expect(restarted.canAdmit(ORIGIN)).toBe(true);
	expect(new BotAudienceTurnGuard(store, read).canAdmit(ORIGIN)).toBe(true);
});

test("corrupt or future-dated durable state fails closed and heals after one window", () => {
	const { state, read } = clock();
	for (const bad of ["not-a-number", "-5", String(state.now + 10 * 60_000)]) {
		const store = memoryStore({ [`bot-audience-charged-at:${ORIGIN}`]: bad });
		const guard = new BotAudienceTurnGuard(store, read);
		expect(guard.canAdmit(ORIGIN)).toBe(false);
		expect(store.rows.get(`bot-audience-charged-at:${ORIGIN}`)).toBe(String(state.now));
	}
	const store = memoryStore({ [`bot-audience-charged-at:${ORIGIN}`]: "not-a-number" });
	const guard = new BotAudienceTurnGuard(store, read);
	expect(guard.canAdmit(ORIGIN)).toBe(false);
	state.now += BOT_AUDIENCE_TURN_COOLDOWN_MS;
	expect(guard.canAdmit(ORIGIN)).toBe(true);
});

test("a store without metaDelete records a cleared charge as an expired one", () => {
	const rows = new Map<string, string>();
	const store: BotAudienceTurnStore = {
		metaGet: (key) => rows.get(key),
		metaSet: (key, value) => {
			rows.set(key, value);
		},
	};
	const { read } = clock();
	const guard = new BotAudienceTurnGuard(store, read);
	guard.recordBotAdmission(ORIGIN);
	guard.recordHumanMessage(ORIGIN);
	expect(rows.get(`bot-audience-charged-at:${ORIGIN}`)).toBe("0");
	expect(new BotAudienceTurnGuard(store, read).canAdmit(ORIGIN)).toBe(true);
});

test("declines are counted for the operator and survive a restart", () => {
	const store = memoryStore();
	const guard = new BotAudienceTurnGuard(store);
	expect(guard.botAudienceDeclines()).toBe(0);
	guard.recordBotAudienceDecline();
	guard.recordBotAudienceDecline();
	expect(guard.botAudienceDeclines()).toBe(2);
	expect(new BotAudienceTurnGuard(store).botAudienceDeclines()).toBe(2);
});

test("a corrupt decline counter reads as zero rather than throwing", () => {
	const store = memoryStore({ "bot-audience-declines": "many" });
	expect(new BotAudienceTurnGuard(store).botAudienceDeclines()).toBe(0);
});
