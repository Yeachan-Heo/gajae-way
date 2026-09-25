/**
 * Next-fire computation for a five-field cron schedule.
 *
 * `TriggerSpec` stores a cron as an opaque string and no verb returns a computed
 * next-fire time (gap G6 in the design). "Next fire in 3h 12m" is the single
 * most useful thing a monitor row can say, so the console computes it locally
 * from the same string the gateway scheduled on rather than showing the raw
 * expression and making the owner parse it.
 */

type FieldRange = { readonly min: number; readonly max: number };

const FIELDS: readonly FieldRange[] = [
	{ min: 0, max: 59 }, // minute
	{ min: 0, max: 23 }, // hour
	{ min: 1, max: 31 }, // day of month
	{ min: 1, max: 12 }, // month
	{ min: 0, max: 7 }, // day of week; 0 and 7 both mean Sunday, folded to 0 below
];

const MONTH_NAMES = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
const DAY_NAMES = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

function named(token: string, index: number): string {
	const lower = token.toLowerCase();
	const table = index === 3 ? MONTH_NAMES : index === 4 ? DAY_NAMES : undefined;
	if (!table) return token;
	const position = table.indexOf(lower);
	return position === -1 ? token : String(position + (index === 3 ? 1 : 0));
}

/** Expand one cron field into the exact set of values it matches, or null when malformed. */
function parseField(raw: string, index: number): Set<number> | null {
	const range = FIELDS[index];
	if (!range) return null;
	const values = new Set<number>();
	for (const part of raw.split(",")) {
		const [spec, stepText] = part.split("/");
		if (spec === undefined || spec.length === 0) return null;
		const step = stepText === undefined ? 1 : Number(stepText);
		if (!Number.isInteger(step) || step < 1) return null;

		let from: number;
		let to: number;
		if (spec === "*") {
			from = range.min;
			to = range.max;
		} else {
			const bounds = spec.split("-").map((token) => Number(named(token, index)));
			const first = bounds[0];
			if (bounds.length > 2 || first === undefined || !Number.isInteger(first)) return null;
			const second = bounds.length === 2 ? bounds[1] : first;
			if (second === undefined || !Number.isInteger(second)) return null;
			from = first;
			to = second;
		}
		if (from < range.min || to > range.max || from > to) return null;
		for (let value = from; value <= to; value += step) values.add(index === 4 && value === 7 ? 0 : value);
	}
	return values.size === 0 ? null : values;
}

export type CronSchedule = {
	readonly minute: Set<number>;
	readonly hour: Set<number>;
	readonly dayOfMonth: Set<number>;
	readonly month: Set<number>;
	readonly dayOfWeek: Set<number>;
	/** True when both day fields are restricted; cron then matches either, not both. */
	readonly dayUnion: boolean;
};

export function parseCron(schedule: string): CronSchedule | null {
	const parts = schedule.trim().split(/\s+/);
	if (parts.length !== 5) return null;
	const parsed = parts.map((part, index) => parseField(part, index));
	const [minute, hour, dayOfMonth, month, dayOfWeek] = parsed;
	if (!minute || !hour || !dayOfMonth || !month || !dayOfWeek) return null;
	return {
		minute,
		hour,
		dayOfMonth,
		month,
		dayOfWeek,
		dayUnion: parts[2] !== "*" && parts[4] !== "*",
	};
}
