export function cronMatches(schedule: string, date: Date): boolean {
	const fields = schedule.trim().split(/\s+/);
	if (fields.length !== 5) throw new Error("cron schedule must have five fields");
	return [date.getMinutes(), date.getHours(), date.getDate(), date.getMonth() + 1, date.getDay()].every(
		(value, index) => cronFieldMatches(fields[index]!, value, 0),
	);
}
export function cronFieldMatches(field: string, value: number, min: number): boolean {
	return field.split(",").some((part) => {
		const [base, stepText] = part.split("/");
		const step = stepText ? Number(stepText) : 1;
		if (!Number.isInteger(step) || step < 1) return false;
		if (base === "*") return (value - min) % step === 0;
		const range = base.split("-").map(Number);
		if (range.length === 1) return value === range[0] && step === 1;
		return (
			range.length === 2 &&
			Number.isInteger(range[0]) &&
			Number.isInteger(range[1]) &&
			value >= range[0]! &&
			value <= range[1]! &&
			(value - range[0]!) % step === 0
		);
	});
}
export function startCron(schedule: string, fire: () => void, now: () => Date = () => new Date()): () => void {
	let minute = -1;
	const tick = () => {
		const date = now();
		if (date.getMinutes() !== minute && cronMatches(schedule, date)) {
			minute = date.getMinutes();
			fire();
		}
	};
	tick();
	const timer = setInterval(tick, 30_000);
	return () => clearInterval(timer);
}
