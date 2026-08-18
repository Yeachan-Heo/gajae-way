/** Authority-complete broker snapshot boundary reserved for P6. */
export interface BrokerSnapshot {
	readonly observedAt: string;
	readonly sessions: readonly unknown[];
}
