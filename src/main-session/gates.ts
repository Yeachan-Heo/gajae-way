/** Main-session gate handle reserved for the P4 gate bridge. */
export interface GateHandle {
	readonly gateId: string;
	readonly expectedSessionId: string;
}
