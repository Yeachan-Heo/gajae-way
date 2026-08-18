/** Server-derived submission shape reserved for the P4 admission implementation. */
export interface AdmissionRequest {
	readonly text: string;
	readonly surfaceId: string;
	readonly idempotencyKey: string;
}
