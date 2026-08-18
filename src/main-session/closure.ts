/** In-daemon closure execution is intentionally deferred to the P5 lock implementation. */
export interface ClosureRequest {
	readonly sessionId: string;
	readonly corpusPath: string;
}
