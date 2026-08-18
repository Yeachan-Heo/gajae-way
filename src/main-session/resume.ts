/** Strict-resume identity fields will be persisted and verified in P3. */
export interface ResumeIdentity {
	readonly sessionId: string;
	readonly canonicalPath: string;
}
