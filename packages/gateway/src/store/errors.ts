export class DatabaseStartupError extends Error {
	readonly code: "newer_schema" | "integrity_check_failed" | "migration_corrupt" | "schema_mismatch" | "quick_check_failed";

	constructor(code: DatabaseStartupError["code"], message: string) {
		super(message);
		this.name = "DatabaseStartupError";
		this.code = code;
	}
}

export class BrokerAuthorityError extends Error {
	constructor(
		readonly code:
			| "cutover_required"
			| "authority_mismatch"
			| "unowned_session"
			| "old_work_open"
			| "quarantined"
			| "invalid_authority",
	) {
		super(`broker authority: ${code}`);
		this.name = "BrokerAuthorityError";
	}
}
