export interface RuntimeConfig {
	/** PATH entries used by managed launch agents; an explicit list replaces login-shell discovery. */
	readonly path?: readonly string[];
	/** Whether service installation inherits the login-shell PATH when no explicit path is configured. */
	readonly inheritLoginPath?: boolean;
}

export class RuntimeConfigError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "RuntimeConfigError";
	}
}

export function parseRuntimeConfig(value: unknown): RuntimeConfig | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "object" || value === null || Array.isArray(value))
		throw new RuntimeConfigError("runtime must be an object");
	const input = value as Record<string, unknown>;
	if (Object.keys(input).some((key) => !["path", "inheritLoginPath"].includes(key)))
		throw new RuntimeConfigError("runtime contains an unknown field");
	let path: readonly string[] | undefined;
	if (input.path !== undefined) {
		if (!Array.isArray(input.path) || input.path.length === 0)
			throw new RuntimeConfigError("runtime.path must be a non-empty array");
		if (input.path.some((entry) => typeof entry !== "string" || entry.length === 0))
			throw new RuntimeConfigError("runtime.path entries must be non-empty strings");
		path = [...input.path] as string[];
	}
	const inheritLoginPath = input.inheritLoginPath;
	if (inheritLoginPath !== undefined && typeof inheritLoginPath !== "boolean")
		throw new RuntimeConfigError("runtime.inheritLoginPath must be boolean");
	if (path !== undefined && inheritLoginPath === true)
		throw new RuntimeConfigError("runtime.path cannot be combined with inheritLoginPath:true");
	return {
		...(path === undefined ? {} : { path }),
		...(inheritLoginPath === undefined ? {} : { inheritLoginPath }),
	};
}
