export interface WayConfig {
	stateDir: string;
	profilePath: string;
}

/** P0 defaults only establish the config boundary; loading and validation arrive in P3. */
export function defaultConfig(): WayConfig {
	return {
		stateDir: ".way-state",
		profilePath: "ops/profiles/gaebal-gajae.example.toml",
	};
}
