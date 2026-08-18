export interface ProfileInjection {
	files: readonly string[];
}

export interface OwnerSurface {
	id: string;
	platform: string;
	kind: string;
}

export interface WayProfile {
	corpusPath: string;
	workspace: string;
	injection: ProfileInjection;
	ownerSurface: OwnerSurface;
}
