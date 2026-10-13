import { describe, expect, test } from "bun:test";
import { assertNoPrivateWorkspaceDeps } from "./release-packages";

const RELEASE_PACKAGE_NAMES = new Set(["@gajae-gateway/protocol", "@gajae-gateway/sdk"]);
const RELEASE_PACKAGE_NAMES_WITH_PRIVATE = new Set([...RELEASE_PACKAGE_NAMES, "@gajae-gateway/home"]);

const WORKSPACE_PACKAGES = new Map([
	["@gajae-gateway/protocol", { name: "@gajae-gateway/protocol", private: false }],
	["@gajae-gateway/sdk", { name: "@gajae-gateway/sdk", private: false }],
	["@gajae-gateway/home", { name: "@gajae-gateway/home", private: true }],
	["@gajae-gateway/log", { name: "@gajae-gateway/log", private: false }],
]);

const PRIVATE_WORKSPACE_DEPENDENCY_FIXTURES = [
	{
		section: "dependencies",
		manifest: { name: "@gajae-gateway/cli", dependencies: { "@gajae-gateway/home": "workspace:*" } },
	},
	{
		section: "optionalDependencies",
		manifest: { name: "@gajae-gateway/cli", optionalDependencies: { "@gajae-gateway/home": "workspace:*" } },
	},
	{
		section: "peerDependencies",
		manifest: { name: "@gajae-gateway/cli", peerDependencies: { "@gajae-gateway/home": "workspace:*" } },
	},
] as const;

describe("release package workspace dependency guard", () => {
	test("allows dependencies on released workspace packages and external packages", () => {
		const manifest = {
			name: "@gajae-gateway/cli",
			dependencies: {
				"@gajae-gateway/protocol": "0.1.8",
				"@gajae-gateway/sdk": "0.1.8",
				kleur: "^4.1.5",
			},
			optionalDependencies: { "optional-external": "^1.0.0" },
			peerDependencies: { "peer-external": "^2.0.0" },
		};

		expect(() => assertNoPrivateWorkspaceDeps(manifest, WORKSPACE_PACKAGES, RELEASE_PACKAGE_NAMES)).not.toThrow();
	});

	for (const fixture of PRIVATE_WORKSPACE_DEPENDENCY_FIXTURES) {
		test(`rejects private workspace dependencies in ${fixture.section}`, () => {
			expect(() => assertNoPrivateWorkspaceDeps(fixture.manifest, WORKSPACE_PACKAGES, RELEASE_PACKAGE_NAMES_WITH_PRIVATE)).toThrow(
				"@gajae-gateway/home",
			);
		});
	}

	test("rejects workspace packages missing from the release map even if they are not private", () => {
		const manifest = {
			name: "@gajae-gateway/cli",
			dependencies: { "@gajae-gateway/log": "0.1.0" },
		};

		expect(() => assertNoPrivateWorkspaceDeps(manifest, WORKSPACE_PACKAGES, RELEASE_PACKAGE_NAMES)).toThrow(
			"not in the release map",
		);
	});
});
