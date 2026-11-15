import { expect, test } from "bun:test";
import { join } from "node:path";
import { servicePinManifest } from "../../cli/src/services";
import { pinnedGjcCandidatePaths } from "../../gateway/src/orchestrator/broker";

/**
 * `gajaeway services install|repair` installs the pin from the target release manifest, and
 * the definitions it writes run `<binDir>/gajaeway-gateway`, whose runtime resolves the pin
 * beside itself. The two readers live in packages that may not import each other (see
 * sdk-boundary-dogfood), so the pairing that keeps them equal is pinned here: a release
 * broker must never enforce a pin other than the one the installer installed for it.
 */
test("the installer reads the pin the broker resolves beside its binary", () => {
	// The generated definitions run `<binDir>/gajaeway-gateway`; for a regular file there, the
	// installer's manifest and the runtime's first candidate must name the same file.
	const binDir = "/opt/gajaeway/bin";
	expect(servicePinManifest(binDir, { HOME: "/home/op" })).toBe(
		pinnedGjcCandidatePaths(join(binDir, "gajaeway-gateway"))[0],
	);
});

test("a compiled release resolves that manifest and nothing else", () => {
	// The installer has always had exactly one pin source for a target. The compiled runtime
	// now matches it: a release whose manifest cannot supply the pin fails closed instead of
	// enforcing the build checkout's pin, so the installer's manifest is the whole contract.
	const binDir = "/opt/gajaeway/bin";
	expect(pinnedGjcCandidatePaths(join(binDir, "gajaeway-gateway"), "/$bunfs/root/gajaeway-gateway")).toEqual([
		servicePinManifest(binDir, { HOME: "/home/op" }),
	]);
});
