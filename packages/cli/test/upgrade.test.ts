import { describe, expect, it } from "bun:test";
import { performUpgrade } from "../src/upgrade";

describe("ops upgrade CI gate", () => {
	it("refuses upgrade when CI has not passed", async () => {
		const mockCheckCiStatus = async () => false;

		const result = await performUpgrade({
			home: "/tmp/test-home",
			checkCiStatus: mockCheckCiStatus,
		});

		expect(result.status).toBe("failed");
		expect(result.detail).toContain("latest CI run did not pass");
	});

	it("proceeds when CI has passed", async () => {
		const mockCheckCiStatus = async () => true;

		// This will fail at the gjc version check step, but that's OK
		// We're just testing the CI gate passes
		const result = await performUpgrade({
			home: "/tmp/test-home",
			checkCiStatus: mockCheckCiStatus,
		});

		// Should fail at gjc --version step, not CI gate step
		expect(result.status).toBe("failed");
		expect(result.detail).not.toContain("CI");
	});

	it("reports CI check errors", async () => {
		const mockCheckCiStatus = async () => {
			throw new Error("CI API unreachable");
		};

		const result = await performUpgrade({
			home: "/tmp/test-home",
			checkCiStatus: mockCheckCiStatus,
		});

		expect(result.status).toBe("failed");
		expect(result.detail).toContain("CI status check failed");
		expect(result.detail).toContain("CI API unreachable");
	});
});
