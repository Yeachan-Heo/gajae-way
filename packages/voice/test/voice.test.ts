import { expect, test } from "bun:test";
import { classifyProviderError, formatVoiceDuration } from "../src/index";

test("an OpenAI-compatible error envelope is classified and redacted", async () => {
	const failure = await classifyProviderError(
		Response.json(
			{
				error: {
					message: "Invalid voice for key sk-live-abcdef123456",
					type: "invalid_request_error",
					code: "invalid_voice",
				},
			},
			{ status: 400 },
		),
	);
	expect(failure.code).toBe("invalid_voice");
	expect(failure.category).toBe("provider_error");
	expect(failure.diagnostic).not.toContain("sk-live-abcdef123456");
	const auth = await classifyProviderError(Response.json({ error: { message: "bad key" } }, { status: 401 }));
	expect(auth.code).toBe("authentication");
});

test("ElevenLabs fields still take precedence over an error envelope", async () => {
	const failure = await classifyProviderError(
		Response.json({ detail: { code: "voice_not_found" }, error: { code: "other" } }, { status: 404 }),
	);
	expect(failure.code).toBe("voice_not_found");
});

test("voice durations read in seconds under a minute and m:ss above", () => {
	expect(formatVoiceDuration(4.26)).toBe("4.3s");
	expect(formatVoiceDuration(75)).toBe("1:15");
});
