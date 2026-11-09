import { expect, test } from "bun:test";
import type { ImageFetch } from "@gajae-gateway/images";
import { describeInboundBody, describeSlackFile, prepareSlackImages } from "../src/attachments";

const ONE_PIXEL_PNG = Uint8Array.from(
	Buffer.from(
		"iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEklEQVR4nGP4z8DAAMIM/4EAAB/uBfsL2WiLAAAAAElFTkSuQmCC",
		"base64",
	),
);

test("Slack files use MIME kind, preferred name/url, and readable sizes", () => {
	expect(
		describeSlackFile({
			mimetype: "image/png",
			name: "photo.png",
			title: "title",
			size: 1.2 * 1024 * 1024,
			url_private: "https://files.slack.com/photo",
			permalink: "ignored",
		}),
	).toBe("[image · photo.png · 1.2 MB]");
	expect(
		describeSlackFile({
			name: " ",
			title: "clip",
			mimetype: "VIDEO/mp4",
			size: 1024,
			url_private: "private-url",
			permalink: "url",
		}),
	).toBe("[video · clip · 1 KB · private-url]");
	expect(describeSlackFile({ mimetype: "audio/ogg", size: 12 })).toBe("[audio · 12 B]");
	for (const size of [-1, Number.NaN, Number.POSITIVE_INFINITY]) expect(describeSlackFile({ size })).toBe("[file]");
});

test("Slack files name their file id so same-named attachments stay distinct", () => {
	const a = describeSlackFile({ id: "F0C8P71RK54", name: "image.png", mimetype: "image/png", size: 2048 });
	const b = describeSlackFile({ id: "F0C8PPGAV7G", name: "image.png", mimetype: "image/png", size: 2048 });
	expect(a).toBe("[image · image.png · id F0C8P71RK54 · 2 KB]");
	expect(b).toBe("[image · image.png · id F0C8PPGAV7G · 2 KB]");
	// Only a well-formed Slack file id is rendered: it sits inside the message text.
	for (const id of [
		"",
		"U0C8P71RK54",
		"F0c8p71",
		"F0C8P71RK54] [image",
		"F0C8P71RK54\n",
		"F0C8P71RK54/../../secret",
		"F1",
	])
		expect(describeSlackFile({ id, name: "image.png" })).toBe("[image · image.png]");
});

test("Slack inbound bodies retain captions, cap file rendering, and distinguish empty messages", () => {
	expect(describeInboundBody({})).toBe("");
	expect(describeInboundBody({ text: " untouched " })).toBe(" untouched ");
	expect(describeInboundBody({ text: "caption", files: [{ name: "one" }] })).toBe("caption\n[file · one]");
	expect(describeInboundBody({ text: "  ", files: [{}] })).toBe("[file]");
	for (const count of [11, 12]) {
		const lines = describeInboundBody({ files: Array.from({ length: count }, () => ({})) }).split("\n");
		expect(lines).toHaveLength(11);
		expect(lines[10]).toBe(`[+${count - 10} more attachment${count === 11 ? "" : "s"}]`);
	}
});

test("Slack native image preparation downloads only platform image private URLs with a restricted credentialed request", async () => {
	const requests: { url: string; init?: RequestInit }[] = [];
	const result = await prepareSlackImages(
		[
			{ mimetype: "application/pdf", url_private: "https://files.slack.com/not-an-image" },
			{
				mimetype: "image/png",
				url_private: "https://files.slack.com/files-pri/T1-F123/image.png",
				permalink: "https://evil.example/must-not-be-used",
			},
		],
		"xoxb-slack-test",
		async (input, init) => {
			requests.push({ url: String(input), init });
			return new Response(ONE_PIXEL_PNG, { status: 200, headers: { "content-type": "image/png" } });
		},
	);

	expect(requests).toHaveLength(1);
	expect(requests[0]?.url).toBe("https://files.slack.com/files-pri/T1-F123/image.png");
	expect(new Headers(requests[0]?.init?.headers).get("Authorization")).toBe("Bearer xoxb-slack-test");
	expect(requests[0]?.init?.redirect).toBe("error");
	expect(result.images).toHaveLength(1);
});

test("Slack image preparation rejects absent private URLs and off-host URLs without using permalinks", async () => {
	const requests: string[] = [];
	const fetchImpl: ImageFetch = async (input) => {
		requests.push(String(input));
		return new Response(ONE_PIXEL_PNG);
	};
	await expect(
		prepareSlackImages(
			[{ mimetype: "image/jpeg", permalink: "https://files.slack.com/public-looking-url" }],
			"xoxb-slack-test",
			fetchImpl,
		),
	).rejects.toThrow("missing its private URL");
	await expect(
		prepareSlackImages(
			[
				{
					mimetype: "image/jpeg",
					url_private: "https://evil.example/image.jpg",
					permalink: "https://files.slack.com/valid-looking-url",
				},
			],
			"xoxb-slack-test",
			fetchImpl,
		),
	).rejects.toThrow();
	expect(requests).toEqual([]);
});

test("Slack image preparation enforces the four-image limit before downloading", async () => {
	const requests: string[] = [];
	const fetchImpl: ImageFetch = async (input) => {
		requests.push(String(input));
		return new Response(ONE_PIXEL_PNG);
	};
	await expect(
		prepareSlackImages(
			Array.from({ length: 5 }, (_, index) => ({
				mimetype: "image/png",
				url_private: `https://files.slack.com/files-pri/T1-F12345${index}/photo.png`,
			})),
			"xoxb-slack-test",
			fetchImpl,
		),
	).rejects.toThrow("at most four images");
	expect(requests).toEqual([]);
});
