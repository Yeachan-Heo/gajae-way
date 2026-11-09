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

test("Slack image descriptions preserve unsupported image and PDF URLs while omitting native PNG URLs", async () => {
	const files = [
		{
			name: "animation.gif",
			mimetype: "image/gif",
			url_private: "https://files.slack.com/animation.gif",
			permalink: "https://slack-files.example/animation.gif",
		},
		{ name: "report.pdf", mimetype: "application/pdf", url_private: "https://files.slack.com/report.pdf" },
		{ name: "native.png", mimetype: "image/png", url_private: "https://files.slack.com/native.png" },
	];
	expect(describeInboundBody({ text: "caption", files })).toBe(
		"caption\n[image · animation.gif · https://files.slack.com/animation.gif]\n" +
			"[file · report.pdf · https://files.slack.com/report.pdf]\n" +
			"[image · native.png]",
	);

	const requested: string[] = [];
	const prepared = await prepareSlackImages(files, "xoxb-slack-test", async (input) => {
		requested.push(String(input));
		return new Response(ONE_PIXEL_PNG);
	});
	expect(requested).toEqual(["https://files.slack.com/native.png"]);
	expect(prepared.images).toHaveLength(1);
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
	expect(requests[0]?.init?.redirect).toBe("manual");
	expect(result.images).toHaveLength(1);
});

test("Slack follows a signed CDN redirect without forwarding Authorization or cookies", async () => {
	const requests: { url: string; init?: RequestInit }[] = [];
	let redirectBodyCancelled = false;
	const result = await prepareSlackImages(
		[
			{
				mimetype: "image/jpeg",
				url_private: "https://files.slack.com/files-pri/T1-F123/photo.jpg",
			},
		],
		"xoxb-slack-test",
		async (input, init) => {
			requests.push({ url: String(input), init });
			if (requests.length === 1) {
				const body = new ReadableStream<Uint8Array>({
					start(controller) {
						controller.enqueue(new Uint8Array([1]));
					},
					cancel() {
						redirectBodyCancelled = true;
					},
				});
				return new Response(body, {
					status: 302,
					headers: { location: "https://files-edge.slack.com/signed/image?token=temporary" },
				});
			}
			return new Response(ONE_PIXEL_PNG, { status: 200, headers: { "content-type": "image/png" } });
		},
	);

	expect(requests.map(({ url }) => url)).toEqual([
		"https://files.slack.com/files-pri/T1-F123/photo.jpg",
		"https://files-edge.slack.com/signed/image?token=temporary",
	]);
	expect(requests.map(({ init }) => init?.redirect)).toEqual(["manual", "manual"]);
	expect(new Headers(requests[0]?.init?.headers).get("authorization")).toBe("Bearer xoxb-slack-test");
	expect(new Headers(requests[1]?.init?.headers).get("authorization")).toBeNull();
	expect(new Headers(requests[1]?.init?.headers).get("cookie")).toBeNull();
	expect(redirectBodyCancelled).toBe(true);
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
