import { expect, test } from "bun:test";
import { mkdtemp, open, rm, truncate, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadDiscordMedia, parseDiscordMedia, readDiscordMediaFile } from "../src/outgoing-attachments";

// Purpose: pin the explicit standalone directive contract and protect ordinary inline text.
// Expected: only whole MEDIA lines become paths, while other text is preserved; these assertions complete the test.
test("extracts standalone MEDIA lines and preserves ordinary text", () => {
	expect(parseDiscordMedia("Here is the file:\r\nMEDIA:/tmp/a source.cpp\r\nThanks")).toEqual({
		text: "Here is the file:\nThanks",
		paths: ["/tmp/a source.cpp"],
	});
	expect(parseDiscordMedia("Inline MEDIA:/tmp/file.txt is not a directive")).toEqual({
		text: "Inline MEDIA:/tmp/file.txt is not a directive",
		paths: [],
	});
});

// Purpose: reject malformed directives and batches Discord cannot accept.
// Expected: empty paths and more than ten files are rejected; both boundaries are required for completion.
test("rejects empty MEDIA paths and more than ten files", () => {
	expect(() => parseDiscordMedia("MEDIA:")).toThrow("requires a file path");
	const tooMany = Array.from({ length: 11 }, (_, index) => `MEDIA:/tmp/${index}.txt`).join("\n");
	expect(() => parseDiscordMedia(tooMany)).toThrow("at most 10 files");
});

// Purpose: ensure a file changing after its open-handle stat cannot become an incomplete successful attachment.
// Expected: truncation, growth, and same-size writes all reject before upload construction.
test("rejects files truncated, extended, or rewritten during preparation", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-media-race-"));
	try {
		const truncatedPath = join(home, "truncated.bin");
		await writeFile(truncatedPath, "original");
		const truncated = await open(truncatedPath, "r");
		try {
			const snapshot = await truncated.stat({ bigint: true });
			await truncate(truncatedPath, 2);
			await expect(readDiscordMediaFile(truncated, snapshot)).rejects.toThrow("file changed");
		} finally {
			await truncated.close();
		}

		const extendedPath = join(home, "extended.bin");
		await writeFile(extendedPath, "before");
		const extended = await open(extendedPath, "r");
		try {
			const snapshot = await extended.stat({ bigint: true });
			await writeFile(extendedPath, "after+", { flag: "a" });
			await expect(readDiscordMediaFile(extended, snapshot)).rejects.toThrow("file changed");
		} finally {
			await extended.close();
		}

		const rewrittenPath = join(home, "rewritten.bin");
		await writeFile(rewrittenPath, "before");
		const rewritten = await open(rewrittenPath, "r");
		try {
			const snapshot = await rewritten.stat({ bigint: true });
			await writeFile(rewrittenPath, "after!");
			await expect(readDiscordMediaFile(rewritten, snapshot)).rejects.toThrow("file changed");
		} finally {
			await rewritten.close();
		}
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

// Purpose: verify the accepted file-count boundary at the upload loader, not only in its parser caller.
// Expected: ten files load; eleven direct paths reject before opening, both required for completion.
test("accepts ten files and rejects eleven at the upload boundary", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-media-count-"));
	try {
		const paths = await Promise.all(
			Array.from({ length: 10 }, async (_, index) => {
				const path = join(home, `${index}.txt`);
				await writeFile(path, "x");
				return path;
			}),
		);
		expect(await loadDiscordMedia(paths)).toHaveLength(10);
		await expect(loadDiscordMedia(Array(11).fill(paths[0] as string))).rejects.toThrow("at most 10 files");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

// Purpose: ensure uploads accept regular absolute files only and enforce the aggregate memory/upload bound.
// Expected: relative paths, directories, oversized files, and multiple files over 25 MiB all reject.
test("rejects relative paths, directories, and oversized uploads", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-media-validation-"));
	try {
		const oversized = join(home, "oversized.bin");
		const first = join(home, "first.bin");
		const second = join(home, "second.bin");
		await writeFile(oversized, "");
		await truncate(oversized, 25 * 1024 * 1024 + 1);
		await writeFile(first, "");
		await writeFile(second, "");
		await truncate(first, 13 * 1024 * 1024);
		await truncate(second, 13 * 1024 * 1024);
		await expect(loadDiscordMedia(["relative.txt"])).rejects.toThrow("must be absolute");
		await expect(loadDiscordMedia([home])).rejects.toThrow("regular file");
		await expect(loadDiscordMedia([oversized])).rejects.toThrow("25 MiB");
		await expect(loadDiscordMedia([first, second])).rejects.toThrow("25 MiB");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
