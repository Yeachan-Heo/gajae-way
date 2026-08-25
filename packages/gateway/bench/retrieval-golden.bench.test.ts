import { expect, test } from "bun:test";
import { cp, mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initializeMemory, regenerateMap } from "../src/memory/doctrine";
import { searchMemory } from "../src/memory/retrieve";

type Golden = { query: string; expectedTopPath: string };
const corpus = join(import.meta.dir, "fixtures/corpus");
const pinnedHash = "dde74fa6af0b3d76cf8623ddc96ea26dce418bf24d863646f538fa4403cea531";
const benchmark = process.env.GAJAEWAY_BENCH === "1" ? test : test.skip;

benchmark("frozen golden retrieval corpus has Hit@3 of 100%", async () => {
	const names = (await readdir(corpus)).sort();
	const contents = await Promise.all(names.map((name) => readFile(join(corpus, name))));
	const hash = new Bun.CryptoHasher("sha256").update(Buffer.concat(contents)).digest("hex");
	expect(hash).toBe(pinnedHash);
	const home = await mkdtemp(join(tmpdir(), "gajaeway-retrieval-bench-"));
	try {
		const root = await initializeMemory(home);
		await cp(corpus, join(root, "projects"), { recursive: true });
		await regenerateMap(root);
		const golden = JSON.parse(await readFile(join(import.meta.dir, "golden-queries.json"), "utf8")) as Golden[];
		const ranks: number[] = [];
		for (const entry of golden) {
			const hits = await searchMemory(root, entry.query, 3);
			const rank = hits.findIndex((hit) => hit.path === entry.expectedTopPath) + 1;
			ranks.push(rank);
			console.log(
				`retrieval-golden query=${JSON.stringify(entry.query)} expected=${entry.expectedTopPath} rank=${rank}`,
			);
		}
		expect(ranks.filter((rank) => rank > 0 && rank <= 3)).toHaveLength(golden.length);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
