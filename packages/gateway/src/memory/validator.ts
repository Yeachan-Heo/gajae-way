import { readdir, readFile, stat } from "node:fs/promises";
import { join, normalize, relative } from "node:path";
import { AXES } from "./doctrine";

export interface MemoryIssue {
	code:
		| "map_dangling"
		| "unmapped_axis_dir"
		| "long_form_map"
		| "duplicate_file_hash"
		| "orphan_file"
		| "out_of_root_link"
		| "map_content_drift";
	path: string;
	message: string;
}

async function files(root: string, directory = ""): Promise<string[]> {
	const result: string[] = [];
	for (const entry of await readdir(join(root, directory), { withFileTypes: true })) {
		if (entry.name === ".git") continue;
		const path = directory ? `${directory}/${entry.name}` : entry.name;
		if (entry.isDirectory()) result.push(...(await files(root, path)));
		else if (entry.isFile() && entry.name.endsWith(".md")) result.push(path);
	}
	return result.sort();
}

export async function validateMemory(root: string): Promise<MemoryIssue[]> {
	const issues: MemoryIssue[] = [];
	const mapPath = join(root, "MEMORY.md");
	const map = await readFile(mapPath, "utf8");
	const pointers = [...map.matchAll(/\]\(([^)]+\.md)\)/g)].map((match) => match[1]);
	for (const [index, line] of map.split("\n").entries())
		if (line.length > 200)
			issues.push({ code: "long_form_map", path: "MEMORY.md", message: `line ${index + 1} exceeds 200 characters` });
	for (const pointer of pointers) {
		const target = normalize(join(root, pointer));
		if (relative(root, target).startsWith(".."))
			issues.push({ code: "out_of_root_link", path: "MEMORY.md", message: `pointer escapes root: ${pointer}` });
		else
			try {
				await stat(target);
			} catch {
				issues.push({ code: "map_dangling", path: "MEMORY.md", message: `pointer missing: ${pointer}` });
			}
	}
	for (const axis of AXES)
		if (!new RegExp(`## ${axis}(?:\\n|$)`).test(map))
			issues.push({ code: "unmapped_axis_dir", path: "MEMORY.md", message: `axis missing from map: ${axis}` });
	const markdown = await files(root);
	const hashes = new Map<string, string>();
	for (const path of markdown) {
		if (path !== "MEMORY.md" && !AXES.some((axis) => path.startsWith(`${axis}/`)))
			issues.push({ code: "orphan_file", path, message: "Markdown file is outside an axis directory" });
		const content = await readFile(join(root, path), "utf8");
		const hash = new Bun.CryptoHasher("sha256").update(content).digest("hex");
		const first = hashes.get(hash);
		if (first) issues.push({ code: "duplicate_file_hash", path, message: `same content as ${first}` });
		else hashes.set(hash, path);
		for (const match of content.matchAll(/\]\(([^)#]+)(?:#[^)]+)?\)/g)) {
			const target = normalize(join(root, path, "..", match[1]));
			if (relative(root, target).startsWith(".."))
				issues.push({ code: "out_of_root_link", path, message: `link escapes root: ${match[1]}` });
		}
	}
	const daily = markdown.filter((path) => path.startsWith("daily/")).sort();
	const mapDaily = pointers.filter((path) => path.startsWith("daily/")).sort();
	if (daily.length && (!mapDaily.length || mapDaily.at(-1)! < daily.at(-1)!))
		issues.push({ code: "map_content_drift", path: "MEMORY.md", message: "map does not include newest daily file" });
	return issues;
}
