import { readFile, stat } from "node:fs/promises";
import { dirname, join } from "node:path";
import { corpusEntries, mapListsAxis, safePointer } from "./doctrine";
import { type AxisRegistry, layoutViolation, loadRegistry, REGISTRY_FILE } from "./registry";

export interface MemoryIssue {
	code:
		| "map_dangling"
		| "unmapped_axis_dir"
		| "long_form_map"
		| "duplicate_file_hash"
		| "orphan_file"
		| "out_of_root_link"
		| "axis_layout_violation"
		| "map_content_drift";
	path: string;
	message: string;
}

export async function validateMemory(root: string, registry?: AxisRegistry): Promise<MemoryIssue[]> {
	const resolved = registry ?? (await loadRegistry(root));
	const axes = resolved.axes;
	const issues: MemoryIssue[] = [];
	const mapPath = join(root, "MEMORY.md");
	const map = await readFile(mapPath, "utf8");
	const pointers = [...map.matchAll(/\]\(([^)]+\.md)\)/g)].map((match) => match[1]);
	// The rule bans long-form prose in a navigation-only map, so a line is measured
	// as what it says, not as what it points at: a generated `- [path](path)`
	// pointer counts its path once. Otherwise a legitimately deep corpus path makes
	// the generated map fail an audit that no operator action could ever clear.
	for (const [index, line] of map.split("\n").entries())
		if (line.replaceAll(/\[([^\]]*)\]\([^)]*\)/g, "$1").length > 200)
			issues.push({ code: "long_form_map", path: "MEMORY.md", message: `line ${index + 1} exceeds 200 characters` });
	for (const pointer of pointers) {
		// Confinement is judged once, by the link scan below, which sees MEMORY.md as
		// an ordinary corpus file. Here an unfollowable pointer is simply not a
		// dangling one, so the map does not earn two issues for the same link.
		const target = safePointer(root, pointer);
		if (target === undefined) continue;
		try {
			await stat(join(root, target));
		} catch {
			issues.push({ code: "map_dangling", path: "MEMORY.md", message: `pointer missing: ${pointer}` });
		}
	}
	for (const axis of axes)
		if (!mapListsAxis(map, axis))
			issues.push({ code: "unmapped_axis_dir", path: "MEMORY.md", message: `axis missing from map: ${axis.id}` });
	const markdown = await corpusEntries(root);
	const hashes = new Map<string, string>();
	for (const path of markdown) {
		// An unknown directory is never promoted to canonical by being written into:
		// membership comes from the registry and nothing else. Nesting is legitimate
		// inside a registered root, so this is a root test rather than a depth test,
		// and layout policy is what constrains where a file may sit within its axis.
		const axis = path === "MEMORY.md" ? undefined : resolved.axisForPath(path);
		if (path !== "MEMORY.md" && !axis)
			issues.push({
				code: "orphan_file",
				path,
				message: `Markdown file is outside every registered axis (register it in ${REGISTRY_FILE} to make it canonical)`,
			});
		if (axis) {
			const violation = layoutViolation(axis, path);
			if (violation) issues.push({ code: "axis_layout_violation", path, message: violation });
		}
		const content = await readFile(join(root, path), "utf8");
		const hash = new Bun.CryptoHasher("sha256").update(content).digest("hex");
		const first = hashes.get(hash);
		if (first) issues.push({ code: "duplicate_file_hash", path, message: `same content as ${first}` });
		else hashes.set(hash, path);
		// Judged with the same confinement rule recall follows: percent-escapes are
		// decoded first, so `%2e%2e/secret.md` is reported as the escape it is
		// rather than shrugged off as a missing file.
		for (const match of content.matchAll(/\]\(([^)#]+)(?:#[^)]+)?\)/g))
			if (safePointer(root, match[1], dirname(path)) === undefined)
				issues.push({ code: "out_of_root_link", path, message: `link escapes root: ${match[1]}` });
	}
	// An append-only axis is read newest-first, so its newest entry being
	// unreachable from the map is a real navigation failure, not cosmetic lag.
	for (const axis of axes.filter((candidate) => candidate.appendOnly)) {
		const newest = markdown
			.filter((path) => path.startsWith(`${axis.root}/`))
			.sort()
			.at(-1);
		const newestMapped = pointers
			.filter((path) => path.startsWith(`${axis.root}/`))
			.sort()
			.at(-1);
		if (newest !== undefined && (newestMapped === undefined || newestMapped < newest))
			issues.push({
				code: "map_content_drift",
				path: "MEMORY.md",
				message: `map does not include newest ${axis.id} file`,
			});
	}
	return issues;
}
