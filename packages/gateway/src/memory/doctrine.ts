import { appendFile, mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import { join, relative } from "node:path";

export const AXES = ["daily", "events", "tasks", "people", "projects", "channels", "decisions"] as const;
export type Axis = (typeof AXES)[number];

export function memoryRoot(home: string): string {
	return join(home, "memory");
}

function gitEnv(): Record<string, string> {
	return {
		PATH: process.env.PATH ?? "/usr/bin:/bin",
		HOME: process.env.HOME ?? "/tmp",
		GIT_AUTHOR_NAME: "gajaeway",
		GIT_AUTHOR_EMAIL: "gajaeway@local",
		GIT_COMMITTER_NAME: "gajaeway",
		GIT_COMMITTER_EMAIL: "gajaeway@local",
	};
}

export async function memoryGit(root: string, args: readonly string[]): Promise<string> {
	const child = Bun.spawn(["git", ...args], { cwd: root, env: gitEnv(), stdout: "pipe", stderr: "pipe" });
	const [stdout, stderr, code] = await Promise.all([
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
		child.exited,
	]);
	if (code !== 0) throw new Error(`memory git ${args[0]} failed: ${stderr.trim()}`);
	return stdout.trim();
}

export async function initializeMemory(home: string): Promise<string> {
	const root = memoryRoot(home);
	await mkdir(root, { recursive: true, mode: 0o700 });
	for (const axis of AXES) await mkdir(join(root, axis), { recursive: true, mode: 0o700 });
	try {
		await stat(join(root, ".git"));
	} catch {
		await memoryGit(root, ["init"]);
	}
	try {
		await readFile(join(root, "MEMORY.md"));
	} catch {
		await regenerateMap(root);
	}
	return root;
}

async function markdownFiles(root: string, axis: Axis): Promise<string[]> {
	const directory = join(root, axis);
	const entries = await readdir(directory, { withFileTypes: true });
	return entries
		.filter((entry) => entry.isFile() && entry.name.endsWith(".md"))
		.map((entry) => relative(root, join(directory, entry.name)).replaceAll("\\", "/"))
		.sort();
}

/** MEMORY.md contains navigation only; its pointers are regenerated from the canonical tree. */
export async function regenerateMap(root: string): Promise<void> {
	const files = await Promise.all(AXES.map((axis) => markdownFiles(root, axis)));
	const lines = ["# Memory map", "", "Generated pointers; canonical facts live in axis files.", ""];
	for (let index = 0; index < AXES.length; index++) {
		lines.push(`## ${AXES[index]}`, "");
		for (const path of files[index].slice(-20).reverse()) lines.push(`- [${path}](${path})`);
		lines.push("");
	}
	await writeFile(join(root, "MEMORY.md"), `${lines.join("\n")}\n`);
}

export async function appendDaily(
	root: string,
	originRefJson: string,
	userText: string,
	replyText: string,
): Promise<string> {
	const date = new Date().toISOString().slice(0, 10);
	const path = join(root, "daily", `${date}.md`);
	const bounded = (text: string) => text.slice(0, 500).replaceAll("\u0000", "");
	const entry = `\n## ${new Date().toISOString()}\n\n- origin: ${bounded(originRefJson)}\n- user: ${bounded(userText)}\n- reply: ${bounded(replyText)}\n`;
	await appendFile(path, entry, { encoding: "utf8" });
	await regenerateMap(root);
	return relative(root, path).replaceAll("\\", "/");
}
