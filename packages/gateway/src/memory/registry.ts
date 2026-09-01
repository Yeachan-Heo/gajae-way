/**
 * The memory axis registry.
 *
 * An axis is not a name in a list: it is a descriptor that says where its files
 * live, whether they nest, how they are indexed, what a legal file name looks
 * like, how strongly recall should prefer it, what the audit does with a file
 * found under it, whether its content may be rewritten, and which axes it
 * promotes into. Every other module in `memory/` reads those fields instead of
 * testing an axis id, so a deployment can add an axis without any of them
 * learning about it.
 *
 * Deployments extend the set through `<memory root>/axes.json`, never by editing
 * this module. There is exactly one list of built-ins, here.
 */

import { readFile } from "node:fs/promises";
import { join } from "node:path";

/** How an axis lays out directories beneath its canonical root. */
export type AxisNesting = "flat" | "nested";
/** How the generated map indexes the axis. */
export type AxisIndex = "recent" | "tree";
/** What a legal file name under the axis looks like. */
export type AxisLayout = "dated" | "free";
/** What the audit does with a Markdown file found under the axis root. */
export type AxisOrphanPolicy = "any-depth" | "partitioned";

export interface AxisDescriptor {
	/** Stable machine id; the map heading, audit messages and promotion targets all key off it. */
	readonly id: string;
	/** Human label rendered under the map heading. */
	readonly displayName: string;
	/** Canonical root, relative to the memory root. Roots may not overlap. */
	readonly root: string;
	/** `flat` keeps every file directly under the root; `nested` allows subdirectories at any depth. */
	readonly nesting: AxisNesting;
	/** Named subdirectories that partition a nested axis (`ops` -> rules/distillations/handoffs). */
	readonly partitions: readonly string[];
	/** `recent` maps the newest entries; `tree` maps the whole hierarchy grouped by partition. */
	readonly index: AxisIndex;
	/** `dated` restricts entries to `[YYYY-MM/]YYYY-MM-DD.md`; `free` accepts any `.md` name. */
	readonly layout: AxisLayout;
	/** Higher wins when two documents score equally during recall. */
	readonly retrievalPriority: number;
	/** `partitioned` rejects a file that is not inside a declared partition; `any-depth` accepts it. */
	readonly orphanPolicy: AxisOrphanPolicy;
	/** True when entries are only ever appended to, never rewritten in place. */
	readonly appendOnly: boolean;
	/** Axis ids this axis promotes durable material into. Must be acyclic. */
	readonly promotesTo: readonly string[];
}

/**
 * The one permitted entry name for a `dated` axis, optionally sharded by month.
 * Real month and day ranges, so `2026-99-99.md` is rejected as the typo it is
 * rather than being filed as a date nothing will ever sort next to.
 */
const MONTH = "(?:0[1-9]|1[0-2])";
export const DATED_ENTRY = new RegExp(`^(?:\\d{4}-${MONTH}/)?\\d{4}-${MONTH}-(?:0[1-9]|[12]\\d|3[01])\\.md$`);
/** A `tree` index lists every entry; the cap only stops a runaway corpus from swamping the map. */
export const TREE_INDEX_CAP = 200;
/** A `recent` index lists this many newest entries per axis. */
export const RECENT_INDEX_CAP = 20;

const ID = /^[a-z][a-z0-9-]*$/;
// Lowercase only: a case-insensitive filesystem would happily let `Ops` and `ops`
// name the same directory while the registry believed they were two axes.
const SEGMENT = /^[a-z0-9][a-z0-9._-]*$/;
/** Names the corpus already owns; an axis rooted at one of them would collide with a file. */
const RESERVED_ROOTS = new Set(["memory.md", "axes.json", "memory-receipts.jsonl", ".git"]);

export class AxisRegistryError extends Error {
	readonly code:
		| "malformed_descriptor"
		| "duplicate_axis_id"
		| "overlapping_axis_root"
		| "unknown_promotion_target"
		| "circular_promotion";
	constructor(code: AxisRegistryError["code"], message: string) {
		super(message);
		this.name = "AxisRegistryError";
		this.code = code;
	}
}

/**
 * The built-in axes. Each one states what it takes and what it refuses, because
 * the whole value of an axis set is that a writer can decide where a fact goes
 * without asking anyone.
 */
export const BUILT_IN_AXES: readonly AxisDescriptor[] = [
	{
		// Dated append-only capture of what was actually said; never curated fact.
		// Curated digests live in daily/YYYY-MM/, so the layout stays free: the raw
		// capture layer writes dated names, but corpora already contain digests.
		id: "daily",
		displayName: "Raw capture",
		root: "daily",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "free",
		retrievalPriority: 10,
		orphanPolicy: "any-depth",
		appendOnly: true,
		promotesTo: ["events", "tasks", "people", "projects", "channels", "decisions", "ops", "reflections"],
	},
	{
		// Something that happened at a point in time; not a plan and not a rule.
		id: "events",
		displayName: "Things that happened",
		root: "events",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "free",
		retrievalPriority: 20,
		orphanPolicy: "any-depth",
		appendOnly: false,
		promotesTo: [],
	},
	{
		// Work that is still owed, with its state; not a record of finished work.
		id: "tasks",
		displayName: "Work still owed",
		root: "tasks",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "free",
		retrievalPriority: 20,
		orphanPolicy: "any-depth",
		appendOnly: false,
		promotesTo: [],
	},
	{
		// Durable facts about a person; not what they said in one conversation.
		id: "people",
		displayName: "People",
		root: "people",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "free",
		retrievalPriority: 40,
		orphanPolicy: "any-depth",
		appendOnly: false,
		promotesTo: [],
	},
	{
		// Durable facts about an ongoing effort; not its individual work items.
		id: "projects",
		displayName: "Projects",
		root: "projects",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "free",
		retrievalPriority: 50,
		orphanPolicy: "any-depth",
		appendOnly: false,
		promotesTo: [],
	},
	{
		// Durable facts about a place we talk in; not the messages sent there.
		id: "channels",
		displayName: "Channels",
		root: "channels",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "free",
		retrievalPriority: 30,
		orphanPolicy: "any-depth",
		appendOnly: false,
		promotesTo: [],
	},
	{
		// A choice made at a point in time and why: context, options, chosen,
		// rationale, scope, timestamp, supersedes. `decisions` records what was
		// chosen; `ops` constrains what to do next. That is the whole boundary.
		id: "decisions",
		displayName: "Decisions",
		root: "decisions",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "free",
		retrievalPriority: 60,
		orphanPolicy: "any-depth",
		appendOnly: false,
		promotesTo: [],
	},
	{
		// Repeatable operating rules, runtime/session/tool procedure, principles
		// distilled from failure, and state the next executor picks up. Never raw
		// transcript, secrets, dated small talk or one-off dumps. Routable rather
		// than one growing file, so it is partitioned and indexed as a tree, and
		// recall prefers it over raw capture.
		id: "ops",
		displayName: "Operating doctrine",
		root: "ops",
		nesting: "nested",
		partitions: ["rules", "distillations", "handoffs"],
		index: "tree",
		layout: "free",
		retrievalPriority: 80,
		orphanPolicy: "partitioned",
		appendOnly: false,
		promotesTo: [],
	},
	{
		// What canonicalisation learned about our own behaviour: observed failure or
		// drift, the invariant learned, why it matters, the next action, and where it
		// promotes. Dated append-only entries, several per day; a per-subject file
		// must never become a second authority, so the layout admits exactly one
		// shape and a subject view can only ever be a generated projection.
		id: "reflections",
		displayName: "Reflections",
		root: "reflections",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "dated",
		retrievalPriority: 70,
		orphanPolicy: "any-depth",
		appendOnly: true,
		promotesTo: ["ops", "projects", "channels", "people"],
	},
];

export interface AxisRegistry {
	/** Registered axes in declaration order: built-ins first, then custom axes. */
	readonly axes: readonly AxisDescriptor[];
	/** Registered axes ordered by descending retrieval priority. */
	readonly byPriority: readonly AxisDescriptor[];
	byId(id: string): AxisDescriptor | undefined;
	/** The axis owning a root-relative Markdown path, or undefined when it belongs to none. */
	axisForPath(path: string): AxisDescriptor | undefined;
}

const FIELDS = new Set([
	"id",
	"displayName",
	"root",
	"nesting",
	"partitions",
	"index",
	"layout",
	"retrievalPriority",
	"orphanPolicy",
	"appendOnly",
	"promotesTo",
]);

function malformed(message: string): never {
	throw new AxisRegistryError("malformed_descriptor", message);
}

function stringArray(value: unknown, field: string, id: string, fallback: readonly string[]): readonly string[] {
	if (value === undefined) return fallback;
	if (!Array.isArray(value) || value.some((item) => typeof item !== "string"))
		malformed(`axis ${id}: ${field} must be an array of strings`);
	return value as readonly string[];
}

function oneOf<T extends string>(value: unknown, allowed: readonly T[], field: string, id: string, fallback: T): T {
	if (value === undefined) return fallback;
	if (typeof value !== "string" || !allowed.includes(value as T))
		malformed(`axis ${id}: ${field} must be one of ${allowed.join(", ")}`);
	return value as T;
}

/**
 * Normalize one declared descriptor. An omitted field falls back to `base` when
 * the declaration restates an existing axis, and to the documented default
 * otherwise, so restating `ops` to widen its partitions cannot silently demote
 * the rest of the axis to new-axis defaults. Anything present is validated
 * strictly, and an unknown field is a typo that would otherwise be ignored, so
 * it fails closed.
 */
export function parseDescriptor(value: unknown, base?: AxisDescriptor): AxisDescriptor {
	if (typeof value !== "object" || value === null || Array.isArray(value))
		malformed("axis descriptor must be an object");
	const raw = value as Record<string, unknown>;
	const id = raw.id;
	if (typeof id !== "string" || !ID.test(id))
		malformed(`axis id must match ${ID.source} (received ${JSON.stringify(id)})`);
	for (const key of Object.keys(raw)) if (!FIELDS.has(key)) malformed(`axis ${id}: unknown field ${key}`);

	const root = raw.root === undefined ? (base?.root ?? id) : raw.root;
	if (typeof root !== "string" || !root.length) malformed(`axis ${id}: root must be a non-empty string`);
	const segments = root.split("/");
	if (root.startsWith("/") || root.includes("\\") || segments.some((segment) => !SEGMENT.test(segment)))
		malformed(
			`axis ${id}: root must be a relative lowercase path of plain segments (received ${JSON.stringify(root)})`,
		);
	if (RESERVED_ROOTS.has(segments[0].toLowerCase()))
		malformed(`axis ${id}: root ${JSON.stringify(root)} collides with a reserved corpus name`);

	const displayName = raw.displayName === undefined ? (base?.displayName ?? id) : raw.displayName;
	if (typeof displayName !== "string" || !displayName.trim().length)
		malformed(`axis ${id}: displayName must be a non-empty string`);

	const nesting = oneOf(raw.nesting, ["flat", "nested"] as const, "nesting", id, base?.nesting ?? "nested");
	const partitions = stringArray(raw.partitions, "partitions", id, base?.partitions ?? []);
	for (const partition of partitions)
		if (!SEGMENT.test(partition)) malformed(`axis ${id}: partition ${JSON.stringify(partition)} is not a path segment`);
	if (new Set(partitions).size !== partitions.length) malformed(`axis ${id}: partitions contain a duplicate`);
	if (nesting === "flat" && partitions.length) malformed(`axis ${id}: a flat axis cannot declare partitions`);

	const index = oneOf(raw.index, ["recent", "tree"] as const, "index", id, base?.index ?? "recent");
	const layout = oneOf(raw.layout, ["dated", "free"] as const, "layout", id, base?.layout ?? "free");
	const orphanPolicy = oneOf(
		raw.orphanPolicy,
		["any-depth", "partitioned"] as const,
		"orphanPolicy",
		id,
		base?.orphanPolicy ?? "any-depth",
	);
	if (orphanPolicy === "partitioned" && !partitions.length)
		malformed(`axis ${id}: orphanPolicy partitioned requires at least one partition`);
	if (orphanPolicy === "partitioned" && layout === "dated")
		malformed(`axis ${id}: a dated axis cannot also be partitioned`);

	const retrievalPriority =
		raw.retrievalPriority === undefined ? (base?.retrievalPriority ?? 0) : raw.retrievalPriority;
	if (typeof retrievalPriority !== "number" || !Number.isFinite(retrievalPriority))
		malformed(`axis ${id}: retrievalPriority must be a finite number`);

	const appendOnly = raw.appendOnly === undefined ? (base?.appendOnly ?? layout === "dated") : raw.appendOnly;
	if (typeof appendOnly !== "boolean") malformed(`axis ${id}: appendOnly must be a boolean`);
	// A dated axis is dated precisely because entries accumulate; letting one be
	// rewritable would make the date meaningless.
	if (layout === "dated" && !appendOnly) malformed(`axis ${id}: a dated axis must be appendOnly`);

	return {
		id,
		displayName,
		root,
		nesting,
		partitions,
		index,
		layout,
		retrievalPriority,
		orphanPolicy,
		appendOnly,
		promotesTo: stringArray(raw.promotesTo, "promotesTo", id, base?.promotesTo ?? []),
	};
}

/** True when `inner` is `outer` or sits beneath it, compared segment-wise. */
function overlaps(outer: string, inner: string): boolean {
	return inner === outer || inner.startsWith(`${outer}/`);
}

function assertAcyclic(axes: readonly AxisDescriptor[]): void {
	const byId = new Map(axes.map((axis) => [axis.id, axis]));
	for (const axis of axes)
		for (const target of axis.promotesTo)
			if (!byId.has(target))
				throw new AxisRegistryError(
					"unknown_promotion_target",
					`axis ${axis.id} promotes into unregistered axis ${target}`,
				);
	const state = new Map<string, "open" | "done">();
	const walk = (axis: AxisDescriptor, trail: readonly string[]): void => {
		const seen = state.get(axis.id);
		if (seen === "done") return;
		if (seen === "open")
			throw new AxisRegistryError("circular_promotion", `circular promotion: ${[...trail, axis.id].join(" -> ")}`);
		state.set(axis.id, "open");
		for (const target of axis.promotesTo) {
			const next = byId.get(target);
			if (next) walk(next, [...trail, axis.id]);
		}
		state.set(axis.id, "done");
	};
	for (const axis of axes) walk(axis, []);
}

/**
 * Build a registry from the built-ins plus declared custom axes. A declaration
 * whose id names a built-in *overrides* it: stated fields win, omitted fields
 * keep the built-in's value. The built-in set is a default, not a floor, so a
 * deployment whose `ops` corpus grew its own partitions restates just those
 * partitions instead of editing this module or moving files, and cannot demote
 * the rest of the axis to new-axis defaults by omission.
 * Everything else fails closed: two declarations may not share an id, a root may
 * not contain or sit inside another axis's root, and a promotion target must
 * exist and must not close a cycle.
 */
export function createRegistry(custom: readonly unknown[] = []): AxisRegistry {
	const axes = [...BUILT_IN_AXES];
	const declaredIds = new Set<string>();
	for (const declared of custom) {
		const id = (declared as { id?: unknown })?.id;
		const axis = parseDescriptor(
			declared,
			axes.find((existing) => existing.id === id),
		);
		if (declaredIds.has(axis.id))
			throw new AxisRegistryError("duplicate_axis_id", `axis id ${axis.id} is declared twice`);
		declaredIds.add(axis.id);
		const replaced = axes.findIndex((existing) => existing.id === axis.id);
		const others = axes.filter((_, index) => index !== replaced);
		const overlap = others.find((existing) => overlaps(existing.root, axis.root) || overlaps(axis.root, existing.root));
		if (overlap)
			throw new AxisRegistryError(
				"overlapping_axis_root",
				`axis ${axis.id} root ${axis.root} overlaps axis ${overlap.id} root ${overlap.root}`,
			);
		if (replaced === -1) axes.push(axis);
		else axes[replaced] = axis;
	}
	assertAcyclic(axes);
	const byId = new Map(axes.map((axis) => [axis.id, axis]));
	// Longest root first, so a deeply rooted axis wins over a shallower one. Roots
	// cannot overlap, but resolving by longest match keeps that independent of
	// registration order.
	const byRootLength = [...axes].sort((a, b) => b.root.length - a.root.length);
	return {
		axes,
		byPriority: [...axes].sort((a, b) => b.retrievalPriority - a.retrievalPriority || a.id.localeCompare(b.id)),
		byId: (id) => byId.get(id),
		axisForPath: (path) => byRootLength.find((axis) => path.startsWith(`${axis.root}/`)),
	};
}

/** Where a deployment declares its custom axes. */
export const REGISTRY_FILE = "axes.json";

/**
 * Load the registry for a corpus. A corpus with no `axes.json` gets exactly the
 * built-ins, which is what every existing corpus is. A malformed or unreadable
 * declaration is fatal rather than ignored: silently dropping a registered axis
 * would make every file under it an orphan on the next audit.
 */
export async function loadRegistry(root: string): Promise<AxisRegistry> {
	let body: string;
	try {
		body = await readFile(join(root, REGISTRY_FILE), "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return createRegistry();
		throw error;
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(body);
	} catch (error) {
		throw new AxisRegistryError("malformed_descriptor", `${REGISTRY_FILE} is not valid JSON: ${String(error)}`);
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
		throw new AxisRegistryError("malformed_descriptor", `${REGISTRY_FILE} must be an object with an axes array`);
	const { axes } = parsed as { axes?: unknown };
	if (!Array.isArray(axes))
		throw new AxisRegistryError("malformed_descriptor", `${REGISTRY_FILE} must declare an axes array`);
	return createRegistry(axes);
}

/**
 * Why a Markdown path is not a legal entry of its axis, or undefined when it is.
 * This is the single place layout policy is interpreted, so no caller needs to
 * know which axis it is looking at.
 */
export function layoutViolation(axis: AxisDescriptor, path: string): string | undefined {
	const relative = path.slice(axis.root.length + 1);
	if (axis.nesting === "flat" && relative.includes("/"))
		return `${axis.id} is flat: entries live directly in ${axis.root}/`;
	if (axis.layout === "dated" && !DATED_ENTRY.test(relative))
		return `${axis.id} entries are dated: ${axis.root}/[YYYY-MM/]YYYY-MM-DD.md, not per-subject files`;
	if (axis.orphanPolicy === "partitioned") {
		const partition = relative.split("/")[0];
		if (!relative.includes("/") || !axis.partitions.includes(partition))
			return `${axis.id} entries live in a declared partition: ${axis.partitions.map((name) => `${axis.root}/${name}/`).join(", ")}`;
	}
	return undefined;
}
