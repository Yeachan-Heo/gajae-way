/**
 * The console document: one HTML file, one stylesheet, one script, zero
 * dependencies and no external assets.
 *
 * Division of labour, and it is the reason this file has no protocol knowledge:
 * the server ships finished strings inside `RowView`s and the row markup that
 * holds them, and the client only ever maps a field name onto a node's
 * `textContent`. Rows are reconciled by key, so a refresh never replaces a
 * panel - scroll position, text selection and open `<details>` all survive - and
 * the browser never builds HTML from server data, so there is no injection
 * surface at all.
 *
 * The first paint is complete without JavaScript: every row below is
 * server-rendered from the same snapshot the client later reconciles against.
 * The `<template>` elements carry the identical markup with empty fields, which
 * is how the client can create a new row without a second copy of the markup
 * existing anywhere.
 */

import { formatClock, formatClockSeconds, formatDuration } from "./format";
import type { MutationOperation } from "./gate";
import { TURN_CEILING_MS, TURN_STALL_MS } from "./turns";
import type { ConsoleSnapshot, Panel, RowView } from "./view";

function escapeHtml(value: string): string {
	return value
		.replaceAll("&", "&amp;")
		.replaceAll("<", "&lt;")
		.replaceAll(">", "&gt;")
		.replaceAll('"', "&quot;")
		.replaceAll("'", "&#39;");
}

/**
 * `11m ago`. Takes epoch milliseconds rather than a Date because this exact
 * function is also shipped into the page (see SHARED_SOURCE) and the client
 * only ever has a millisecond clock.
 */
function relative(iso: string, atMs: number): string {
	const then = new Date(iso).getTime();
	if (Number.isNaN(then)) return "—";
	const diff = atMs - then;
	if (diff < 10_000) return "just now";
	return `${formatDuration(diff)} ago`;
}

/**
 * One algorithm, two runtimes.
 *
 * Ages tick in the browser, so the client cannot ask the server to re-render
 * "11m ago" every second - it must compute durations itself. Restating the
 * algorithm in the client script is the obvious way to do that and it is a trap:
 * the hand-written mirror had already drifted from `formatDuration` by one
 * commit, which would have made every age change format one second after load.
 * So the real implementations are serialised into the page instead. There is
 * exactly one duration algorithm in this package.
 */
const SHARED_SOURCE = [
	`var formatDuration = ${formatDuration.toString()};`,
	`var formatClock = ${formatClock.toString()};`,
	`var formatClockSeconds = ${formatClockSeconds.toString()};`,
	`var relative = ${relative.toString()};`,
].join("\n");

type Ctx = { readonly at: Date };

/** One field cell, filled from the row or left empty to serve as the template. */
function cell(row: RowView | null, name: string, className: string, ctx: Ctx, tag = "span"): string {
	const iso = row?.ages?.[name];
	const value = iso ? relative(iso, ctx.at.getTime()) : (row?.fields[name] ?? "");
	const tone = row?.tones?.[name];
	const attrs = [
		`class="${className}"`,
		`data-field="${escapeHtml(name)}"`,
		tone ? `data-tone="${escapeHtml(tone)}"` : "",
		iso ? `data-age="${escapeHtml(iso)}"` : "",
	]
		.filter(Boolean)
		.join(" ");
	return `<${tag} ${attrs}>${escapeHtml(value)}</${tag}>`;
}

function meter(row: RowView | null, name: string): string {
	const value = row?.meters?.[name];
	const attrs = [
		`class="row__meter"`,
		`data-meter="${escapeHtml(name)}"`,
		`max="${value?.max ?? 1}"`,
		`value="${value?.value ?? 0}"`,
		value ? "" : "hidden",
	]
		.filter(Boolean)
		.join(" ");
	return `<progress ${attrs}></progress>`;
}

function rowAttrs(row: RowView | null): string {
	if (!row) return `class="row" data-tone="muted"`;
	return [
		`class="row"`,
		`data-key="${escapeHtml(row.key)}"`,
		`data-tone="${escapeHtml(row.tone)}"`,
		row.state ? `data-state="${escapeHtml(row.state)}"` : "",
	]
		.filter(Boolean)
		.join(" ");
}

function attentionRow(row: RowView | null, ctx: Ctx): string {
	return `<li ${rowAttrs(row)}>
<p class="row__title">${cell(row, "title", "row__symptom", ctx, "strong")}<span class="row__spacer"></span>${cell(row, "age", "row__age", ctx, "time")}</p>
<p class="row__detail">${cell(row, "detail", "row__cause", ctx)}</p>
<p class="row__meta">${cell(row, "meta", "mono", ctx)}</p>
</li>`;
}

function liveRow(row: RowView | null, ctx: Ctx): string {
	return `<li ${rowAttrs(row)}>
<p class="row__title">${cell(row, "title", "row__name", ctx, "strong")}<span class="row__spacer"></span>${cell(row, "elapsed", "row__age mono", ctx)}</p>
<p class="row__state">${cell(row, "stateLabel", "row__badge", ctx)}</p>
${meter(row, "progress")}
<p class="row__meta">${cell(row, "evidence", "row__evidence", ctx)}<span class="row__dot">·</span>last event ${cell(row, "lastEvent", "row__age", ctx, "time")}</p>
<p class="row__meta">${cell(row, "turn", "mono", ctx)}</p>
</li>`;
}

function sessionRow(row: RowView | null, ctx: Ctx): string {
	return `<li ${rowAttrs(row)}>
<p class="row__title">${cell(row, "title", "row__name", ctx, "strong")}<span class="row__spacer"></span>${cell(row, "activity", "row__age", ctx, "time")}</p>
<p class="row__meta">${cell(row, "epoch", "row__tag", ctx)}<span class="row__dot">·</span>opened ${cell(row, "created", "row__age", ctx, "time")}</p>
<p class="row__meta">${cell(row, "key", "mono row__key", ctx)}</p>
</li>`;
}

function monitorRow(row: RowView | null, ctx: Ctx): string {
	return `<li ${rowAttrs(row)}>
<p class="row__title">${cell(row, "name", "row__name", ctx, "strong")}<span class="row__spacer"></span>${cell(row, "outcome", "row__badge", ctx)}</p>
<dl class="row__facts">
<dt>Fires</dt><dd>${cell(row, "trigger", "", ctx)}</dd>
<dt>Next</dt><dd>${cell(row, "next", "", ctx)}</dd>
<dt>Emits</dt><dd>${cell(row, "emits", "", ctx)}</dd>
<dt>Posts to</dt><dd>${cell(row, "target", "", ctx)}</dd>
<dt>Last</dt><dd>${cell(row, "outcomeAge", "row__age", ctx, "time")}</dd>
</dl>
<p class="row__meta">${cell(row, "id", "mono row__key", ctx)}</p>
</li>`;
}

function auditRow(row: RowView | null, ctx: Ctx): string {
	return `<li ${rowAttrs(row)}>
<p class="row__title">${cell(row, "decision", "row__badge", ctx)}<span class="row__spacer"></span>${cell(row, "at", "row__age mono", ctx, "time")}</p>
<p class="row__meta">${cell(row, "operation", "mono", ctx)}<span class="row__dot">·</span>${cell(row, "actor", "", ctx)}</p>
<p class="row__detail">${cell(row, "reason", "row__cause", ctx)}</p>
</li>`;
}

type RowRenderer = (row: RowView | null, ctx: Ctx) => string;

const RENDERERS: Record<string, RowRenderer> = {
	attention: attentionRow,
	live: liveRow,
	sessions: sessionRow,
	monitors: monitorRow,
	audit: auditRow,
};

function gapsBlock(panel: Panel): string {
	if (panel.gaps.length === 0) return "";
	const items = panel.gaps
		.map((gap) => `<li><span class="mono row__tag">${escapeHtml(gap.gap)}</span> ${escapeHtml(gap.missing)}</li>`)
		.join("");
	return `<details class="gaps">
<summary>Not shown here (${panel.gaps.length})</summary>
<p class="gaps__lead">These need a protocol verb that does not exist yet. The console shows nothing rather than guessing.</p>
<ul class="gaps__list">${items}</ul>
</details>`;
}

function panelSection(id: string, title: string, panel: Panel, ctx: Ctx, subtitle?: string): string {
	const renderer = RENDERERS[id];
	if (!renderer) throw new Error(`no row renderer for panel ${id}`);
	const count = panel.rows.length === 0 ? "" : String(panel.rows.length);
	return `<section class="panel" id="panel-${id}" data-panel="${id}" data-live="1" data-state="${escapeHtml(panel.state)}">
<header class="panel__head">
<h2 class="panel__title">${escapeHtml(title)}</h2>
<span class="panel__count" data-count>${escapeHtml(count)}</span>
</header>
${subtitle ? `<p class="panel__sub">${escapeHtml(subtitle)}</p>` : ""}
<p class="panel__note" data-note>${escapeHtml(panel.note)}</p>
<ol class="rows" data-rows>${panel.rows.map((row) => renderer(row, ctx)).join("")}</ol>
<template data-template="${id}">${renderer(null, ctx)}</template>
${gapsBlock(panel)}
</section>`;
}

function statusBar(snapshot: ConsoleSnapshot, ctx: Ctx): string {
	const row = snapshot.status;
	const chip = (name: string) => cell(row, name, "chip", ctx);
	return `<header class="bar" id="statusbar" data-tone="${escapeHtml(row.tone)}" data-state="${escapeHtml(row.state ?? "")}">
<p class="bar__chips">
${chip("alive")}${chip("sessions")}${chip("working")}${chip("attention")}${chip("delivery")}${chip("profile")}
</p>
<p class="bar__stream" id="stream-state" data-tone="ok">${escapeHtml(row.fields.stream ?? "")}</p>
</header>`;
}

function operationsPanel(operations: readonly MutationOperation[], mutationsEnabled: boolean, ctx: Ctx): string {
	const options = operations
		.map(
			(operation) =>
				`<option value="${escapeHtml(operation.id)}">${escapeHtml(operation.summary)}${operation.severity === "high" ? " (destructive)" : ""}</option>`,
		)
		.join("");

	return `<section class="panel panel--ops" id="panel-ops">
<header class="panel__head">
<h2 class="panel__title">Operations</h2>
<span class="panel__count">${operations.length}</span>
</header>
<p class="panel__sub">Read-only by default. A change needs an allowlisted operation, a named actor, and — when it destroys something — the target's own name typed back. Every attempt is audited, allowed or not.</p>
${mutationsEnabled ? "" : `<p class="panel__note" data-state="blocked">Mutations are disabled for this deployment. Every request below will be refused with 403.</p>`}
<noscript><p class="panel__note">The operation forms need JavaScript. Every panel above is complete without it.</p></noscript>

<div class="ops" id="ops" data-stage="compose" hidden>
<div class="ops__stage" data-stage-name="compose">
<label class="field"><span class="field__label">Operation</span>
<select id="ops-op" class="control">${options}</select></label>
<p class="ops__summary" id="ops-summary"></p>
<div id="ops-fields"></div>
<label class="field"><span class="field__label">Actor</span>
<input id="ops-actor" class="control" autocomplete="off" placeholder="who is doing this">
<span class="field__hint">Recorded in the audit trail. Required.</span></label>
<p class="ops__error" id="ops-error" hidden></p>
<div class="ops__buttons"><button type="button" class="button button--primary" id="ops-next">Review</button></div>
</div>

<div class="ops__stage" data-stage-name="review">
<h3 class="ops__headline" id="ops-headline"></h3>
<dl class="ops__facts" id="ops-facts"></dl>
<p class="ops__warning" id="ops-warning"></p>
<div id="ops-confirm-wrap" hidden>
<label class="field"><span class="field__label" id="ops-confirm-label">Type the name to confirm</span>
<input id="ops-confirm" class="control" autocomplete="off" spellcheck="false"></label>
</div>
<p class="ops__error" id="ops-review-error" hidden></p>
<div class="ops__buttons">
<button type="button" class="button" id="ops-cancel">Cancel</button>
<button type="button" class="button button--danger" id="ops-run">Run</button>
</div>
</div>

<div class="ops__stage" data-stage-name="receipt">
<p class="ops__receipt" id="ops-receipt"></p>
<p class="ops__audited mono" id="ops-audited"></p>
<div class="ops__buttons"><button type="button" class="button button--primary" id="ops-again">Done</button></div>
</div>
</div>

<section class="panel panel--nested" id="panel-audit" data-panel="audit" data-state="loading">
<header class="panel__head"><h3 class="panel__title">Audit trail</h3><span class="panel__count" data-count></span></header>
<p class="panel__note" data-note>Reading the audit trail…</p>
<ol class="rows" data-rows></ol>
<template data-template="audit">${auditRow(null, ctx)}</template>
</section>
</section>`;
}

const STYLE = `
:root{
  color-scheme: dark light;

  /* Type scale: 1.125 ratio off a 0.875rem base. Nothing outside this scale. */
  --text-xs: 0.75rem;
  --text-sm: 0.8125rem;
  --text-md: 0.9375rem;
  --text-lg: 1.0625rem;
  --text-xl: 1.25rem;
  --leading-tight: 1.25;
  --leading: 1.55;
  --tracking-caps: 0.08em;

  /* Spacing scale, 4px base. Nothing outside this scale. */
  --space-1: 0.25rem;
  --space-2: 0.5rem;
  --space-3: 0.75rem;
  --space-4: 1rem;
  --space-5: 1.5rem;
  --space-6: 2rem;

  --radius: 10px;
  --radius-sm: 6px;
  --border: 1px;
  --tap: 2.75rem;
  --measure: 68ch;

  /* Neutrals carry the whole layout; colour is reserved for status semantics. */
  --canvas: light-dark(#f7f7f6, #101112);
  --surface: light-dark(#ffffff, #17191b);
  --surface-sunken: light-dark(#efefed, #1d2023);
  --line: light-dark(#dcdcd8, #2b2f33);
  --line-strong: light-dark(#c3c3bd, #3a3f45);
  --ink: light-dark(#16181a, #e9eaec);
  --ink-dim: light-dark(#54585d, #a3a8ae);
  --ink-faint: light-dark(#7c8086, #7a8087);

  --ok: light-dark(#1f6f43, #5bbd88);
  --warn: light-dark(#8a5a06, #d9a441);
  --danger: light-dark(#a12a1e, #e8776a);
  --active: light-dark(#1d4f8f, #6fa8e8);

  --font: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
  --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
}

*, *::before, *::after { box-sizing: border-box; }

html { -webkit-text-size-adjust: 100%; }

body{
  margin:0;
  background: var(--canvas);
  color: var(--ink);
  font: var(--text-md)/var(--leading) var(--font);
  font-variant-numeric: tabular-nums;
  padding-bottom: var(--space-6);
}

.mono{ font-family: var(--mono); font-size: var(--text-xs); }

/* ---- status bar ---------------------------------------------------------- */

.bar{
  position: sticky; top: 0; z-index: 2;
  background: var(--surface);
  border-bottom: var(--border) solid var(--line);
  padding: var(--space-3) var(--space-4);
  display: grid; gap: var(--space-1);
}
.bar[data-state="unreachable"]{ border-bottom-color: var(--danger); }
.bar__chips{ margin:0; display:flex; flex-wrap:wrap; gap: var(--space-1) var(--space-3); align-items:baseline; }
.bar__stream{
  margin:0; font-size: var(--text-xs); color: var(--ink-faint);
  font-family: var(--mono);
}
.bar__stream[data-tone="warn"]{ color: var(--warn); }
.bar__stream[data-tone="danger"]{ color: var(--danger); }

.chip{ font-size: var(--text-sm); color: var(--ink-dim); }
.chip:empty{ display:none; }
.chip[data-tone="ok"]{ color: var(--ink); font-weight:600; }
.chip[data-tone="active"]{ color: var(--active); font-weight:600; }
.chip[data-tone="warn"]{ color: var(--warn); font-weight:600; }
.chip[data-tone="danger"]{ color: var(--danger); font-weight:600; }
.chip[data-tone="muted"]{ color: var(--ink-faint); }

/* ---- layout ------------------------------------------------------------- */

main{
  display:grid;
  gap: var(--space-4);
  padding: var(--space-4);
  align-items: start;
}
@media (min-width: 56rem){
  main{ grid-template-columns: repeat(auto-fit, minmax(24rem, 1fr)); padding: var(--space-5); }
  #panel-ops{ grid-column: 1 / -1; }
}

.panel{
  background: var(--surface);
  border: var(--border) solid var(--line);
  border-radius: var(--radius);
  padding: var(--space-4);
}
.panel--nested{ margin-top: var(--space-4); background: var(--surface-sunken); }
.panel__head{ display:flex; align-items:baseline; gap: var(--space-2); }
.panel__title{
  margin:0; font-size: var(--text-xs); font-weight:700;
  text-transform: uppercase; letter-spacing: var(--tracking-caps);
  color: var(--ink-dim);
}
.panel__count{
  margin-left:auto; font-size: var(--text-xs); font-family: var(--mono);
  color: var(--ink-faint);
}
.panel__sub{
  margin: var(--space-2) 0 0; max-width: var(--measure);
  font-size: var(--text-xs); color: var(--ink-faint); line-height: var(--leading);
}
.panel__note{
  margin: var(--space-3) 0 0; max-width: var(--measure);
  font-size: var(--text-sm); color: var(--ink-dim);
}
.panel[data-state="ready"] .panel__note{ display:none; }
.panel[data-state="error"] .panel__note,
.panel__note[data-state="blocked"]{ color: var(--danger); }
.panel[data-state="loading"] .panel__note{ color: var(--ink-faint); }
/* Frozen data must look frozen: nothing here animates, and stale rows recede. */
.panel[data-stale="1"] .rows{ opacity: .6; }
.panel[data-stale="1"] .panel__head::after{
  content: "stale"; margin-left: var(--space-2);
  font-size: var(--text-xs); letter-spacing: var(--tracking-caps); color: var(--warn);
}

/* ---- rows --------------------------------------------------------------- */

.rows{ list-style:none; margin: var(--space-3) 0 0; padding:0; display:grid; gap: var(--space-2); }
.rows:empty{ margin:0; }

.row{
  border: var(--border) solid var(--line);
  border-left: 3px solid var(--line-strong);
  border-radius: var(--radius-sm);
  padding: var(--space-3);
  background: var(--canvas);
}
.row[data-tone="ok"]{ border-left-color: var(--ok); }
.row[data-tone="warn"]{ border-left-color: var(--warn); }
.row[data-tone="danger"]{ border-left-color: var(--danger); }
.row[data-tone="active"]{ border-left-color: var(--active); }
.row[data-tone="muted"]{ border-left-color: var(--line-strong); }
.row[data-state="disabled"]{ opacity: .72; }

.row__title{
  margin:0; display:flex; flex-wrap:wrap; align-items:baseline;
  gap: var(--space-1) var(--space-2);
  font-size: var(--text-md); line-height: var(--leading-tight);
}
.row__spacer{ flex:1 1 var(--space-2); }
.row__symptom, .row__name{ font-weight:650; }
.row__age{ font-size: var(--text-xs); color: var(--ink-faint); white-space: nowrap; }
.row__detail{ margin: var(--space-2) 0 0; font-size: var(--text-sm); color: var(--ink-dim); max-width: var(--measure); }
.row__cause:empty{ display:none; }
.row__meta{
  margin: var(--space-2) 0 0; font-size: var(--text-xs); color: var(--ink-faint);
  display:flex; flex-wrap:wrap; gap: var(--space-1) var(--space-2); align-items:baseline;
}
.row__dot{ color: var(--line-strong); }
.row__state{ margin: var(--space-2) 0 0; }
.row__badge{ font-size: var(--text-xs); font-weight:650; }
.row[data-tone="ok"] .row__badge{ color: var(--ok); }
.row[data-tone="warn"] .row__badge{ color: var(--warn); }
.row[data-tone="danger"] .row__badge{ color: var(--danger); }
.row[data-tone="active"] .row__badge{ color: var(--active); }
.row__tag{
  border: var(--border) solid var(--line); border-radius: var(--radius-sm);
  padding: 0 var(--space-1);
}
.row__key{ overflow-wrap:anywhere; color: var(--ink-faint); }
/* display:block below would otherwise beat the UA's [hidden]{display:none},
   leaving an empty bar under every row that has no meter. */
.row__meter[hidden]{ display:none; }
.row__meter{
  display:block; width:100%; height: var(--space-2);
  margin: var(--space-2) 0 0;
  appearance:none; border:0; border-radius: var(--radius-sm);
  background: var(--surface-sunken);
}
.row__meter::-webkit-progress-bar{ background: var(--surface-sunken); border-radius: var(--radius-sm); }
.row__meter::-webkit-progress-value{ background: var(--active); border-radius: var(--radius-sm); }
.row__meter::-moz-progress-bar{ background: var(--active); border-radius: var(--radius-sm); }
.row[data-state="stalled"] .row__meter::-webkit-progress-value{ background: var(--warn); }
.row[data-state="stalled"] .row__meter::-moz-progress-bar{ background: var(--warn); }
.row__facts{ margin: var(--space-2) 0 0; display:grid; grid-template-columns: 5.5rem 1fr; gap: var(--space-1) var(--space-2); font-size: var(--text-sm); }
.row__facts dt{ color: var(--ink-faint); font-size: var(--text-xs); text-transform: uppercase; letter-spacing: var(--tracking-caps); }
.row__facts dd{ margin:0; color: var(--ink-dim); overflow-wrap:anywhere; }

/* ---- disclosures -------------------------------------------------------- */

.gaps, .raw{ margin-top: var(--space-3); font-size: var(--text-xs); }
.gaps > summary, .raw > summary{
  cursor:pointer; color: var(--ink-faint); min-height: var(--tap);
  display:flex; align-items:center;
}
.gaps__lead{ margin: var(--space-1) 0 var(--space-2); color: var(--ink-faint); max-width: var(--measure); }
.gaps__list{ margin:0; padding-left: var(--space-4); display:grid; gap: var(--space-1); color: var(--ink-dim); }
.raw pre{
  margin:0; padding: var(--space-3); overflow:auto; max-height: 24rem;
  background: var(--surface-sunken); border-radius: var(--radius-sm);
  font-family: var(--mono); font-size: var(--text-xs); line-height: var(--leading-tight);
}

/* ---- operations --------------------------------------------------------- */

.ops__stage{ display:none; }
.ops[data-stage="compose"] [data-stage-name="compose"],
.ops[data-stage="review"] [data-stage-name="review"],
.ops[data-stage="receipt"] [data-stage-name="receipt"]{ display:block; }

.field{ display:grid; gap: var(--space-1); margin-top: var(--space-3); max-width: 28rem; }
.field__label{ font-size: var(--text-xs); text-transform:uppercase; letter-spacing: var(--tracking-caps); color: var(--ink-dim); }
.field__hint{ font-size: var(--text-xs); color: var(--ink-faint); }
.control{
  font: inherit; font-size: var(--text-md); color: var(--ink);
  background: var(--canvas);
  border: var(--border) solid var(--line-strong); border-radius: var(--radius-sm);
  padding: 0 var(--space-3); min-height: var(--tap); width:100%;
}
.control:focus-visible{ outline: 2px solid var(--active); outline-offset: 1px; }

.ops__summary{ margin: var(--space-2) 0 0; font-size: var(--text-sm); color: var(--ink-dim); max-width: var(--measure); }
.ops__headline{ margin: 0; font-size: var(--text-lg); line-height: var(--leading-tight); }
.ops__facts{ margin: var(--space-3) 0 0; display:grid; grid-template-columns: 5.5rem 1fr; gap: var(--space-1) var(--space-2); font-size: var(--text-sm); }
.ops__facts dt{ color: var(--ink-faint); font-size: var(--text-xs); text-transform:uppercase; letter-spacing: var(--tracking-caps); }
.ops__facts dd{ margin:0; color: var(--ink); overflow-wrap:anywhere; }
.ops__warning{ margin: var(--space-4) 0 0; font-size: var(--text-sm); color: var(--danger); max-width: var(--measure); }
.ops__error{ margin: var(--space-3) 0 0; font-size: var(--text-sm); color: var(--danger); max-width: var(--measure); }
.ops__receipt{ margin:0; font-size: var(--text-lg); color: var(--ok); }
.ops__audited{ margin: var(--space-2) 0 0; color: var(--ink-faint); }
.ops__buttons{ display:flex; flex-wrap:wrap; gap: var(--space-2); margin-top: var(--space-4); }

.button{
  font: inherit; font-size: var(--text-md); min-height: var(--tap);
  padding: 0 var(--space-4); border-radius: var(--radius-sm);
  border: var(--border) solid var(--line-strong);
  background: var(--surface); color: var(--ink); cursor:pointer;
}
.button:focus-visible{ outline: 2px solid var(--active); outline-offset: 1px; }
.button--primary{ border-color: var(--active); color: var(--active); font-weight:650; }
.button--danger{ border-color: var(--danger); color: var(--danger); font-weight:650; }
.button[disabled]{ opacity:.5; cursor:not-allowed; }

footer{
  padding: 0 var(--space-4);
  font-size: var(--text-xs); color: var(--ink-faint);
  max-width: var(--measure);
}

@media (prefers-reduced-motion: reduce){ *{ transition:none !important; animation:none !important; } }
`;

const SCRIPT_BODY = String.raw`
"use strict";
(function(){
__SHARED__
  var boot = document.getElementById("bootstrap");
  if (!boot) return;
  var snapshot = JSON.parse(boot.textContent || "{}");
  var meta = JSON.parse((document.getElementById("ops-meta") || {}).textContent || "{}");
  var operations = meta.operations || [];

  /* ---- formatting ------------------------------------------------------ */
  /* The duration and clock helpers are not restated here; they are injected
     above from src/format.ts, so one algorithm serves both runtimes. */

  /* ---- DOM writes: only what differs ---------------------------------- */

  function setText(node, value){ if (node && node.textContent !== value) node.textContent = value; }
  function setAttr(node, name, value){
    if (!node) return;
    if (value === null || value === undefined || value === "") { if (node.hasAttribute(name)) node.removeAttribute(name); }
    else if (node.getAttribute(name) !== value) node.setAttribute(name, value);
  }

  function applyRow(element, row, at){
    setAttr(element, "data-key", row.key);
    setAttr(element, "data-tone", row.tone);
    setAttr(element, "data-state", row.state || null);
    var cells = element.querySelectorAll("[data-field]");
    for (var i = 0; i < cells.length; i += 1) {
      var cell = cells[i];
      var name = cell.getAttribute("data-field");
      var iso = row.ages ? row.ages[name] : undefined;
      setAttr(cell, "data-age", iso || null);
      setText(cell, iso ? relative(iso, at) : (row.fields[name] !== undefined ? row.fields[name] : ""));
      setAttr(cell, "data-tone", (row.tones && row.tones[name]) || null);
      if (cell.tagName === "TIME" && iso) setAttr(cell, "datetime", iso);
    }
    var meters = element.querySelectorAll("[data-meter]");
    for (var m = 0; m < meters.length; m += 1) {
      var bar = meters[m];
      var spec = row.meters ? row.meters[bar.getAttribute("data-meter")] : undefined;
      bar.hidden = !spec;
      if (spec) { bar.max = spec.max; bar.value = spec.value; }
    }
  }

  /* Reconcile by key: existing nodes are reused and reordered, never rebuilt. */
  function reconcile(section, rows, at){
    var list = section.querySelector("[data-rows]");
    var template = section.querySelector("[data-template]");
    if (!list || !template) return;
    var existing = new Map();
    for (var node = list.firstElementChild; node; node = node.nextElementSibling) {
      var key = node.getAttribute("data-key");
      if (key) existing.set(key, node);
    }
    var cursor = list.firstElementChild;
    for (var i = 0; i < rows.length; i += 1) {
      var row = rows[i];
      var element = existing.get(row.key);
      if (element) existing.delete(row.key);
      else element = template.content.firstElementChild.cloneNode(true);
      applyRow(element, row, at);
      if (element !== cursor) list.insertBefore(element, cursor);
      else cursor = cursor.nextElementSibling;
    }
    existing.forEach(function(node){ node.remove(); });
  }

  function applyPanel(name, panel, at){
    var section = document.querySelector('[data-panel="' + name + '"]');
    if (!section || !panel) return;
    setAttr(section, "data-state", panel.state);
    setText(section.querySelector("[data-note]"), panel.note);
    setText(section.querySelector("[data-count]"), panel.rows.length === 0 ? "" : String(panel.rows.length));
    reconcile(section, panel.rows, at);
  }

  function applySnapshot(next){
    snapshot = next;
    var at = new Date(next.at).getTime();
    var bar = document.getElementById("statusbar");
    setAttr(bar, "data-tone", next.status.tone);
    setAttr(bar, "data-state", next.status.state || null);
    applyRow(bar, next.status, at);
    applyPanel("attention", next.attention, at);
    applyPanel("live", next.live, at);
    applyPanel("sessions", next.sessions, at);
    applyPanel("monitors", next.monitors, at);
    var raw = document.getElementById("raw-json");
    if (raw) setText(raw, JSON.stringify(next.raw, null, 2));
    lastData = Date.now();
    reportStream();
    refreshMonitorRefs();
  }

  /* ---- stream health: never animate what is not arriving ---------------- */

  var STALE_MS = 90000;
  var lastData = Date.now();
  var streamOpen = false;
  var streamEverOpen = false;
  var stopping = false;

  function streamMessage(){
    var origin = snapshot.at ? formatClockSeconds(new Date(snapshot.at)) : "??:??:??";
    if (stopping) return { tone: "danger", text: "\u26a0 gateway is stopping \u2014 showing data from " + origin };
    if (snapshot.gateway && snapshot.gateway.reachable === false) {
      return { tone: "danger", text: "\u26a0 gateway unreachable \u2014 the daemon may be down. Data from " + origin };
    }
    if (!streamOpen) {
      return {
        tone: "danger",
        text: (streamEverOpen ? "\u26a0 console stream lost \u2014 reconnecting\u2026 " : "\u26a0 console stream not open \u2014 ") + "showing data from " + origin
      };
    }
    if (Date.now() - lastData > STALE_MS) {
      return { tone: "warn", text: "\u26a0 stream quiet \u2014 showing data from " + origin };
    }
    return { tone: "ok", text: "stream \u2713 data " + origin };
  }

  function reportStream(){
    var state = streamMessage();
    var node = document.getElementById("stream-state");
    setAttr(node, "data-tone", state.tone);
    setText(node, state.text);
    var frozen = state.tone !== "ok";
    document.querySelectorAll('.panel[data-live="1"]').forEach(function(section){
      if (frozen && section.getAttribute("data-state") === "ready") setAttr(section, "data-stale", "1");
      else setAttr(section, "data-stale", null);
    });
    return frozen;
  }

  /* Ages tick only while data is arriving. A frozen console must look frozen. */
  setInterval(function(){
    if (reportStream()) return;
    var now = Date.now();
    document.querySelectorAll("[data-age]").forEach(function(node){
      setText(node, relative(node.getAttribute("data-age"), now));
    });
  }, 1000);

  function openStream(){
    var source = new EventSource("/api/stream");
    source.addEventListener("open", function(){ streamOpen = true; streamEverOpen = true; stopping = false; reportStream(); });
    source.addEventListener("error", function(){ streamOpen = false; reportStream(); });
    source.addEventListener("snapshot", function(event){ streamOpen = true; applySnapshot(JSON.parse(event.data)); });
    source.addEventListener("gateway.stopping", function(){ stopping = true; reportStream(); });
    source.addEventListener("monitor.event", function(){ lastData = Date.now(); });
  }

  /* Correctness backstop: a silently wedged stream self-heals within a minute. */
  setInterval(function(){
    if (Date.now() - lastData < 55000) return;
    fetch("/api/snapshot", { headers: { accept: "application/json" } })
      .then(function(r){ return r.json(); })
      .then(function(body){ if (body && body.ok) applySnapshot(body.result); })
      .catch(function(){ reportStream(); });
  }, 30000);

  /* ---- audit tail ------------------------------------------------------- */

  function loadAudit(){
    return fetch("/api/audit?limit=20", { headers: { accept: "application/json" } })
      .then(function(r){ return r.json(); })
      .then(function(body){
        if (!body || !body.ok) return;
        var at = Date.now();
        var rows = body.result.entries.map(function(entry){
          return {
            key: entry.at + ":" + entry.operationId + ":" + entry.decision,
            tone: entry.decision === "allowed" ? "ok" : "warn",
            fields: {
              decision: entry.decision === "allowed" ? "\u2713 allowed" : "\u2715 rejected",
              operation: entry.operationId,
              actor: "actor=" + entry.actor,
              reason: entry.reason || ""
            },
            ages: { at: entry.at }
          };
        });
        applyPanel("audit", { state: rows.length === 0 ? "empty" : "ready", note: "No mutation has been attempted.", rows: rows }, at);
      })
      .catch(function(){});
  }

  /* ---- operations: compose -> consequence -> receipt -------------------- */

  var ops = document.getElementById("ops");
  if (!ops) return;
  ops.hidden = false;

  var select = document.getElementById("ops-op");
  var fieldsHost = document.getElementById("ops-fields");
  var inputs = new Map();

  function current(){
    for (var i = 0; i < operations.length; i += 1) if (operations[i].id === select.value) return operations[i];
    return null;
  }

  function monitorChoices(){
    var rows = (snapshot.monitors && snapshot.monitors.rows) || [];
    return rows.map(function(row){ return { value: row.key, label: row.fields.name }; });
  }

  function buildFields(){
    var operation = current();
    inputs.clear();
    fieldsHost.replaceChildren();
    setText(document.getElementById("ops-summary"), operation ? operation.summary + " \u00b7 severity " + operation.severity : "");
    if (!operation) return;
    operation.fields.forEach(function(field){
      var label = document.createElement("label");
      label.className = "field";
      var caption = document.createElement("span");
      caption.className = "field__label";
      caption.textContent = field.label + (field.required ? "" : " (optional)");
      label.appendChild(caption);
      var control;
      if (field.kind === "monitor-ref") {
        control = document.createElement("select");
        monitorChoices().forEach(function(choice){
          var option = document.createElement("option");
          option.value = choice.value;
          option.textContent = choice.label;
          control.appendChild(option);
        });
        if (control.options.length === 0) {
          var none = document.createElement("option");
          none.value = "";
          none.textContent = "no monitor is registered";
          control.appendChild(none);
        }
      } else {
        control = document.createElement("input");
        control.type = "text";
        control.autocomplete = "off";
        if (field.placeholder) control.placeholder = field.placeholder;
      }
      control.className = "control";
      label.appendChild(control);
      if (field.hint) {
        var hint = document.createElement("span");
        hint.className = "field__hint";
        hint.textContent = field.hint;
        label.appendChild(hint);
      }
      inputs.set(field.name, control);
      fieldsHost.appendChild(label);
    });
  }

  function refreshMonitorRefs(){
    var operation = current();
    if (!operation) return;
    operation.fields.forEach(function(field){
      if (field.kind !== "monitor-ref") return;
      var control = inputs.get(field.name);
      if (!control || control.tagName !== "SELECT") return;
      var choices = monitorChoices();
      var signature = choices.map(function(c){ return c.value + "\u0000" + c.label; }).join("\u0001");
      if (control.dataset.signature === signature) return;
      var kept = control.value;
      control.replaceChildren();
      choices.forEach(function(choice){
        var option = document.createElement("option");
        option.value = choice.value;
        option.textContent = choice.label;
        control.appendChild(option);
      });
      control.dataset.signature = signature;
      if (kept) control.value = kept;
    });
  }

  function assign(target, path, value){
    var segments = path.split(".");
    var cursor = target;
    for (var i = 0; i < segments.length - 1; i += 1) {
      var segment = segments[i];
      if (typeof cursor[segment] !== "object" || cursor[segment] === null) cursor[segment] = {};
      cursor = cursor[segment];
    }
    cursor[segments[segments.length - 1]] = value;
  }

  function collect(operation){
    var params = JSON.parse(JSON.stringify(operation.paramsTemplate || {}));
    var missing = [];
    operation.fields.forEach(function(field){
      var control = inputs.get(field.name);
      var raw = control ? String(control.value || "").trim() : "";
      if (raw.length === 0) {
        if (field.required) missing.push(field.label);
        return;
      }
      var value = raw;
      if (field.list) value = raw.split(",").map(function(part){ return part.trim(); }).filter(Boolean);
      if (field.kind === "json") {
        try { value = JSON.parse(raw); } catch (error) { missing.push(field.label + " (not valid json)"); return; }
      }
      assign(params, field.path || field.name, value);
    });
    return { params: params, missing: missing };
  }

  function stage(name){ ops.setAttribute("data-stage", name); }
  function fail(id, text){
    var node = document.getElementById(id);
    node.hidden = !text;
    setText(node, text || "");
  }

  var pending = null;

  document.getElementById("ops-next").addEventListener("click", function(){
    var operation = current();
    if (!operation) return;
    var actor = String(document.getElementById("ops-actor").value || "").trim();
    if (!actor) { fail("ops-error", "An actor is required. The audit trail records who did this."); return; }
    var collected = collect(operation);
    if (collected.missing.length > 0) { fail("ops-error", "Fill in: " + collected.missing.join(", ")); return; }
    fail("ops-error", "");
    pending = { operation: operation, actor: actor, params: collected.params };

    var confirmWrap = document.getElementById("ops-confirm-wrap");
    var confirmInput = document.getElementById("ops-confirm");
    var runButton = document.getElementById("ops-run");
    confirmInput.value = "";
    fail("ops-review-error", "");

    var needsTarget = operation.confirmToken === "target-name";
    confirmWrap.hidden = !needsTarget;
    runButton.className = "button " + (operation.severity === "high" ? "button--danger" : "button--primary");

    if (!needsTarget) {
      setText(document.getElementById("ops-headline"), operation.summary);
      var facts = document.getElementById("ops-facts");
      facts.replaceChildren();
      operation.fields.forEach(function(field){
        var value = pending.params;
        (field.path || field.name).split(".").forEach(function(segment){ value = value === undefined || value === null ? undefined : value[segment]; });
        if (value === undefined) return;
        var dt = document.createElement("dt");
        dt.textContent = field.label;
        var dd = document.createElement("dd");
        dd.textContent = Array.isArray(value) ? value.join(", ") : String(value);
        facts.appendChild(dt);
        facts.appendChild(dd);
      });
      setText(document.getElementById("ops-warning"), operation.consequence || "");
      setText(runButton, operation.summary);
      stage("review");
      return;
    }

    /* The consequence is computed from a live read, never restated from the form. */
    var ref = operation.fields.filter(function(f){ return f.kind === "monitor-ref"; })[0];
    var monitorId = ref ? String(inputs.get(ref.name).value || "") : "";
    if (!monitorId) { fail("ops-error", "Pick a monitor first."); return; }
    setText(document.getElementById("ops-headline"), "Reading the target\u2026");
    document.getElementById("ops-facts").replaceChildren();
    setText(document.getElementById("ops-warning"), "");
    setText(runButton, "Run");
    runButton.disabled = true;
    stage("review");

    fetch("/api/consequence?operationId=" + encodeURIComponent(operation.id) + "&monitorId=" + encodeURIComponent(monitorId), { headers: { accept: "application/json" } })
      .then(function(r){ return r.json(); })
      .then(function(body){
        if (!body || !body.ok) throw new Error(body && body.error ? body.error : "the gateway did not answer");
        var consequence = body.result;
        pending.targetName = consequence.targetName;
        setText(document.getElementById("ops-headline"), consequence.headline);
        var facts = document.getElementById("ops-facts");
        facts.replaceChildren();
        consequence.facts.forEach(function(fact){
          var dt = document.createElement("dt");
          dt.textContent = fact.label;
          var dd = document.createElement("dd");
          dd.textContent = fact.value;
          facts.appendChild(dt);
          facts.appendChild(dd);
        });
        setText(document.getElementById("ops-warning"), consequence.warning);
        setText(document.getElementById("ops-confirm-label"), "Type " + consequence.targetName + " to confirm");
        setText(runButton, consequence.actionLabel);
        runButton.disabled = false;
      })
      .catch(function(error){
        setText(document.getElementById("ops-headline"), "The target could not be read");
        fail("ops-review-error", String(error.message || error));
        runButton.disabled = true;
      });
  });

  document.getElementById("ops-cancel").addEventListener("click", function(){ stage("compose"); });
  document.getElementById("ops-again").addEventListener("click", function(){ stage("compose"); });

  document.getElementById("ops-run").addEventListener("click", function(){
    if (!pending) return;
    var body = {
      operationId: pending.operation.id,
      actor: pending.actor,
      /* The machine echo the gate has always required; the human ceremony is the
         target name above, which the server checks against a live read. */
      confirm: pending.operation.id,
      params: pending.params
    };
    if (pending.operation.confirmToken === "target-name") {
      body.targetConfirm = String(document.getElementById("ops-confirm").value || "");
    }
    var runButton = document.getElementById("ops-run");
    runButton.disabled = true;
    fetch("/api/mutations", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })
      .then(function(response){ return response.json().then(function(payload){ return { status: response.status, payload: payload }; }); })
      .then(function(result){
        runButton.disabled = false;
        if (!result.payload.ok) { fail("ops-review-error", result.status + " \u2014 " + result.payload.error); return loadAudit(); }
        setText(document.getElementById("ops-receipt"), result.payload.receipt.headline);
        setText(document.getElementById("ops-audited"), result.payload.receipt.audited);
        stage("receipt");
        return loadAudit();
      })
      .catch(function(error){
        runButton.disabled = false;
        fail("ops-review-error", String(error.message || error));
      });
  });

  select.addEventListener("change", function(){ fail("ops-error", ""); buildFields(); });
  buildFields();
  loadAudit();
  openStream();
  reportStream();
})();
`;

export function renderIndex(
	snapshot: ConsoleSnapshot,
	operations: readonly MutationOperation[],
	mutationsEnabled = true,
): string {
	const ctx: Ctx = { at: new Date(snapshot.at) };

	return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark light">
<meta name="robots" content="noindex, nofollow">
<title>gajae-way console</title>
<style>${STYLE}</style>
</head>
<body>
${statusBar(snapshot, ctx)}
<main>
${panelSection("attention", "Attention", snapshot.attention, ctx, "What needs a human. Ordered by severity, computed on the server so there is only ever one threshold.")}
${panelSection("live", "Live work", snapshot.live, ctx, `Turns in flight, against the gateway's ${formatDuration(TURN_CEILING_MS)} ceiling. A row with no progress for ${formatDuration(TURN_STALL_MS)} stops claiming to be alive.`)}
<section class="panel" id="panel-conversation" data-state="${escapeHtml(snapshot.conversation.state)}">
<header class="panel__head"><h2 class="panel__title">Conversation</h2><span class="panel__count"></span></header>
<p class="panel__sub">What it said, whether it stayed silent, and whether the reply landed.</p>
<p class="panel__note" data-state="blocked">${escapeHtml(snapshot.conversation.note)}</p>
${gapsBlock(snapshot.conversation)}
</section>
${panelSection("sessions", "Sessions", snapshot.sessions, ctx, "One conversational origin, one isolated session.")}
${panelSection("monitors", "Monitors", snapshot.monitors, ctx, "What fires on its own, when it fires next, and how its last event ended.")}
${operationsPanel(operations, mutationsEnabled, ctx)}
</main>
<footer>
<details class="raw">
<summary>Raw protocol results</summary>
<pre id="raw-json">${escapeHtml(JSON.stringify(snapshot.raw, null, 2))}</pre>
</details>
<p>Loopback only, read-only by default. <code>chat.send</code> is not on the mutation allowlist and cannot be reached from this console.</p>
</footer>
<script type="application/json" id="bootstrap">${JSON.stringify(snapshot).replaceAll("<", "\\u003c")}</script>
<script type="application/json" id="ops-meta">${JSON.stringify({ operations, mutationsEnabled }).replaceAll("<", "\\u003c")}</script>
<script>${SCRIPT_BODY.replace("__SHARED__", SHARED_SOURCE)}</script>
</body>
</html>`;
}
