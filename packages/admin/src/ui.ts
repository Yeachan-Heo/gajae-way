/**
 * Single-file admin web UI.
 *
 * Deliberately dependency-free: no bundler, no framework, no build step, so the
 * gateway binary can serve it directly. Read panels refresh on a timer; the
 * mutation panel requires typing the operation id, mirroring the server-side
 * gate rather than trusting the client.
 */

import type { MutationOperation } from "./gate";

function escapeHtml(value: string): string {
	return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
}

export function renderIndex(operations: readonly MutationOperation[]): string {
	const options = operations
		.map(
			(operation) =>
				`<option value="${escapeHtml(operation.id)}">${escapeHtml(operation.id)} - ${escapeHtml(operation.summary)}</option>`,
		)
		.join("");

	return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>gajae-way admin</title>
<style>
:root { color-scheme: dark light; --gap: 12px; }
body { font: 14px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; margin: 0; padding: var(--gap); }
h1 { font-size: 16px; margin: 0 0 var(--gap); }
section { border: 1px solid currentColor; border-radius: 6px; padding: var(--gap); margin-bottom: var(--gap); }
h2 { font-size: 13px; margin: 0 0 8px; text-transform: uppercase; letter-spacing: .08em; }
pre { margin: 0; max-height: 320px; overflow: auto; white-space: pre-wrap; word-break: break-word; }
.danger { border-color: #c0392b; }
label { display: block; margin-bottom: 6px; }
input, select, textarea, button { font: inherit; padding: 4px 6px; }
.row { display: flex; gap: 8px; flex-wrap: wrap; align-items: end; }
.note { opacity: .75; }
</style>
</head>
<body>
<h1>gajae-way admin <span class="note" id="clock"></span></h1>

<section><h2>Gateway status</h2><pre id="status">loading…</pre></section>
<section><h2>Sessions</h2><pre id="sessions">loading…</pre></section>
<section><h2>Monitors</h2><pre id="monitors">loading…</pre></section>

<section class="danger">
<h2>Mutations (double gate)</h2>
<p class="note">Read-only by default. A mutation needs an allowlisted operation, an actor, and the operation id typed into <code>confirm</code>. Every attempt is audited server-side.</p>
<div class="row">
<label>operation<br><select id="op">${options}</select></label>
<label>actor<br><input id="actor" placeholder="who is doing this"></label>
<label>confirm<br><input id="confirm" placeholder="type the operation id"></label>
<label>params (json)<br><input id="params" value="{}"></label>
<button id="run">run</button>
</div>
<pre id="result"></pre>
</section>

<script>
const show = (id, value) => { document.getElementById(id).textContent = value; };

async function load(path, id) {
  try {
    const response = await fetch(path);
    const body = await response.json();
    show(id, JSON.stringify(body.result ?? body, null, 2));
  } catch (error) {
    show(id, "unreachable: " + error);
  }
}

async function refresh() {
  document.getElementById("clock").textContent = new Date().toISOString();
  await Promise.all([
    load("/api/status", "status"),
    load("/api/sessions", "sessions"),
    load("/api/monitors", "monitors"),
  ]);
}

document.getElementById("run").addEventListener("click", async () => {
  let params;
  try {
    params = JSON.parse(document.getElementById("params").value || "{}");
  } catch (error) {
    show("result", "params is not valid json: " + error);
    return;
  }
  const response = await fetch("/api/mutations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      operationId: document.getElementById("op").value,
      actor: document.getElementById("actor").value,
      confirm: document.getElementById("confirm").value,
      params,
    }),
  });
  show("result", response.status + " " + JSON.stringify(await response.json(), null, 2));
  await refresh();
});

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>`;
}
