/**
 * Live finding (jip-gajae, 2026-09-02): a message with no attachment was
 * answered by reading an unrelated, months-old image from the workspace and
 * describing it as if the user had just sent it. Attachments are only what
 * the current message's body lists; the model must not go looking on disk.
 *
 * Live finding (sandoh, 2026-10-13): a screenshot posted in one Slack thread
 * was answered with another thread's photo. Both were `image.png`; the other
 * thread's failed turn had already downloaded its file into the shared
 * workspace, and the new turn opened that file instead of fetching its own.
 * A file on disk is the current attachment only when its id matches.
 */
export const ATTACHMENT_SCOPE_NOTICE =
	'Attachments: a message includes an attachment only when its body lists one (e.g. "[image · name.png · id F0123ABCD · 12 KB · url]", "voice message", a URL). If the current message lists none, it has none — do not open, search for, or describe files on disk as if the user had attached them, and never treat an older file as part of the current message. When it lists one, fetch exactly that attachment by the id or URL in the body. A file already on disk (for example an earlier download in an attachments folder) belongs to the message and conversation it was downloaded for: the same or a generic file name such as image.png does not make it this message\'s attachment unless its id matches the id listed here.';

/**
 * Adapters render an attachment as `[kind · name · (id ·) size · url]` so the live turn
 * can fetch it. Replayed history must not: a fresh session's 24h recap carried
 * those CDN urls verbatim, and the model opened days-old screenshots and talked
 * about them as the current message (live: jip-gajae, 2026-09-01, the
 * "look-at-live-screenshot" incident). History keeps the human-readable part
 * and loses the handle.
 */
const HISTORICAL_ATTACHMENT = /\[((?:image|video|audio|file|voice message)(?: · [^\]\n]*?)?) · https?:\/\/[^\s\]]+\]/g;

export function redactHistoricalAttachments(body: string): string {
	return body.replaceAll(HISTORICAL_ATTACHMENT, "[$1 · past attachment; not part of this message, do not fetch]");
}
