/**
 * System notice localization. Gateway-authored messages are detected based on
 * the conversation's most recent human message language and rendered in the
 * appropriate locale, falling back to the configured default.
 */

/** Supported locale codes. */
export type SupportedLocale = "en" | "ko";

/** Detect language from text using Hangul ratio heuristic. */
export function detectLanguage(text: string): SupportedLocale | undefined {
	if (!text || text.trim().length === 0) return undefined;

	// Count Hangul characters (Korean script) and ASCII/Latin characters (English)
	// Hangul syllables: AC00–D7A3 (19,968 characters)
	// Hangul jamo: 1100–11FF (pre-composed), 3130–318F (compatibility)
	let hangulCount = 0;
	let latinCount = 0;
	let totalCharCount = 0;

	for (const char of text) {
		const code = char.charCodeAt(0);
		totalCharCount++;

		// Hangul syllables range
		if (code >= 0xac00 && code <= 0xd7a3) {
			hangulCount++;
		}
		// Hangul jamo ranges
		else if ((code >= 0x1100 && code <= 0x11ff) || (code >= 0x3130 && code <= 0x318f)) {
			hangulCount++;
		}
		// ASCII/Latin letters (A-Z, a-z)
		else if ((code >= 0x41 && code <= 0x5a) || (code >= 0x61 && code <= 0x7a)) {
			latinCount++;
		}
	}

	const hangulRatio = totalCharCount > 0 ? hangulCount / totalCharCount : 0;
	const latinRatio = totalCharCount > 0 ? latinCount / totalCharCount : 0;

	// If more than 10% of characters are Hangul, treat as Korean
	if (hangulRatio > 0.1) {
		return "ko";
	}

	// If more than 20% of characters are Latin/ASCII letters, treat as English
	// (Higher threshold to avoid false positives from mixed-language text)
	if (latinRatio > 0.2) {
		return "en";
	}

	return undefined;
}

/** Message catalog keyed by message ID. */
export interface MessageCatalog {
	readonly [key: string]: string;
}

/** English message catalog. */
export const EN_CATALOG: MessageCatalog = {
	"turn.held.unresolved":
		"[turn held] I could not confirm whether my answer to an earlier message finished, so I will not run it again. Newer messages continue in a fresh session; if that earlier answer still arrives, it will be posted here.",
	"turn.held.capacity": "[turn held] session hosts at capacity ({live}/{max} busy). Starting as soon as one frees.",
	"turn.held.broker_wedged":
		"[turn held] sdk unavailable / broker wedged since {since} ({why}; {failures}){exitDetails}. Retrying in the background.",
	"turn.held.broker_discovery_absent":
		"[turn held] sdk unavailable / broker discovery absent ({failures}){exitDetails}. Retrying in the background.",
	"turn.held.sdk_unavailable":
		"[turn held] sdk unavailable (broker daemon {verdict}; {failures}){exitDetails}. Retrying in the background.",
	"turn.completed.text_unrecoverable":
		"[turn completed] The session finished but its result text was not recoverable. Send the request again if you need the answer.",
	"gateway.notice.agent_run":
		"[Gateway notice] The runtime started one or more runs on its own (background job / subagent results) since your last reply that reached this conversation. Text you wrote in those runs was NOT delivered, except replies you gave to user messages. If anything the user should see is still undelivered, send it now in full; otherwise reply [SILENT].",
	"watch.registered":
		"Watching {target} for its next human message until {expiresAt}. The reason is stored with this watch but not copied to other conversations.",
	"watch.failure": "Watch for {target} failed: {reason}. No watch was registered.",
	"wake.failure": "Wake for {target} failed: {reason}. Nothing was sent there.",
	"handoff.pointer":
		"Moved to {targetLabel}: this work belongs to that conversation, and the follow-up continues there.",
	"handoff.failure":
		"Handoff to {target} failed: {reason}. Nothing was sent there; the work stays in this conversation.",
};

/** Korean message catalog. */
export const KO_CATALOG: MessageCatalog = {
	"turn.held.unresolved":
		"[턴 보류됨] 이전 메시지에 대한 답변이 완료되었는지 확인할 수 없어서 다시 실행하지 않았습니다. 새로운 메시지는 새 세션에서 계속됩니다. 이전 답변이 나중에 도착하면 여기에 게시될 것입니다.",
	"turn.held.capacity": "[턴 보류됨] 세션 호스트 용량 초과 ({live}/{max} 바쁨). 하나가 해제되는 대로 시작합니다.",
	"turn.held.broker_wedged":
		"[턴 보류됨] SDK 사용 불가 / 브로커 멈춤 {since} 이후 ({why}; {failures}){exitDetails}. 백그라운드에서 재시도 중입니다.",
	"turn.held.broker_discovery_absent":
		"[턴 보류됨] SDK 사용 불가 / 브로커 검색 없음 ({failures}){exitDetails}. 백그라운드에서 재시도 중입니다.",
	"turn.held.sdk_unavailable":
		"[턴 보류됨] SDK 사용 불가 (브로커 데몬 {verdict}; {failures}){exitDetails}. 백그라운드에서 재시도 중입니다.",
	"turn.completed.text_unrecoverable":
		"[턴 완료됨] 세션이 완료되었지만 결과 텍스트를 복구할 수 없습니다. 필요하면 요청을 다시 보내십시오.",
	"gateway.notice.agent_run":
		"[게이트웨이 알림] 마지막 회신 이후 런타임이 자체적으로 하나 이상의 실행을 시작했습니다(백그라운드 작업/서브에이전트 결과). 이러한 실행에서 작성한 텍스트는 사용자 메시지에 대한 회신을 제외하고는 전달되지 않았습니다. 사용자가 여전히 볼 수 있어야 하는 것이 아직 전달되지 않았다면 지금 전체 내용으로 보내십시오. 그렇지 않으면 [SILENT]로 회신하십시오.",
	"watch.registered":
		"{target}를 모니터링하고 {expiresAt}까지 다음 사람 메시지를 기다립니다. 이유는 이 감시와 함께 저장되지만 다른 대화에 복사되지 않습니다.",
	"watch.failure": "{target}에 대한 감시 실패: {reason}. 감시가 등록되지 않았습니다.",
	"wake.failure": "{target}에 대한 웨이크 실패: {reason}. 아무것도 그곳으로 전송되지 않았습니다.",
	"handoff.pointer": "{targetLabel}로 이동됨: 이 작업은 해당 대화에 속하고 후속 작업은 그곳에서 계속됩니다.",
	"handoff.failure":
		"{target}로의 핸드오프 실패: {reason}. 아무것도 그곳으로 전송되지 않았고 작업은 이 대화에서 유지됩니다.",
};

/** Get a catalog for the given locale, with English as fallback. */
function getCatalog(locale: SupportedLocale | undefined): MessageCatalog {
	if (locale === "ko") return KO_CATALOG;
	return EN_CATALOG;
}

/** Render a message from the catalog with variable substitution. */
function renderMessage(catalog: MessageCatalog, messageId: string, vars: Record<string, string> = {}): string {
	const template = catalog[messageId] ?? EN_CATALOG[messageId] ?? messageId;

	let result = template;
	for (const [key, value] of Object.entries(vars)) {
		result = result.replace(new RegExp(`\\{${key}\\}`, "g"), value);
	}
	return result;
}

/**
 * Localization resolver that selects locale based on detected language from
 * the conversation's most recent message, falling back to configured default.
 */
export class LocalizationResolver {
	#defaultLocale: SupportedLocale;

	constructor(defaultLocale: SupportedLocale = "en") {
		this.#defaultLocale = defaultLocale;
	}

	/** Update the default locale (called when config reloads). */
	setDefaultLocale(locale: SupportedLocale): void {
		this.#defaultLocale = locale;
	}

	/** Get the effective locale for a message. */
	private getEffectiveLocale(lastHumanMessage: string | undefined): SupportedLocale {
		if (!lastHumanMessage) return this.#defaultLocale;

		const detected = detectLanguage(lastHumanMessage);
		return detected ?? this.#defaultLocale;
	}

	/** Render a message with optional language detection. */
	render(messageId: string, vars?: Record<string, string>, lastHumanMessage?: string): string {
		const locale = this.getEffectiveLocale(lastHumanMessage);
		const catalog = getCatalog(locale);
		return renderMessage(catalog, messageId, vars);
	}
}
