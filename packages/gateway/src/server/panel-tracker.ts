import crypto from "node:crypto";
import type { OriginRef } from "@gajae-gateway/protocol";

/**
 * Tracks pending interactive panels waiting for user responses. Maps panelIds
 * to question metadata so responses can be routed back to gjc sessions via
 * the coordinator answer API.
 */
export interface PendingPanel {
	/** Unique panel identifier */
	readonly panelId: string;
	/** The gjc question_id this panel is bound to */
	readonly questionId: string;
	/** The turn_id for coordinator context */
	readonly turnId: string;
	/** Session ID owning this panel */
	readonly sessionId: string;
	/** Origin the panel is being asked in */
	readonly origin: OriginRef;
	/** User who initiated the question (authorized responders) */
	readonly authorId: string;
	/** Panel expiration timestamp */
	readonly expiresAt: Date;
	/** Panel creation timestamp */
	readonly createdAt: Date;
	/** Answer binding from gjc coordinator for idempotent resolution */
	readonly answerBinding: string;
}

/**
 * Manages pending interactive panels. Tracks which panels are waiting for
 * responses, validates responders, handles expiry, and maps responses back
 * to gjc coordinator answers.
 */
export class PanelTracker {
	private panels = new Map<string, PendingPanel>();
	private panelsBySessionId = new Map<string, Set<string>>();
	private expiryTimer: NodeJS.Timeout | null = null;

	/**
	 * Register a new pending panel.
	 */
	registerPanel(input: Omit<PendingPanel, "createdAt">): string {
		const panel: PendingPanel = {
			...input,
			createdAt: new Date(),
		};

		this.panels.set(panel.panelId, panel);

		if (!this.panelsBySessionId.has(panel.sessionId)) {
			this.panelsBySessionId.set(panel.sessionId, new Set());
		}
		this.panelsBySessionId.get(panel.sessionId)!.add(panel.panelId);

		// Reschedule expiry check
		this.scheduleExpiryCheck();

		return panel.panelId;
	}

	/**
	 * Look up a pending panel by ID.
	 */
	getPanel(panelId: string): PendingPanel | undefined {
		const panel = this.panels.get(panelId);
		if (!panel) return undefined;

		// Check expiry
		if (Date.now() > panel.expiresAt.getTime()) {
			this.panels.delete(panelId);
			return undefined;
		}

		return panel;
	}

	/**
	 * Validate that a responder is authorized for this panel.
	 */
	isAuthorizedResponder(panel: PendingPanel, responderId: string): boolean {
		// Responder must match the original author
		return responderId === panel.authorId;
	}

	/**
	 * Mark a panel as resolved (answered or expired).
	 */
	resolvePanel(panelId: string): PendingPanel | undefined {
		const panel = this.panels.get(panelId);
		if (!panel) return undefined;

		this.panels.delete(panelId);

		const sessionPanels = this.panelsBySessionId.get(panel.sessionId);
		if (sessionPanels) {
			sessionPanels.delete(panelId);
			if (sessionPanels.size === 0) {
				this.panelsBySessionId.delete(panel.sessionId);
			}
		}

		return panel;
	}

	/**
	 * Get all pending panels for a session.
	 */
	getPanelsBySession(sessionId: string): PendingPanel[] {
		const panelIds = this.panelsBySessionId.get(sessionId);
		if (!panelIds) return [];

		const panels: PendingPanel[] = [];
		const now = Date.now();

		for (const panelId of panelIds) {
			const panel = this.panels.get(panelId);
			if (panel && panel.expiresAt.getTime() > now) {
				panels.push(panel);
			}
		}

		return panels;
	}

	/**
	 * Clean up expired panels and return them for expiry processing.
	 */
	reapExpiredPanels(): PendingPanel[] {
		const now = Date.now();
		const expired: PendingPanel[] = [];

		for (const [panelId, panel] of this.panels) {
			if (panel.expiresAt.getTime() <= now) {
				expired.push(panel);
				this.resolvePanel(panelId);
			}
		}

		return expired;
	}

	/**
	 * Schedule a check for expired panels. Called whenever a new panel is added.
	 */
	private scheduleExpiryCheck(): void {
		if (this.expiryTimer !== null) return;

		// Find the nearest expiry time
		let nearestExpiry = Infinity;
		for (const panel of this.panels.values()) {
			const expiryMs = panel.expiresAt.getTime();
			if (expiryMs < nearestExpiry) {
				nearestExpiry = expiryMs;
			}
		}

		if (nearestExpiry === Infinity) return;

		const delayMs = Math.max(0, nearestExpiry - Date.now() + 100); // 100ms buffer
		this.expiryTimer = setTimeout(() => {
			this.expiryTimer = null;
			this.reapExpiredPanels();
			this.scheduleExpiryCheck();
		}, delayMs);
	}

	/**
	 * Clear all pending panels and cancel timers.
	 */
	clear(): void {
		this.panels.clear();
		this.panelsBySessionId.clear();
		if (this.expiryTimer !== null) {
			clearTimeout(this.expiryTimer);
			this.expiryTimer = null;
		}
	}
}
