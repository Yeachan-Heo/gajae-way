import * as crypto from "node:crypto";
import type { CanonicalValue, ProjectionDiff, WayProfile } from "../profile";
import { diffProfileProjections, profileProjectionCanonical } from "../profile";
import type { GatewayStateStore } from "./state";

export class ProfileApprovalError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason) {
		super(message);
		this.name = "ProfileApprovalError";
		this.reason = reason;
	}
}

export interface ProfileApprovalPreview {
	readonly previousDigest?: string;
	readonly nextDigest: string;
	readonly changes: readonly ProjectionDiff[];
}

export interface ProfileApprovalResult extends ProfileApprovalPreview {
	readonly receiptId: string;
	readonly approvedAt: number;
	readonly cursor?: string;
}

function nextProjection(profile: WayProfile): CanonicalValue {
	return JSON.parse(profileProjectionCanonical(profile.projection)) as CanonicalValue;
}

/** Creates the secret-free projection diff that an operator must confirm. */
export function previewProfileApproval(state: GatewayStateStore, profile: WayProfile): ProfileApprovalPreview {
	const durable = state.read();
	return {
		previousDigest: durable.profileDigest,
		nextDigest: profile.digest.sha256,
		changes: diffProfileProjections(durable.profileProjection, nextProjection(profile)),
	};
}

/** Atomically records the approved digest/projection and its journal receipt. */
export function approveProfile(
	state: GatewayStateStore,
	profile: WayProfile,
	confirm: boolean,
	options: { readonly now?: () => number; readonly receiptId?: () => string } = {},
): ProfileApprovalResult {
	const preview = previewProfileApproval(state, profile);
	if (!confirm) {
		throw new ProfileApprovalError(
			"confirmation_required",
			`Profile projection changes require --confirm: ${JSON.stringify(preview.changes)}`,
		);
	}
	const approvedAt = (options.now ?? Date.now)();
	const receiptId = (options.receiptId ?? (() => `profile-${crypto.randomUUID()}`))();
	const committed = state.approveProfile(profile, receiptId, approvedAt);
	return { ...preview, receiptId, approvedAt, cursor: committed.cursor };
}
