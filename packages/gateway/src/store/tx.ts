import type { OwnerGroup } from "./ownership";

const writeTxBrand: unique symbol = Symbol("WriteTx");
const openWorkBrand: unique symbol = Symbol("OpenWorkExported");

export type WriteTx = {
	readonly [writeTxBrand]: {
		readonly store: object;
		readonly serial: number;
	};
};

export type OpenWorkExported = {
	readonly tx: WriteTx;
	readonly [openWorkBrand]: true;
};

export type StoreTransactionErrorReason = "nested" | "async_callback" | "inactive_tx" | "foreign_store";

export class StoreTransactionError extends Error {
	constructor(readonly reason: StoreTransactionErrorReason) {
		super(`store transaction: ${reason}`);
		this.name = "StoreTransactionError";
	}
}

export type StoreOwnershipErrorReason = "unclassified" | "not_owner";

export class StoreOwnershipError extends Error {
	readonly reason: StoreOwnershipErrorReason;
	readonly group?: OwnerGroup;
	readonly table?: string;

	constructor(input: { reason: "unclassified" } | { reason: "not_owner"; group: OwnerGroup; table: string }) {
		super(input.reason === "unclassified" ? "store ownership: unclassified SQL" : `store ownership: ${input.group} does not own ${input.table}`);
		this.name = "StoreOwnershipError";
		this.reason = input.reason;
		if (input.reason === "not_owner") {
			this.group = input.group;
			this.table = input.table;
		}
	}
}

export function mintWriteTx(store: object, serial: number): WriteTx {
	return Object.freeze({ [writeTxBrand]: Object.freeze({ store, serial }) });
}

export function writeTxStore(tx: WriteTx): object | undefined {
	return (tx as Partial<WriteTx> | null)?.[writeTxBrand]?.store;
}

export function mintOpenWorkExported(tx: WriteTx): OpenWorkExported {
	return Object.freeze({ tx, [openWorkBrand]: true as const });
}

export function assertOpenWorkExported(tx: WriteTx, exported: OpenWorkExported): void {
	if (!exported || exported.tx !== tx || exported[openWorkBrand] !== true)
		throw new StoreTransactionError("inactive_tx");
}
