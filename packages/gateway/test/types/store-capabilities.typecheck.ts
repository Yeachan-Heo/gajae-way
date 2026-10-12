import {
	assertOpenWorkExported,
	type GroupDb,
	type OpenWorkExported,
	runGroupWrite,
	type WriteTx,
} from "../../src/store";

function settle(tx: WriteTx, exported: OpenWorkExported): void {
	assertOpenWorkExported(tx, exported);
}

declare const group: GroupDb;
declare const tx: WriteTx;

runGroupWrite(group, tx, "INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES ('o', 's', 'now')");
// @ts-expect-error Group writes require an active WriteTx capability.
runGroupWrite(group, "INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES ('o', 's', 'now')");
// @ts-expect-error Settlement requires the OpenWorkExported capability from this transaction.
settle(tx);
