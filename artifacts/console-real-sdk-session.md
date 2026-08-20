Real gajae-code SDK console session, captured 2026-08-20T01:22:16Z after the duplicate-event fix.
Command: gajaeway console (compiled dist/gajaeway) attached to a live gajaeway daemon.
Session: real gjc session 01a01cc1-be28-7000-9024-7038473b8540, bootstrapped WITHOUT the E2E file SDK; main.resumed=true.
Flow proven: owner message -> real model turn -> assistant reply rendered in the console -> server-derived 'Delivered as: prompt'.
Journal read back over the real UDS shows exactly 3 events for the turn: turn_start(1), assistant_message(1), turn_end(1),
with turn_start/turn_end sharing one stable payload shape {attempt_id, generation, lineage}.
This capture supersedes the pre-fix one, which showed duplicated turn transitions from the real SDK event set.
