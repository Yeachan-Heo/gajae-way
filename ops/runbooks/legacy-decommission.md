# Legacy writer decommission

This is an operator procedure, not a managed migration program. Before
`gajaeway` becomes the writer for a corpus, stop and disable **every** legacy
writer and outbound effect for that corpus: bot services, timers, cron jobs,
launch agents, queue workers, webhook relays, and manual push automation. Do
not run a gateway and a legacy bot together "just for validation".

1. Inventory the legacy components before disabling them. Keep a
   machine-readable, secret-free record in the operator change system. Record
   secret *references* only, never values:

   ```yaml
   corpus: /srv/example/corpus
   remote: ssh://git@example.invalid/example/corpus.git
   disabled_at: 2026-08-18T12:00:00Z
   components:
     - name: legacy-discord-bot
       type: systemd-service
       disable_command: systemctl disable --now legacy-discord-bot.service
       credential_reference: secret://ops/legacy-discord-bot-token
     - name: daily-memory-cron
       type: cron
       credential_reference: secret://ops/daily-memory-push-key
   ```

2. Stop and disable each listed writer/outbound component. Verify that no
   residual process, timer, cron invocation, remote worker, or in-flight push
   remains. Inspect the authoritative remote and local checkout before the
   new runtime is started:

   ```sh
   systemctl list-units --all 'legacy-*'
   systemctl list-timers --all
   pgrep -af 'legacy|clawdbot|discord' || true
   git -C /srv/example/corpus fetch --prune origin
   git -C /srv/example/corpus status --porcelain=v1
   git -C /srv/example/corpus log --left-right --graph --cherry-pick origin/main...HEAD
   ```

3. Reconcile unexpected refs, a non-fast-forward history, a dirty index, or
   any residual push before proceeding. Preserve the inventory and verification
   evidence so a reversal can be planned without recovering copied secrets.
4. Configure the gajaeway profile and systemd `ReadWritePaths=` for that one
   corpus, then adopt the operator-owned live GJC `main` session through the
   [bootstrap ceremony](bootstrap-ceremony.md) and start the two units using
   the [operations runbook](operations.md). From that point, gajaeway is the
   sole permitted writer.

Later re-authorizing a legacy job is not a blanket rollback. v2 P13 defines
the owner-gated, **one-job-at-a-time** re-authorization flow; each migration
job must receive its own authorization and migration receipt before it can
write or push again.
