# P1c live Discord drill receipt (G007)

- **Date:** 2026-08-25 (Asia/Seoul); source timestamps below are Discord UTC.
- **Stack:** v1 Bun workspace (merged to `main` at `ea3d3a0`), run from the rewrite worktree.
- **Deployment home:** `GAJAEWAY_HOME=/Users/bellman/gajaeway-play/discord-v1` (mode 0700).
- **Credential handling:** the Discord bot token was consumed only via the credential-file reference `/Users/bellman/gajaeway-play/discord/discord-token` (mode 0600), per config doctrine (`config.json` `credentials.discord.credentialFile`, `adapter-discord.json` `tokenFile`). The token was never printed, logged, or written to this artifact.
- **gjc runtime:** the gateway runs with a dedicated agent dir (`GJC_CODING_AGENT_DIR=/Users/bellman/gajaeway-play/discord-v1/gjc-agent`) because the machine-default gjc SDK broker state was wedged (`terminal_uncertain`: "Lifecycle startup cleanup could not be proven"). Turn path proven directly: `gjc --resume <session> -p "say exactly: drill-ok"` → `drill-ok`, exit 0.
- **Legacy cleanup:** the orphaned legacy adapter (`dist/gajaeway-discord`, pid 63600, whose Rust daemon died 2026-08-24) was stopped before the drill so only the v1 stack held the bot online.

## Live surface

- Bot: `에르가재` (`1468532331001413743`), real guild + owner DM channel `1468535438498336923` (owner `yeachanheo`, `660473980301344768`), mention-gated channel `1493635653441945762` (default mention gating; no `engagement: "open"` configured).
- Gateway: `gajaeway status` → `{"profileVersion":"1.0","capabilities":["gateway.core","chat.loopback"], ...}` healthy over the Unix socket.
- Adapter: `Discord adapter connected to gateway.` + `Discord adapter connected.` (discord.js WS ready) in `adapter.log`.

## Turn 1 — end-to-end delivery over real Discord

- `chat.send` (origin `discord/dm/1468535438498336923/peer=660473980301344768`) → `{"turnId":"bc07c1b2-...","engaged":true}`.
- gjc-authored reply delivered to the real DM: message `1541795176480247810` at `2026-08-25T13:03:35.242Z` (bot=true).
- Ledger: delivery `7b673cf7-a35c-4e96-9b3a-2574a6c71d11` → `confirmed`.
- Note: this turn was itself delivered via the redelivery-on-negotiate path (visible `[recovered - may be a duplicate]` label) because the adapter renegotiated after a gateway restart between prepare and settle — the ledger absorbed the ambiguity exactly as designed.

## Turn 2 — kill -9 mid-delivery, labeled redelivery

1. Adapter stopped; `chat.send` turn 2 → delivery `e076ec1b-3ae3-41bb-b25a-d931c4252494` reached ledger state `inflight` (verified by direct SQLite read).
2. Gateway process (pid 24923) killed with `SIGKILL` while the delivery was inflight; ledger still showed `inflight` after death.
3. Gateway restarted: boot recovery line `{"recovery":{"recovered":1,"pending":1,"pruned":0}}`.
4. Adapter restarted, negotiated, received the redelivery with `duplicateWarning`, and posted to the real DM: message `1541796058344988682` at `2026-08-25T13:07:05.495Z`, content prefixed `[recovered - may be a duplicate]`.
5. Ledger: `e076ec1b-...` → `confirmed`. No duplicate un-labeled send observed (REST history shows exactly one turn-2 message).

## Owner-authored turns

- Drill prompt posted to the owner DM (message `1541796229510471751` at `2026-08-25T13:07:46.304Z`) requesting (1) a DM reply and (2) a bot mention in `1493635653441945762`.
- Owner-authored inbound turns were not exchanged during this window. Per the accepted ultragoal steering (ledger event `42f56a02-cd17-4b5a-a30b-5c3788c92bc4`), the owner directed reusing the prior live validation — `artifacts/g013-live-drill-receipt.md` records real owner DM turns followed by bot replies in this same channel (`1468535438498336923`, 2026-08-22), with restart/no-duplicate evidence — instead of requiring a new credentialed owner exchange. The live v1 stack remains connected; any owner reply or channel mention flows through the running adapter → gateway → gjc path.

## Verdict

Delivery ledger crash contract (at-least-once, boot redelivery, visible duplicate labeling) is proven live against real Discord with a genuine `kill -9` on the v1 stack. Owner-exchange evidence is carried by the accepted steering reuse of the g013 legacy live receipt. G007 requirements are satisfied: credential-file configuration, real guild presence, kill/restart redelivery with visible labeling, and this stored receipt.
