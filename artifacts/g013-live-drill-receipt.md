# G013 live Discord drill receipt (read-only)

- **Frozen QA snapshot:** bb280f6 (sha256:6b8a85008fff58935c41c8225fe1c270e589104b660fd3d7d7b637b75fed8eb0)
- **Scope:** evidence-only audit. No live gateway state, process, broker session, Discord message, typing indicator, reaction, or consumer checkpoint was modified.
- **Credential handling:** the Discord token was read by the local GET process only to form its Authorization header. It was neither printed nor written to this artifact or any QA result.

## UDS evidence

Read-only socket: `/Users/bellman/gajaeway-play/discord/state/rpc.sock`. The only calls were `way.status` and `main.events.read` with `{"cursor":"1:800","limit":100,"wait_ms":0}`.

- Gateway: status=`healthy`, state=`running`, turn_state=`busy`, transcript_proof=`proven`, transcript_verification=`verified`.
- Journal: head_cursor=`1:896`, transcript_delivery_gap_count=`5`, returned next_cursor=`1:896`.
- Discord consumer checkpoint: consumer_id=`gajaeway-discord`, cursor=`1:853`. This is the requested `1:853+` checkpoint evidence (the live head had advanced to `1:896`).

### Owner-DM assistant events

Only durable metadata and content digests are retained below; full owner/assistant text is intentionally omitted from this receipt.

| seq | kind | finalized | transcript message id | text characters | SHA-256(text) |
| ---: | --- | --- | --- | ---: | --- |
| 845 | assistant_message | true | `18d92ffc` | 32 | `40373700d7e4aa92a25e9d8665dbb4b75f8b9bc39c9dd18afd160707018c3054` |
| 848 | assistant_message | true | `1942029f` | 280 | `d56c1c98ed8912a5013c09087c9332a29fcb193ecfb998bbe8a788cb5a96ef8c` |
| 850 | assistant_message | true | `6c3dd036` | 454 | `002b22df610feac86a719ca376ef4a03016a73ebb1d24d00432e91a2e826297a` |
| 853 | assistant_message | true | `4d291a2b` | 173 | `919f3f686c43281c909bcfe5168066347d3ed4186c464661b90b36569089398f` |

### Delivery-gap events in the same read window

| seq | reason | delivered through | unprojectable entry | available through |
| ---: | --- | --- | --- | --- |
| 843 | `transcript_delivery_unprovable` | `2b32ed55` | `923b37f2` | `998c67cd` |
| 844 | `transcript_delivery_unprovable` | `998c67cd` | `da6e173d` | `426947d1` |
| 846 | `transcript_delivery_unprovable` | `18d92ffc` | `a2073f20` | `a2073f20` |
| 851 | `transcript_delivery_unprovable` | `6c3dd036` | `78f88283` | `78f88283` |
| 852 | `transcript_delivery_unprovable` | `78f88283` | `0adc062b` | `4e4a2401` |

## Discord REST history evidence

Read-only GET only: `GET /api/v10/channels/1468535438498336923/messages?limit=100`, with Authorization header. The response held 17 messages; 7 had author.bot=true. No POST/PUT/PATCH/DELETE request was made.

Requested owner/bot timing rows (source UTC retained; Asia/Seoul display conversion supplied):

| message id | source timestamp | Asia/Seoul display | author.bot | content characters | SHA-256(content) |
| --- | --- | --- | --- | ---: | --- |
| `1540569081822974044` | `2026-08-22T03:51:31.496000+00:00` | `2026-08-22T12:51:31+09:00` | false | 2 | `2689367b205c16ce32ed4200942b8b8b1e262dfc70d9bc9fbc77c49699a4f1df` |
| `1540569248127000598` | `2026-08-22T03:52:11.146000+00:00` | `2026-08-22T12:52:11+09:00` | true | 32 | `40373700d7e4aa92a25e9d8665dbb4b75f8b9bc39c9dd18afd160707018c3054` |
| `1540569279777214555` | `2026-08-22T03:52:18.692000+00:00` | `2026-08-22T12:52:18+09:00` | true | 280 | `d56c1c98ed8912a5013c09087c9332a29fcb193ecfb998bbe8a788cb5a96ef8c` |
| `1540569341924343808` | `2026-08-22T03:52:33.509000+00:00` | `2026-08-22T12:52:33+09:00` | false | 16 | `f72d80f4e24f8ba57f97d547cf7373a5aaca6a6725bdf8f1f1d846a587856409` |
| `1540569544144330795` | `2026-08-22T03:53:21.722000+00:00` | `2026-08-22T12:53:21+09:00` | true | 454 | `002b22df610feac86a719ca376ef4a03016a73ebb1d24d00432e91a2e826297a` |

The two non-bot rows at 2026-08-22T03:51:31.496000+00:00 and 2026-08-22T03:52:33.509000+00:00 are followed by bot rows at 03:52:11.146000+00:00, 03:52:18.692000+00:00, and 03:53:21.722000+00:00, matching the requested owner-DM drill chronology.

## Restart / no-duplicate checkpoint result

- Read-only post-drill observation: 7 bot-authored posts are present in the channel history and the durable `gajaeway-discord` checkpoint is `1:853`.
- This is consistent with the stated adapter-restart outcome of seven bot posts before and after restart with checkpoint resume. QA did **not** restart the live adapter or invoke any mutable UDS method, so the artifact does not claim to have independently recreated the restart; it records the live post-restart count and durable resume checkpoint without changing the deployment.

### Bot post inventory (content redacted/digested)

| message id | source timestamp | Asia/Seoul display | content characters | SHA-256(content) |
| --- | --- | --- | ---: | --- |
| `1540550953252425828` | `2026-08-22T02:39:29.308000+00:00` | `2026-08-22T11:39:29+09:00` | 23 | `7bccb0a9a6233632004fc47769a65f4e3dbc9d64bb3ebd5e477005ee1c357d10` |
| `1540550954854912050` | `2026-08-22T02:39:29.690000+00:00` | `2026-08-22T11:39:29+09:00` | 71 | `c8997378ca3bc784cb9dcd49d7b51bd8994ee6362e0efd0606d8ee467911a6d1` |
| `1540550956087906437` | `2026-08-22T02:39:29.984000+00:00` | `2026-08-22T11:39:29+09:00` | 128 | `1bdd788e48c12994b879bd5338d3dc2a215868ff108b1ced8d8c4a94e6962fac` |
| `1540569248127000598` | `2026-08-22T03:52:11.146000+00:00` | `2026-08-22T12:52:11+09:00` | 32 | `40373700d7e4aa92a25e9d8665dbb4b75f8b9bc39c9dd18afd160707018c3054` |
| `1540569279777214555` | `2026-08-22T03:52:18.692000+00:00` | `2026-08-22T12:52:18+09:00` | 280 | `d56c1c98ed8912a5013c09087c9332a29fcb193ecfb998bbe8a788cb5a96ef8c` |
| `1540569544144330795` | `2026-08-22T03:53:21.722000+00:00` | `2026-08-22T12:53:21+09:00` | 454 | `002b22df610feac86a719ca376ef4a03016a73ebb1d24d00432e91a2e826297a` |
| `1540569631641571380` | `2026-08-22T03:53:42.583000+00:00` | `2026-08-22T12:53:42+09:00` | 173 | `919f3f686c43281c909bcfe5168066347d3ed4186c464661b90b36569089398f` |

## Limitations

- The authorized live-surface operations were intentionally limited to two UDS reads and one Discord history GET. No command was sent through the adopted session and no delivery, typing, reaction, gateway, or state-directory mutation was performed.
- The current history count/checkpoint corroborate the no-duplicate restart result but cannot itself prove the pre-restart count; recreating that comparison would require a restart, prohibited for this evidence capture.
