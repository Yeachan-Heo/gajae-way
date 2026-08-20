Live busy-persona adoption proof, captured 2026-08-20T19:44:57Z.

Subject: the LIVE gaebal-gajae persona session (tmux gaebal-play, session 01a01f03-1b87-7000-9461-6f39fe19100c, 1.6G corpus snapshot, continuously active by SOUL).

Sequence proven on the compiled binary at 43bcabe:
1. gajaeway bootstrap adopted the hours-old session: committed, transcript_proof proven at bootstrap (verification tail enveloped).
2. Daemon healthy, resumed=true, turn_state live from context.get polling.
3. main.submit accepted in ~7s (delivered_as prompt, server-derived).
4. The prompt arrived in the persona's REAL gjc TUI and he replied in one sentence (tmux capture retained below). The owner surface received the reply directly - the by-design primary path.
5. The gateway stayed healthy/running throughout (no failed_closed), with transcript_proof proven and honest busy state while the persona continued autonomous work.

Earlier same-day falsifications fixed and re-proven live: pending-proof adoption with admission fencing (submit refused with machine reason until first terminal boundary bound the proof), ring rotation as journaled resync instead of fail-closed, lexicographic (generation,seq) ordering (generation advance is forward progress), watermark bound only from ring envelopes.

KNOWN LIMITATION (recorded honestly): the credential-free broker CLI yields tail envelopes only at terminal turn boundaries, so JOURNAL projection of a finalized reply waits until the persona settles; a continuously-busy persona delays journal consumers (Discord/cockpit) while the owner's attached TUI sees replies immediately. Upstream needs a snapshot-tail or transcript query to close this.

tmux capture excerpt:
 gajae: 게이트웨이 어댑션 최종 검증 수신 완료, 형님 — 한 문장 모드로 정상 응답 중입니다.
