Adopted-session round trip, captured 2026-08-20T11:47:13Z.
Architecture: HostSupervisor external backend. The persona session is a LIVE operator-run interactive gjc (PTY-resident, session 01a01efc-d113-7000-b087-247268ace23d); the compiled gajaeway daemon adopted it (bootstrap committed, strict re-verification fail-closed) and controls it exclusively through the credential-free broker CLI (send/tail/status). gajaeway does not host the session.
Proof: gateway main.submit accepted in ~7s with server-derived delivered_as=prompt; the turn ran in the operator's gjc process; the daemon journal projected exactly:
  turn_start  {attempt_id d44c96a8-..., lineage external}
  assistant_message {finalized true, text 'adopted controller works'}
  turn_end    {attempt_id d44c96a8-..., lineage external}
Reply text arrived via the broker transcript window (the real ring carries no message_end); ordering is enforced turn_start -> assistant_message -> turn_end.
Health honesty verified the same day: a wedged tail on the compiled binary reports unhealthy/degraded/tail_unavailable on way.health, way.status and health.json (the stale-addon extraction cache that previously masked this is now content-hash keyed).
