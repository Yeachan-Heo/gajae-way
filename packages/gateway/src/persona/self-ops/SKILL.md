---
name: self-ops
description: Operate and diagnose a Gajaeway host safely before service control, channel-config changes, or lost-reply investigation.
---

# Gajaeway self operations

Use this skill before restarting a Gajaeway service, before editing channel policy, or when a reply appears lost.

1. Read `service-control.md` before any service-manager command.
2. Read `config-and-restarts.md` before changing configuration or verifying a restart.
3. Read `state-inspection.md` before diagnosing delivery, engagement, or schema state.

Do not execute a restart from the inbound turn that must deliver the current reply. Record or queue the operation for an out-of-turn operator context instead.
