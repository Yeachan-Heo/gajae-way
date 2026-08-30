# Admin service entry: live drill receipt

Compiled binary: `bun build --compile packages/admin/src/main.ts --outfile /tmp/gajaeway-admin-check`.
Gateway: a stub unix-socket server speaking the `@gajaeway/protocol` frame profile (hello/negotiated/request/response),
so the entry's own socket connect, bind, host pinning, routing and mutation gate are exercised end to end without a live gateway.
Environment under test: `GAJAEWAY_SOCKET=/tmp/admin-drill/gw.sock`, `GAJAEWAY_ADMIN_PORT=8899` (both non-default).

Covers the three defects found in review: DNS-rebinding reachability (Host pinning), a zero-width-space
actor passing the non-blank check, and a failed bind leaving the process alive because the gateway socket
held the event loop open.

```text
=== STARTUP LOG ===
admin console listening on http://127.0.0.1:8899 (gateway socket /tmp/admin-drill/gw.sock)

=== READ SURFACE ===
### GET /api/status (200) -> {"ok":true,"result":{"stub":"gateway.status"}} [200]
### GET /api/core (200) -> {"ok":true,"result":{"stub":"gateway.core"}} [200]
### GET /api/sessions (200) -> {"ok":true,"result":{"stub":"session.list"}} [200]
### GET /api/monitors (200) -> {"ok":true,"result":{"stub":"monitor.list"}} [200]
### GET /api/operations (200) -> {"operations":[{"id":"monitor.add","method":"monitor.add","summary":"Create a monitor"},{"id":"monitor.remove","method":"monitor.remove","summary":"Remove a monitor"},{"id":"monitor.test","method":"monitor.test","summary":"Fire a monitor test event"},{"id":"ops.backup","method":"ops.backup","summary":"Write a gateway backup"},{"id":"ops.integrity","method":"ops.integrity","summary":"Run an integrity check"}]} [200]
### POST /api/status (405) -> {"error":"read routes accept GET only"} [405]
### PATCH /api/monitors (405) -> {"error":"read routes accept GET only"} [405]
### POST /api/operations (404, documented) -> {"ok":false,"error":"not found"} [404]
### GET /api/mutations (405) -> {"ok":false,"error":"mutations require POST"} [405]
### unknown path (404) -> {"ok":false,"error":"not found"} [404]

=== A-2 HOST PINNING (DNS rebinding) ===
### GET /api/status Host: evil.example (403) -> {"ok":false,"error":"unexpected host header"} [403]
### POST mutation Host: evil.example (403) -> {"ok":false,"error":"unexpected host header"} [403]
### GET /api/status Host: localhost:8899 (200) -> {"ok":true,"result":{"stub":"gateway.status"}} [200]

=== MUTATION GATE ===
### no confirm (428) -> {"ok":false,"error":"explicit confirmation is required: echo the operation id in `confirm`"} [428]
### confirm = other allowlisted op (428) -> {"ok":false,"error":"explicit confirmation is required: echo the operation id in `confirm`"} [428]
### no actor (401) -> {"ok":false,"error":"an actor is required for a mutation"} [401]
### whitespace actor (401) -> {"ok":false,"error":"an actor is required for a mutation"} [401]
### V2 zero-width-space actor (must be 401, must NOT execute) -> {"ok":false,"error":"an actor is required for a mutation"} [401]
### chat.send (404) -> {"ok":false,"error":"operation chat.send is not allowlisted"} [404]
### malformed json (400) -> {"ok":false,"error":"body must be json"} [400]
### confirmed ops.integrity (200) -> {"ok":true,"operationId":"ops.integrity","result":{"stub":"ops.integrity"}} [200]

=== BIND ===
COMMAND     PID    USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME
gajaeway- 38281 bellman    5u  IPv4 0xc6a890e96f5fd6ca      0t0  TCP 127.0.0.1:8899 (LISTEN)
### non-loopback http://192.168.0.6:8899 (must refuse) -> curl: (7) Failed to connect to 192.168.0.6 port 8899 after 10 ms: Couldn't connect to server
000
CONNECTION REFUSED (curl exit 7)

=== ADMIN SERVICE LOG / AUDIT TRAIL ===
admin console listening on http://127.0.0.1:8899 (gateway socket /tmp/admin-drill/gw.sock)
admin.audit {"at":"2026-08-27T02:18:39.660Z","operationId":"ops.integrity","actor":"hyungnim","decision":"rejected","reason":"explicit confirmation is required: echo the operation id in `confirm`"}
admin.audit {"at":"2026-08-27T02:18:39.668Z","operationId":"ops.integrity","actor":"hyungnim","decision":"rejected","reason":"explicit confirmation is required: echo the operation id in `confirm`"}
admin.audit {"at":"2026-08-27T02:18:39.675Z","operationId":"ops.integrity","actor":"anonymous","decision":"rejected","reason":"an actor is required for a mutation"}
admin.audit {"at":"2026-08-27T02:18:39.683Z","operationId":"ops.integrity","actor":"anonymous","decision":"rejected","reason":"an actor is required for a mutation"}
admin.audit {"at":"2026-08-27T02:18:39.691Z","operationId":"ops.backup","actor":"anonymous","decision":"rejected","reason":"an actor is required for a mutation"}
admin.audit {"at":"2026-08-27T02:18:39.699Z","operationId":"chat.send","actor":"hyungnim","decision":"rejected","reason":"operation chat.send is not allowlisted","params":{"text":"pwned"}}
admin.audit {"at":"2026-08-27T02:18:39.714Z","operationId":"ops.integrity","actor":"hyungnim","decision":"allowed"}
=== VERBS THAT REACHED THE GATEWAY ===
stub gateway on /tmp/admin-drill/gw.sock
stub-gateway saw gateway.status null
stub-gateway saw gateway.core null
stub-gateway saw session.list null
stub-gateway saw monitor.list null
stub-gateway saw gateway.status null
stub-gateway saw ops.integrity null
### chat.send occurrences in the gateway log: 0
### ops.backup occurrences in the gateway log (must be 0 - the invisible actor was refused): 0

=== V1 BIND-FAILURE MUST EXIT, NOT HANG ===
### exited after 1s
### exit code: 1
### output: Failed to start server. Is port 8911 in use?
```
