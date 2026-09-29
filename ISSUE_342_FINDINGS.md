# Issue #342 Investigation Findings

## Problem Statement
144 monitor_failures rows with code `session_bind_failed` had incomplete detail records missing `operation` and `operation_args` JSON fields, while `phase=bind` and `sessionId` were present.

## Root Cause Analysis

### Evidence Location
- **File**: `packages/gateway/src/monitors/propagate.ts`
- **Line**: 933-935 (error detail construction in catch block)
- **Truncation Point**: `packages/gateway/src/store/db.ts:3856` (detail truncated to 500 chars)

### The Problem Chain

1. **Detail Construction** (propagate.ts:933-935, pre-fix):
```typescript
`dispatch phase failed (${code}): ${failureDetail(error)} ${JSON.stringify({ phase: dispatchPhase, operation: dispatchOperation, operation_args: dispatchOperationArgs, sessionId: boundSessionId ?? null, origin: sessionOriginKey, attempt: row.dispatch_attempts + 1 })}`
```

2. **Truncation** (db.ts:3856):
```typescript
.run(eventId, code, detail.slice(0, 500), ...);
```

3. **The Failure**:
   - `failureDetail(error)` produces a long diagnostic string (typically 200-400 chars for operation_failed errors)
   - When combined with failureDetail FIRST, the JSON structure at the end gets truncated
   - Example: A detail string with 450 chars of failureDetail leaves only 50 chars for the JSON
   - The 500-char truncation cuts off the `"operation"` and `"operation_args"` fields

### Test Confirmation
The test `"demonstrates the problem: OLD format (failureDetail first) loses operation"` confirms:
- OLD format with failureDetail first: operation field is LOST in 500-char truncation
- The error detail becomes: `dispatch phase failed (session_bind_failed): GjcCliError(operation_failed...[truncated]...`
- No `"operation":"bind"` or `"operation_args"` in the stored detail

## Bind Operations That Can Fail

Within `sessionPort.bind()` (packages/gateway/src/orchestrator/session-port.ts:338-433), these operations can throw GjcCliError:

1. **inspect** (line 359): Checking if existing session is live - can fail with connection/envelope errors
2. **resume** (line 367): Resuming saved session authority - can fail with multiple errors including those from session.resume operation
3. **create** (line 389 via `#createSession`): Creating new session via session.create - can fail with spawn_failed, terminal_uncertain, etc.
4. **awaitIndexed** (line 431): Waiting for new session to be indexed - can fail with session_unavailable

When any of these fails with a GjcCliError during bind phase:
- `dispatchPhase = "bind"` (set at line 715)
- `dispatchOperation = "bind"` (set at line 715)
- `dispatchOperationArgs = { epoch: boundEpoch, hasModel: effectiveModel !== undefined }` (set at lines 716-719)

## The Fix

### Changes Made to `packages/gateway/src/monitors/propagate.ts` (lines 929-945)

**Key improvements:**
1. Place JSON structured detail FIRST in the string (before failureDetail)
2. Convert `undefined` values to `null` using the nullish coalescing operator (`??`) so JSON.stringify preserves the fields
3. Put the potentially long `failureDetail()` AFTER the JSON so truncation doesn't affect critical fields

```typescript
// FIXED: Place JSON detail FIRST so it survives 500-char truncation
// Convert undefined to null so operation/operation_args are never dropped by JSON.stringify
const structuredDetail = JSON.stringify({
  phase: dispatchPhase,
  operation: dispatchOperation ?? null,  // Never drops undefined
  operation_args: dispatchOperationArgs ?? null,  // Never drops undefined
  sessionId: boundSessionId ?? null,
  origin: sessionOriginKey,
  attempt: row.dispatch_attempts + 1,
});
const detail = `dispatch phase failed (${code}): ${structuredDetail} ${failureDetail(error)}`;
```

### Result
- Critical diagnostic fields (phase, operation, operation_args, sessionId, origin, attempt) are guaranteed to survive 500-char truncation
- The JSON structure is compact (~120-150 chars) and always preserved
- Long failureDetail can be truncated without losing operation context

## Test Coverage

Created `packages/gateway/test/monitor-bind-failure.test.ts` with 4 tests:

1. **should preserve operation and operation_args even with long failure details**
   - Tests that structured detail survives with long failureDetail
   - Verifies all JSON fields are present after truncation

2. **should preserve operation in JSON even when undefined**
   - Tests that undefined values become null
   - Verifies operation field is never dropped

3. **should preserve phase/operation/args by placing JSON before failureDetail**
   - Tests the fixed format with JSON-first approach
   - Confirms operation survives 500-char truncation

4. **demonstrates the problem: OLD format (failureDetail first) loses operation**
   - Reproduces the exact #342 problem with old format
   - Confirms operation is lost with pre-fix code

## Verification
- All new tests pass (4 pass, 0 fail)
- Existing gateway tests pass (monitor-recovery.test.ts: 54 pass, session-port.test.ts: 36 pass)
- No TypeScript errors introduced
- Fix is minimal and surgical (only affects error detail construction)
