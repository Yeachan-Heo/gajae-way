# Console public API

`@gajae-gateway/console` is a Bun-native terminal UI package. Import the API from the package root. Rendering uses ANSI escape sequences; interactive keyboard input requires a TTY. Applications own component layout and dispatch keys to components.

## TUI core

### Types and constants

```ts
type Color = "black" | "red" | "green" | "yellow" | "blue"
  | "magenta" | "cyan" | "white" | "reset";
interface Dimensions { width: number; height: number }
interface KeyPressEvent {
  key: string;
  code: string;
  isShift: boolean;
  isCtrl: boolean;
  isAlt: boolean;
}
interface TerminalOutput {
  write(text: string): unknown;
  flush?(): unknown;
}
type TerminalInput = import("node:stream").Readable & {
  isTTY?: boolean;
  isRaw?: boolean;
  setRawMode?(enabled: boolean): unknown;
};
```

`BLACK`, `RED`, `GREEN`, `YELLOW`, `BLUE`, `MAGENTA`, `CYAN`, `WHITE`, and `RESET` export the corresponding lowercase `Color` values. `KeyPressEvent.key` is a printable character or a named key such as `enter`, `backspace`, `delete`, `escape`, `tab`, `up`, `down`, `left`, `right`, `home`, or `end`. `code` preserves the input sequence; modifiers are separate booleans.

### ANSIRenderer

`new ANSIRenderer(output?: TerminalOutput)` defaults to `Bun.stdout.writer()`. Writes flush when the output provides `flush()`.

| Method | Behavior |
| --- | --- |
| `clear(): void` | Clear screen and move to the top-left corner. |
| `clearLine(): void` | Clear current line and return to its first column. |
| `moveCursor(row: number, col: number): void` | Move to one-based coordinates; throw `RangeError` for non-positive or non-integer coordinates. |
| `write(text: string, color?: Color, background?: Color, bold?: boolean, underline?: boolean): void` | Write styled text and reset styles afterward. Both boolean options default to false. No newline is appended. |
| `resetStyle(): void` | Reset terminal styles. |
| `hideCursor(): void` / `showCursor(): void` | Change cursor visibility. |

The renderer writes raw text, including control sequences: sanitize untrusted text before passing it directly to `write()`.

```ts
import { ANSIRenderer, CYAN } from "@gajae-gateway/console";
const screen = new ANSIRenderer();
try {
  screen.hideCursor();
  screen.clear();
  screen.moveCursor(1, 1);
  screen.write("Gateway ready\n", CYAN, undefined, true);
} finally {
  screen.resetStyle();
  screen.showCursor();
}
```

### KeyboardInput

`new KeyboardInput(input?: TerminalInput, output?: TerminalOutput)` defaults to Bun stdin and stdout. It starts reading immediately and enables raw mode for a TTY. There are no `start()` or `stop()` methods.

| Method | Behavior |
| --- | --- |
| `readKey(): Promise<KeyPressEvent>` | Read the next queued key or wait for input. |
| `readLine(prompt?: string): Promise<string>` | Echo printable input, support backspace, and return on Enter. Prompt defaults to empty. |
| `readSecretLine(prompt?: string): Promise<string>` | Read a line without echoing characters or masks. |
| `close(): void` | Idempotently detach listeners, restore prior raw-mode state, discard queued keys, and reject pending reads. |

End-of-input, stream failure, and reads after close reject. Line reads reject on Ctrl+C/Ctrl+D. Do not overlap line reads with other reads. Always close the reader in `finally`; it owns terminal raw-mode lifetime.

```ts
import { KeyboardInput } from "@gajae-gateway/console";
const keyboard = new KeyboardInput();
try {
  const name = await keyboard.readLine("Name: ");
  const secret = await keyboard.readSecretLine("Token: ");
  // Pass secret to validation; never print it.
} finally {
  keyboard.close();
}
```

### Utilities

- `terminalDimensions(): Dimensions`: current stdout columns and rows, falling back independently to 80 columns and 24 rows.
- `sleep(ms: number): Promise<void>`: delegate to `Bun.sleep(ms)`.
- `redact(text: string, secrets: Iterable<string>): string`: replace literal secret matches with `[redacted]`. Ignore empty secrets, deduplicate, and prioritize longer overlapping secrets. Short secrets are also redacted.

```ts
import { terminalDimensions, sleep, redact } from "@gajae-gateway/console";
const { width, height } = terminalDimensions();
const safe = redact("credential=example-token", ["example-token"]);
await sleep(20);
```

## Components

```ts
import {
  ANSIRenderer, TextInput, SelectInput, ProgressBar, Table,
  type Component, type Position, type KeyPressEvent,
} from "@gajae-gateway/console";

interface Position { row: number; col: number } // One-based coordinates.
interface Component<T = unknown> {
  render(renderer: ANSIRenderer, at: Position, width?: number, height?: number): void;
  handleKey(event: KeyPressEvent): "handled" | "unhandled";
  getValue(): T;
  setValue(value: T): void;
}
```

Widths are terminal cells, not string lengths. Explicit sizes must be non-negative safe integers; invalid sizes throw `RangeError`. Rendering clips to the supplied width and respects height. Component labels and displayed values strip terminal control characters. Components do not clear the entire screen or automatically consume keyboard input.

### TextInput

```ts
interface TextInputOptions {
  label?: string;      // Default: empty.
  initial?: string;    // Default: empty.
  maxLength?: number;  // Unicode code points; default: Number.MAX_SAFE_INTEGER.
  hidden?: boolean;   // Default: false; render asterisks instead of the value.
}
```

`new TextInput(options?)` implements `Component<string>`. `getValue()` returns the actual value, including when hidden; do not log secrets. `setValue(string)` strips controls, truncates to `maxLength`, and moves the caret to the end. Left/right/home/end move the caret; backspace/delete remove characters. Printable characters insert at the caret. Ctrl/Alt combinations, Tab, and Enter are unhandled. Rendering shows a `|` caret and horizontally scrolls to keep it visible.

```ts
const renderer = new ANSIRenderer();
const name = new TextInput({ label: "Name:", initial: "operator", maxLength: 40 });
name.render(renderer, { row: 1, col: 1 }, 50, 1);
name.handleKey({ key: "!", code: "!", isShift: false, isCtrl: false, isAlt: false });
const value = name.getValue(); // "operator!"
const token = new TextInput({ label: "Token:", hidden: true });
token.render(renderer, { row: 2, col: 1 }, 50, 1);
```

### SelectInput

```ts
interface SelectInputOptions<T> {
  label?: string;
  items: readonly { label: string; value: T }[];
  initial?: T;
  multiSelect?: boolean; // Default: false.
  initialSelected?: readonly T[];
}
```

`new SelectInput<T = string>(options)` implements `Component<T | undefined>`. It selects one item, initially the first unless `initial` is provided. Up/down move without wrapping; Enter confirms. `isConfirmed(): boolean` reports confirmation. Moving or calling `setValue(value)` clears confirmation. Values are matched with `Object.is`; an unknown value throws `RangeError`. Empty lists return `undefined` and do not handle keys. The viewport follows selection.

With `multiSelect: true`, Space toggles the focused item's checkbox and clears confirmation. `getSelectedValues(): T[]` returns checked values in item order. `initialSelected` prechecks matching values (unknown values throw `RangeError`). `getValue()` and `setValue()` still address the focused item, not the checked set.

```ts
const adapter = new SelectInput({
  label: "Adapter",
  items: [
    { label: "Discord", value: "discord" },
    { label: "Slack", value: "slack" },
  ],
  initial: "slack",
});
adapter.render(renderer, { row: 4, col: 1 }, 30, 3);
adapter.handleKey({ key: "enter", code: "\r", isShift: false, isCtrl: false, isAlt: false });
if (adapter.isConfirmed()) console.log(adapter.getValue());
const adapters = new SelectInput({
  multiSelect: true,
  items: [{ label: "Discord", value: "discord" }, { label: "Slack", value: "slack" }],
  initialSelected: ["discord"],
});
const enabled = adapters.getSelectedValues(); // ["discord"]
```

### ProgressBar

```ts
interface ProgressBarOptions { label?: string; width?: number }
```

`new ProgressBar(options?)` implements `Component<number>`. Its initial value is `0`; the default bar width is 20 cells, excluding label and percentage. `setValue(number)` clamps finite values to 0–100 and rejects non-finite values with `RangeError`. `getValue()` returns the clamped percentage. `handleKey()` always returns `"unhandled"`. Render width can shrink the bar.

```ts
const progress = new ProgressBar({ label: "Validation", width: 24 });
progress.setValue(75);
progress.render(renderer, { row: 8, col: 1 }, 50, 1);
```

### Table

```ts
type TableCell = string | number | boolean | null | undefined;
interface TableOptions {
  columns: readonly { header: string; width?: number }[];
  rows: readonly (readonly TableCell[])[];
}
```

`new Table(options)` implements `Component<readonly (readonly TableCell[])[]>`. `setValue(rows)` copies rows; `getValue(): TableCell[][]` returns a defensive copy. Column widths default to the maximum displayed width of the header and cells. Null, undefined, and missing cells display as empty strings; extra cells beyond declared columns are not displayed. Height includes the header. `handleKey()` always returns `"unhandled"`; scrolling belongs to the caller.

```ts
const sessions = new Table({
  columns: [{ header: "Session", width: 24 }, { header: "State" }],
  rows: [["demo-session", "ready"]],
});
sessions.render(renderer, { row: 10, col: 1 }, 60, 5);
sessions.setValue([["demo-session", "busy"]]);
```

## Setup wizard

```ts
interface SetupWizardOptions {
  readonly home?: string;
  readonly renderer?: Pick<ANSIRenderer,
    "clear" | "moveCursor" | "write" | "resetStyle" | "hideCursor" | "showCursor">;
  readonly input?: Pick<KeyboardInput, "readKey" | "close">;
  readonly socket?: string;
  readonly fetcher?: (input: string, init?: RequestInit) => Promise<Response>;
  readonly fromEnv?: boolean;
  readonly env?: NodeJS.ProcessEnv;
  readonly parsed?: ParsedSetupArgs;
}
interface ParsedSetupArgs {
  readonly mode: "setup" | "status" | "broker-agent";
  readonly fromEnv: boolean;
  readonly adapters?: readonly ("discord" | "slack" | "telegram")[];
  readonly owners: readonly string[];
  readonly discordAppId?: string;
  readonly from?: string;
}
function runSetupWizard(options?: SetupWizardOptions): Promise<void>;
```

`ParsedSetupArgs` comes from the CLI setup module; the wizard consumes its adapters, owners, Discord application ID, and environment-mode setting. Use `dispatchSetupCommand` for status and broker-agent modes.

`home` defaults to `env.GAJAEWAY_HOME`, then `<env.HOME or OS home>/.gajaeway`. `env` defaults to `process.env`; `fetcher` defaults to global `fetch`. `fromEnv` defaults to `parsed.fromEnv`, then false. `socket` only appears in suggested daemon commands: the wizard does not contact a daemon.

Interactive setup offers update-existing/start-fresh configuration, adapter multi-selection, hidden token entry, validation with retry/re-entry, owner IDs, and confirmation before writing. Starting fresh replaces adapter configuration after confirmation. Credentials are validated before persistence; secrets are written with mode 0600. Escape or Ctrl+C cancels with a rejected promise. Cancellation after the configuration has been saved does not undo writes. On completion, service/daemon actions print instructions rather than starting processes.

Environment mode consumes no keyboard input and writes after successful validation without interactive confirmation. It reads the existing setup environment variables and respects adapter selection. The function resolves `undefined` on success, rejects on validation, cancellation, or filesystem errors, and returns no credentials or configuration object. It closes injected input when present and restores terminal styles/cursor in `finally`.

```ts
import { runSetupWizard } from "@gajae-gateway/console";

// Interactive: writes only after validation and confirmation.
await runSetupWizard({ home: "/tmp/gajaeway-demo" });

// Non-interactive: supply credentials through environment, never command-line flags.
await runSetupWizard({
  home: "/tmp/gajaeway-demo",
  fromEnv: true,
  env: process.env,
  parsed: { mode: "setup", fromEnv: true, adapters: ["telegram"], owners: ["123456789"] },
});
```

## Console dashboard

```ts
type ConsolePanel = "status" | "sessions" | "lanes" | "updates" | "logs";
interface ConsoleClient {
  request<T = unknown>(verb: string, params?: unknown): Promise<T>;
  on(event: string, handler: (payload: unknown) => void): () => void;
  onDisconnect(handler: (error: Error) => void): () => void;
  close(): Promise<void>;
}
interface ConsoleOptions {
  socket?: string;
  home?: string;
  panel?: ConsolePanel;
  renderer?: Pick<ANSIRenderer,
    "clear" | "moveCursor" | "write" | "hideCursor" | "showCursor" | "resetStyle">;
  input?: Pick<KeyboardInput, "readKey" | "close">;
  client?: ConsoleClient;
}
function runConsole(options?: ConsoleOptions): Promise<void>;
```

Runs until quit or input termination; resolves without a result object. `home` defaults to `GAJAEWAY_HOME` or the user's `.gajaeway` directory. `panel` defaults to `status`. Pass `socket` explicitly for predictable routing, especially in tests. A supplied client bypasses connection creation. The dashboard closes its client and input, including injected instances, unsubscribes handlers, cancels its refresh timer, and restores cursor/style on exit.

Status, sessions, and lanes use `gateway.status`, `session.list`, and `work.jobs`. Logs read `<home>/gateway.log` with bounded reads (last 128 KiB, up to 1,000 lines). Data refreshes every two seconds and on relevant gateway events. Disconnects require quitting and reopening to reconnect; unavailable capabilities are displayed rather than simulated. The updates panel delegates to the existing CLI updater and can perform network requests.

| Command | Action |
| --- | --- |
| `1`–`5` | Open Status, Sessions, Lanes, Updates, or Logs. |
| Right / Tab, Left | Cycle panels forward or backward. |
| Up / Down | Select a session or scroll log lines. |
| `i` / Enter in Sessions | Toggle selected-session inspection. |
| `r` | Refresh; also check updates when in Updates. |
| `u` / `a` | Open Updates, check if needed, and request confirmation when an update is available. |
| Enter / Escape during confirmation | Apply update / cancel. Applying can modify the installation. |
| `d` | Request diagnostics through `ops.cycle`. |
| `g` | Report cleanup unavailable; no garbage-collection RPC is implemented. |
| Space in Logs | Resume log auto-scroll. |
| `q` / Ctrl+C | Quit. |

```ts
import { runConsole } from "@gajae-gateway/console";
await runConsole({
  home: "/tmp/gajaeway-demo",
  socket: "/tmp/gajaeway-demo/gateway.sock", // A running test gateway.
  panel: "sessions",
});
```

## CLI dispatchers

```ts
function dispatchSetupCommand(
  args: readonly string[], options?: SetupCommandOptions,
): Promise<number>;
function dispatchConsoleCommand(
  args: readonly string[], options?: ConsoleCommandOptions,
): Promise<number>;
```

Pass only arguments after the subcommand. Both resolve to exit codes: `0` for success/help, `2` for invalid arguments, and `1` for runtime failure. Broker-agent mode preserves the legacy runner's exit status. These functions return codes rather than exiting the process.

### SetupCommandOptions

Extends `Partial<RunSetupOptions>` from the CLI setup module:

| Option | Type / purpose |
| --- | --- |
| `home` | Optional string; configuration home. Defaults to `env.GAJAEWAY_HOME` or `<env.HOME>/.gajaeway`, using process environment when `env` is absent. |
| `env` | `NodeJS.ProcessEnv`; environment for setup. |
| `binDir` | Optional string; binary directory for legacy broker provisioning. |
| `fetch` | `(input: string, init?: RequestInit) => Promise<Response>`; credential-validation transport. Forwarded to wizard as `fetcher`. |
| `prompter` | `() => Prompter`; legacy prompt factory (not the TUI input override). |
| `log` | `(line: string) => void`; output fallback when `out` is absent. |
| `runBrokerAgent` | `BrokerAgentRunner`; legacy provisioning runner. |
| `socket` | Optional string; forwarded to the wizard. |
| `out`, `err` | `(text: string) => void`; default stdout/stderr console callbacks. |
| `wizard` | `typeof runSetupWizard`; injectable interactive entry point. |
| `legacySetup` | `typeof runSetup`; injectable legacy entry point. |

`--from-env`, `--status`, and `--broker-agent [--from DIR]` use legacy setup. Interactive setup uses the wizard and forwards parsed `--adapters`, `--owner`, and `--discord-app-id`. Credentials intentionally have no command-line token flags. Use hidden prompts or the documented setup environment variables.

### ConsoleCommandOptions

Extends `ConsoleOptions` with `out?: (text: string) => void`, `err?: (text: string) => void`, and `dashboard?: typeof runConsole`. `--panel status|sessions|lanes|updates|logs` overrides `options.panel`. The dispatcher resolves the socket from `options.socket`, `GAJAEWAY_SOCKET_PATH`, or `<home>/gateway.sock`; empty or NUL-containing paths return code 2. Global CLI `--socket PATH` is resolved before dispatch, not parsed as a dispatcher argument. Both dispatchers recognize standalone `--help` and `-h`.

```ts
import { dispatchSetupCommand, dispatchConsoleCommand } from "@gajae-gateway/console";

const helpCode = await dispatchSetupCommand(["--help"], { out: console.log });
const consoleCode = await dispatchConsoleCommand(["--panel", "logs"], {
  home: "/tmp/gajaeway-demo",
  socket: "/tmp/gajaeway-demo/gateway.sock",
});
```

```sh
gajaeway setup
gajaeway setup --adapters discord --owner 123456789 --discord-app-id 987654321
gajaeway setup --from-env --adapters telegram
gajaeway setup --status
gajaeway --socket /tmp/gajaeway-demo/gateway.sock console --panel sessions
gajaeway-console --help
gajaeway-console console --panel logs
```

For isolated verification, use a temporary home, a test-only socket, injected input/output, and mocked credential validation. Do not use production credentials or invoke update confirmation against an installed production binary.
