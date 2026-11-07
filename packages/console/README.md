# @gajae-gateway/console

A lightweight, Bun-native terminal UI library for building gajaeway setup and console commands. It provides ANSI rendering, keyboard input handling, and basic interactive components without external TUI or CLI dependencies.

## Main exports

- `ANSIRenderer`: ANSI colors, styling, cursor movement, and screen clearing.
- `KeyboardInput`: keyboard input with raw-mode handling and secret-input support.
- `Component`: base interface for UI components.
- `TextInput`: text and masked-secret input.
- `SelectInput`: interactive option selection.
- `ProgressBar`: terminal progress display.
- `Table`: tabular terminal output.
- `terminalDimensions()`: terminal width and height detection.
- `redact()`: secret masking for output strings.

```ts
import {
	ANSIRenderer,
	KeyboardInput,
	TextInput,
	SelectInput,
	ProgressBar,
	terminalDimensions,
	redact,
} from "@gajae-gateway/console";
import type { Component } from "@gajae-gateway/console";
```

Requires Bun 1.4.0 or newer. The SDK and protocol packages are peer dependencies. Secret values must only appear as masked output; never log or display raw credentials.

Licensed under AGPL-3.0-only.
