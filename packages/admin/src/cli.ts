/**
 * Argv contract for `gajaeway-admin`.
 *
 * The binary used to ignore argv entirely: it connected to the gateway socket
 * and bound a port at import time, so `gajaeway-admin` with no arguments — the
 * first thing anyone types to discover the interface — blocked until the
 * caller's timeout. Booting is now an explicit verb, exactly as it is for
 * `gajaeway-gateway daemon`, and everything else is a usage error.
 */

export const ADMIN_USAGE = "usage: gajaeway-admin serve";

/** Usage errors exit 2, as `gajaeway-gateway` does; 1 stays a runtime failure. */
export const USAGE_EXIT_CODE = 2;

/** The usage text when `args` does not ask for the server, undefined when it does. */
export function usageFor(args: readonly string[]): string | undefined {
	return args.length === 1 && args[0] === "serve" ? undefined : ADMIN_USAGE;
}
