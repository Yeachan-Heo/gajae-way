import rootPackage from "../../../package.json";

/** Bundled from the workspace root so the compiled app reports the release version. */
export const VERSION = rootPackage.version;
