import { MountosError } from "./errors.js";
import { Method } from "./ops_gen.js";

/** Capability that gates a method, from api/filesystem.yaml. Methods not listed need none. */
const METHOD_CAPABILITY: Partial<Record<number, string>> = {
  [Method.Ls]: "ls_stat",
  [Method.StatBatch]: "bulk_io",
  [Method.ReadBatch]: "bulk_io",
  [Method.WriteBatch]: "bulk_io",
  [Method.ReadFile]: "bulk_io",
  [Method.WriteFile]: "bulk_io",
  [Method.RemoveAll]: "remove_all",
  [Method.Watch]: "watch",
  [Method.WarmOpen]: "warm",
  [Method.WarmRead]: "warm",
};

/**
 * Throws UNSUPPORTED_OPERATION when the server reported capabilities and the method's one is
 * missing. A server that reported none is older and gets every call.
 */
export function requireCapability(offered: string[], method: number): void {
  const cap = METHOD_CAPABILITY[method];
  if (cap && offered.length > 0 && !offered.includes(cap)) {
    throw new MountosError("UNSUPPORTED_OPERATION", `server does not offer capability ${cap}`);
  }
}
