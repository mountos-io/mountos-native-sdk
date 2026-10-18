import { MountosError, ProtocolError } from "./errors.js";
import * as pb from "./gen/mountos_fs.js";
import { dec } from "./api.js";

export type ClientEventKind = "warmDone" | "warmFailed" | "change" | "overflow" | "closing" | "connectionLost";
export type ChangeKind = "create" | "remove" | "write" | "rename" | "attrib";

/**
 * A server notice or a local connection event.
 * refId is the warm id for warm events and the watch id for change and overflow events.
 * An overflow event means notices were dropped, so list the watched path again. Its refId is 0
 * when the loss is not tied to one watch. error is a MountosError for warmFailed and a plain
 * Error for connectionLost. For a rename, path is the new name and oldPath the old one.
 */
export interface ClientEvent {
  kind: ClientEventKind;
  sequence: bigint;
  refId: bigint;
  path: string;
  oldPath: string;
  change?: ChangeKind;
  error?: Error;
  time: Date;
}

/** Receives the SDK's own diagnostics. The console satisfies it. */
export interface Logger {
  warn(message: string): void;
}

const KINDS: Partial<Record<number, ClientEventKind>> = {
  [pb.NoticeKind.NOTICE_KIND_WARM_DONE]: "warmDone",
  [pb.NoticeKind.NOTICE_KIND_WARM_FAILED]: "warmFailed",
  [pb.NoticeKind.NOTICE_KIND_CHANGE]: "change",
  [pb.NoticeKind.NOTICE_KIND_OVERFLOW]: "overflow",
  [pb.NoticeKind.NOTICE_KIND_CLOSING]: "closing",
};

const CHANGES: Partial<Record<number, ChangeKind>> = {
  [pb.WatchEventKind.WATCH_EVENT_KIND_CREATE]: "create",
  [pb.WatchEventKind.WATCH_EVENT_KIND_REMOVE]: "remove",
  [pb.WatchEventKind.WATCH_EVENT_KIND_WRITE]: "write",
  [pb.WatchEventKind.WATCH_EVENT_KIND_RENAME]: "rename",
  [pb.WatchEventKind.WATCH_EVENT_KIND_ATTRIB]: "attrib",
};

/**
 * Decodes a Notice message. Returns undefined for a notice kind or change kind this SDK does not
 * know, so a newer server does not break the connection. Bad protobuf is a ProtocolError.
 */
export function decodeNotice(meta: Buffer): ClientEvent | undefined {
  const m = dec(pb.Notice, meta);
  const kind = KINDS[m.kind];
  if (!kind) return undefined;
  const e: ClientEvent = {
    kind, sequence: m.sequence, refId: m.refId, path: m.path, oldPath: m.oldPath, time: new Date(),
  };
  if (kind === "warmFailed") e.error = new MountosError(m.code || "EIO", m.message);
  if (kind === "change") {
    if (m.changeKind === pb.WatchEventKind.WATCH_EVENT_KIND_OVERFLOW) {
      e.kind = "overflow";
      return e;
    }
    const change = CHANGES[m.changeKind];
    if (!change) return undefined;
    e.change = change;
  }
  return e;
}

/** One line summary for the log. */
export function describeEvent(e: ClientEvent): string {
  const err = e.error ? ` error=${e.error.message}` : "";
  return `kind=${e.kind} refId=${e.refId} path=${JSON.stringify(e.path)}${err}`;
}
