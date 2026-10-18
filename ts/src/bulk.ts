import type { FileInfo } from "./api.js";
import { MountosError, ProtocolError } from "./errors.js";
import * as pb from "./gen/mountos_fs.js";

/** Most items the server accepts in one batch request. */
export const MAX_BATCH_ITEMS = 4096;

/** Result of one path in statBatch. Exactly one field is set. */
export interface StatResult {
  info?: FileInfo;
  error?: MountosError;
}

/** A byte range to read. */
export interface ReadRange {
  offset: number;
  length: number;
}

/** Result of one range in readBatch. Exactly one field is set. */
export interface ReadResult {
  data?: Buffer;
  error?: MountosError;
}

/** Data to write at an offset. */
export interface WriteItem {
  offset: number;
  data: Uint8Array;
}

/** Result of one item in writeBatch. Exactly one field is set. */
export interface WriteResult {
  written?: number;
  error?: MountosError;
}

/** Maps request positions to errors. A position outside the request is a protocol error. */
export function itemErrors(errors: pb.BatchItemError[], n: number): Map<number, MountosError> {
  const m = new Map<number, MountosError>();
  for (const e of errors) {
    if (e.index < 0 || e.index >= n) throw new ProtocolError(`batch error index ${e.index} outside ${n} items`);
    m.set(e.index, new MountosError(e.code || "EIO", e.message));
  }
  return m;
}

/** A slice of one caller item. src is the offset of the slice inside the item. */
export interface Piece {
  item: number;
  src: number;
  offset: number;
  length: number;
}

/**
 * Cuts items into pieces of at most maxPayload bytes and groups the pieces into requests.
 * A group holds at most MAX_BATCH_ITEMS pieces and at most maxPayload bytes in total.
 * A zero length item yields one empty piece so it still gets a result.
 */
export function planPieces(items: { offset: number; length: number }[], maxPayload: number): Piece[][] {
  const groups: Piece[][] = [];
  let cur: Piece[] = [];
  let bytes = 0;
  const push = (p: Piece): void => {
    if (cur.length === MAX_BATCH_ITEMS || bytes + p.length > maxPayload) {
      groups.push(cur);
      cur = [];
      bytes = 0;
    }
    cur.push(p);
    bytes += p.length;
  };
  items.forEach((it, item) => {
    if (!Number.isSafeInteger(it.offset) || it.offset < 0) throw new RangeError(`item ${item}: invalid offset`);
    if (!Number.isSafeInteger(it.length) || it.length < 0) throw new RangeError(`item ${item}: invalid length`);
    let src = 0;
    do {
      const length = Math.min(it.length - src, maxPayload);
      push({ item, src, offset: it.offset + src, length });
      src += length;
    } while (src < it.length);
  });
  if (cur.length > 0) groups.push(cur);
  return groups;
}
