import { BinaryReader, BinaryWriter } from "@bufbuild/protobuf/wire";
import { MountosError, ProtocolError } from "./errors.js";
import * as pb from "./gen/mountos_fs.js";

export const OpenFlag = {
  ReadOnly: 0,
  WriteOnly: 1,
  ReadWrite: 2,
  Create: 64,
  Excl: 128,
  Trunc: 512,
  Append: 1024,
} as const;

export const FileType = { Regular: 0, Directory: 1, Symlink: 2 } as const;

/** File metadata. Sizes are numbers (exact below 2^53). Times and inode are bigint. */
export interface FileInfo {
  name: string;
  size: number;
  mode: number;
  type: number;
  mtimeNs: bigint;
  atimeNs: bigint;
  ctimeNs: bigint;
  uid: number;
  gid: number;
  nlink: number;
  inode: bigint;
}

export interface DirEntry {
  name: string;
  type: number;
  /** Present when the server sends metadata with the entry. Always set by ls. */
  info?: FileInfo;
}

export interface HelloResponse {
  version: number;
  capabilities: string[];
  maxPayloadBytes: number;
  maxInflight: number;
}

interface Codec<T> {
  encode(m: T, w?: BinaryWriter): BinaryWriter;
  decode(input: BinaryReader | Uint8Array, length?: number): T;
}

export function enc<T>(c: Codec<T>, m: T): Buffer {
  return Buffer.from(c.encode(m).finish());
}

/** Decodes a message. Malformed input is a protocol error. */
export function dec<T>(c: Codec<T>, meta: Buffer): T {
  try {
    return c.decode(meta);
  } catch (e) {
    throw new ProtocolError(`malformed protobuf: ${(e as Error).message}`);
  }
}

export function decWireError(meta: Buffer): MountosError {
  const w = dec(pb.WireError, meta);
  return new MountosError(w.code || "EIO", w.message);
}

export function decHello(meta: Buffer): HelloResponse {
  const r = dec(pb.HelloResponse, meta);
  return {
    version: r.version,
    capabilities: r.capabilities,
    maxPayloadBytes: r.maxPayloadBytes,
    maxInflight: r.maxInflight,
  };
}

export function toFileInfo(i: pb.FileInfo): FileInfo {
  return {
    name: i.name,
    size: Number(i.size),
    mode: i.mode,
    type: i.type,
    mtimeNs: i.mtimeNs,
    atimeNs: i.atimeNs,
    ctimeNs: i.ctimeNs,
    uid: i.uid,
    gid: i.gid,
    nlink: i.nlink,
    inode: i.inode,
  };
}

export function toDirEntry(e: pb.DirEntry): DirEntry {
  const d: DirEntry = { name: e.name, type: e.type };
  if (e.info) d.info = toFileInfo(e.info);
  return d;
}

/** Info from a Stat, Lstat or Fstat response. */
export function statInfo(info: pb.FileInfo | undefined): FileInfo {
  if (!info) throw new ProtocolError("stat response has no info");
  return toFileInfo(info);
}
