/** The POSIX error names a server can return. Any other `code` is a protocol error name. */
export const POSIX_ERRNOS = [
  "EACCES", "EAGAIN", "EBADF", "EBUSY", "EDQUOT", "EEXIST", "EFBIG", "EINVAL", "EIO", "EISDIR",
  "ELOOP", "EMLINK", "ENAMETOOLONG", "ENOENT", "ENOSPC", "ENOTDIR", "ENOTEMPTY", "EPERM", "EROFS",
  "ESTALE", "EXDEV",
] as const;

/** Error returned by the server. `code` is a POSIX name such as ENOENT. */
export class MountosError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message ? `${code}: ${message}` : code);
    this.name = "MountosError";
    this.code = code;
  }
}

/** The peer broke the wire protocol. The connection is closed. */
export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

/** The socket failed or closed. */
export class TransportError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "TransportError";
  }
}

/**
 * A call did not finish within Options.callTimeout. `code` is DEADLINE_EXCEEDED. A request that
 * was already sent ends its connection, so a late reply cannot reach another call.
 */
export class DeadlineExceededError extends Error {
  readonly code = "DEADLINE_EXCEEDED";
  constructor(message: string) {
    super(message);
    this.name = "DeadlineExceededError";
  }
}

/**
 * A directory helper hit its bound. `readDirAll` found more than `limit` entries, or `walk` went
 * deeper than `limit` levels below its root. `code` is TOO_MANY_ENTRIES or WALK_TOO_DEEP.
 */
export class TooManyEntriesError extends Error {
  constructor(
    readonly code: "TOO_MANY_ENTRIES" | "WALK_TOO_DEEP",
    readonly path: string,
    readonly limit: number,
  ) {
    super(
      code === "TOO_MANY_ENTRIES"
        ? `${path} has more than ${limit} entries`
        : `${path} is deeper than ${limit} levels`,
    );
    this.name = "TooManyEntriesError";
  }
}
