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
