import { type Conn } from "./conn.js";
import { MountosError, TransportError } from "./errors.js";
import * as pb from "./gen/mountos_fs.js";
import { enc } from "./api.js";
import { type ClientEvent } from "./events.js";
import { Method } from "./ops_gen.js";

export interface WatchOptions {
  /** Also watch everything below the path. */
  recursive?: boolean;
  /** Replay request. The server does not store events, so an older sequence gives one overflow event. */
  since?: bigint;
}

/** Events a slow consumer may leave unread. */
export const WATCH_QUEUE_LIMIT = 256;

/**
 * A live watch. It sits on one pooled connection and reads that connection's notices.
 * Iterate it with for await to get the change and overflow events of this watch, in order.
 * Leaving the loop early closes the watch. The iteration ends after close(), after the client
 * closes, or when the owning connection ends.
 * A consumer that falls 256 events behind loses the oldest ones. An overflow event then marks
 * the gap, and the caller must list the watched path again.
 */
export class Watch implements AsyncIterable<ClientEvent> {
  private queue: ClientEvent[] = [];
  private waiters: ((r: IteratorResult<ClientEvent>) => void)[] = [];
  private done = false;
  private closing: Promise<void> | undefined;

  /** @internal Use Client.watch. */
  constructor(
    /** Server watch id. */
    readonly id: bigint,
    private conn: Conn,
    private release: (w: Watch) => void,
  ) {}

  /** @internal True when the event belongs to this watch. */
  accepts(conn: Conn, e: ClientEvent): boolean {
    if (this.done || conn !== this.conn) return false;
    if (e.kind === "change") return e.refId === this.id;
    return e.kind === "overflow" && (e.refId === this.id || e.refId === 0n);
  }

  /** @internal */
  ownedBy(conn: Conn): boolean {
    return conn === this.conn;
  }

  /** @internal */
  push(e: ClientEvent): void {
    const w = this.waiters.shift();
    if (w) {
      w({ value: e, done: false });
      return;
    }
    if (this.queue.length >= WATCH_QUEUE_LIMIT) {
      const gap: ClientEvent = {
        kind: "overflow", sequence: e.sequence, refId: this.id, path: "", oldPath: "", time: new Date(),
      };
      this.queue.splice(0, 2, gap);
    }
    this.queue.push(e);
  }

  [Symbol.asyncIterator](): AsyncIterator<ClientEvent> {
    return {
      next: () => this.next(),
      return: async () => {
        await this.close();
        return { value: undefined, done: true };
      },
    };
  }

  private next(): Promise<IteratorResult<ClientEvent>> {
    const e = this.queue.shift();
    if (e) return Promise.resolve({ value: e, done: false });
    if (this.done) return Promise.resolve({ value: undefined, done: true });
    return new Promise((r) => this.waiters.push(r));
  }

  /** @internal Ends the iteration without contacting the server. Queued events stay readable. */
  end(): void {
    if (this.done) return;
    this.done = true;
    for (const w of this.waiters.splice(0)) w({ value: undefined, done: true });
    this.release(this);
  }

  /**
   * Stops the watch. Sends unwatch on the owning connection. A connection that is gone, or a
   * watch the server already dropped (EBADF), is not an error. Calling it again returns the same promise.
   */
  close(): Promise<void> {
    this.closing ??= this.shutdown();
    return this.closing;
  }

  private async shutdown(): Promise<void> {
    this.end();
    this.queue = [];
    if (!this.conn.alive) return;
    try {
      await this.conn.call(Method.Unwatch, enc(pb.UnwatchRequest, { watchId: this.id }));
    } catch (e) {
      if (e instanceof TransportError) return;
      if (e instanceof MountosError && e.code === "EBADF") return;
      throw e;
    }
  }
}
