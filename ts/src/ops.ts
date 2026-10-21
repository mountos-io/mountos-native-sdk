import { OperationStateUnknownError, ProtocolError } from "./errors.js";
import type { OpCounts } from "./events.js";
import * as pb from "./gen/mountos_fs.js";

/** Most failures an Operation keeps. The server sends at most this many per operation. */
export const MAX_OP_FAILURES = 64;

/** Extra properties Clone and CloneDir copy from the source. */
export type PreserveKind = "times" | "xattr" | "acl";

/** What Clone did with the destination. */
export type CloneOutcome = "cloned" | "overwritten" | "skipped";

export interface CloneOptions {
  /** Replace the content of a destination that differs. Without it such a destination fails with EEXIST. */
  overwrite?: boolean;
  /** Properties to copy besides data, mode and owner. */
  preserve?: PreserveKind[];
}

/** One entry that failed inside an operation. path is relative to the mount root, and empty when the name is not valid UTF-8. */
export interface OpFailure {
  path: string;
  code: string;
  message: string;
}

export interface OpResult {
  counts: OpCounts;
  /** At most MAX_OP_FAILURES, in the order the server reported them. */
  failures: OpFailure[];
}

export interface WaitOptions {
  signal?: AbortSignal;
}

const PRESERVE: Record<PreserveKind, pb.PreserveKind> = {
  times: pb.PreserveKind.PRESERVE_KIND_TIMES,
  xattr: pb.PreserveKind.PRESERVE_KIND_XATTR,
  acl: pb.PreserveKind.PRESERVE_KIND_ACL,
};

const OUTCOMES: Partial<Record<number, CloneOutcome>> = {
  [pb.CloneOutcome.CLONE_OUTCOME_CLONED]: "cloned",
  [pb.CloneOutcome.CLONE_OUTCOME_OVERWRITTEN]: "overwritten",
  [pb.CloneOutcome.CLONE_OUTCOME_SKIPPED]: "skipped",
};

/** Builds the wire options. Throws RangeError for a preserve name this SDK does not know. */
export function toWireOptions(o: CloneOptions | undefined): pb.CloneOptions | undefined {
  if (!o) return undefined;
  const preserve = (o.preserve ?? []).map((p) => {
    const v = PRESERVE[p] as pb.PreserveKind | undefined;
    if (v === undefined) throw new RangeError(`unknown preserve kind ${JSON.stringify(p)}`);
    return v;
  });
  return { overwrite: o.overwrite ?? false, preserve };
}

/** Maps a wire outcome. Throws ProtocolError for a value this SDK does not know. */
export function fromWireOutcome(o: number): CloneOutcome {
  const v = OUTCOMES[o];
  if (v === undefined) throw new ProtocolError(`unknown clone outcome ${o}`);
  return v;
}

interface Waiter {
  resolve: (r: OpResult) => void;
  reject: (e: Error) => void;
}

/**
 * A running or ended cloneDir or removeDir. It lives on the connection that started it. The
 * client feeds it from notices. wait() settles once, with the result or the reason it cannot know.
 */
export class Operation {
  private readonly failures: OpFailure[] = [];
  private readonly waiters = new Set<Waiter>();
  private outcome: { result: OpResult } | { error: Error } | undefined;

  /** @internal Built by the client. */
  constructor(
    /** Operation id the server gave. It matches ClientEvent.refId. */
    readonly id: bigint,
    private readonly sendCancel: () => Promise<void>,
  ) {}

  /** True once wait() would settle at once. */
  get settled(): boolean {
    return this.outcome !== undefined;
  }

  /**
   * Waits for the end of the operation. Resolves when the server reports the end, with the totals
   * and the failures. Rejects with OperationStateUnknownError when notices were lost, with
   * TransportError when the connection dropped or the client closed, and with the signal's reason
   * on abort. It may be called again and from several places.
   */
  wait(opts: WaitOptions = {}): Promise<OpResult> {
    const { signal } = opts;
    if (this.outcome) {
      return "error" in this.outcome ? Promise.reject(this.outcome.error) : Promise.resolve(this.outcome.result);
    }
    // An abort reason is caller supplied and may be any value, so it is passed through unchanged.
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
    if (signal?.aborted) return Promise.reject(signal.reason);
    return new Promise<OpResult>((resolve, reject) => {
      const onAbort = (): void => {
        this.waiters.delete(waiter);
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
        reject(signal!.reason);
      };
      const waiter: Waiter = {
        resolve: (r) => {
          signal?.removeEventListener("abort", onAbort);
          resolve(r);
        },
        reject: (e) => {
          signal?.removeEventListener("abort", onAbort);
          reject(e);
        },
      };
      this.waiters.add(waiter);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  /**
   * Asks the server to stop the operation. It still ends with an opDone notice, so call wait() for
   * the partial totals. Rejects with ENOENT when the operation already ended.
   */
  cancel(): Promise<void> {
    return this.sendCancel();
  }

  /** @internal */
  addFailure(f: OpFailure): void {
    if (!this.outcome && this.failures.length < MAX_OP_FAILURES) this.failures.push(f);
  }

  /** @internal */
  finish(counts: OpCounts): void {
    this.settle({ result: { counts, failures: this.failures } });
  }

  /** @internal */
  fail(error: Error): void {
    this.settle({ error });
  }

  /** @internal */
  unknown(): void {
    this.fail(new OperationStateUnknownError());
  }

  private settle(outcome: { result: OpResult } | { error: Error }): void {
    if (this.outcome) return;
    this.outcome = outcome;
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const w of waiters) {
      if ("error" in outcome) w.reject(outcome.error);
      else w.resolve(outcome.result);
    }
  }
}
