import { exception2Result, Result } from "../result.js";

export interface PeerStream<C = Uint8Array> {
  write: (chunk: C) => Promise<void>;
  cancel: () => Promise<void>;
  close: () => Promise<void>;
  // commit: () => Promise<void>;
}
export interface Peer<C = Uint8Array> {
  begin: () => Promise<Result<PeerStream<C>>>;
}

export interface TeeWriterOk {
  readonly peer: PeerStream;
  // readonly commit: () => Promise<void>;
}

export interface TeeWriterOptions {
  readonly peerTimeout?: number; // ms — if a peer op exceeds this, treat as failure
}

function withTimeout<T>(promise: Promise<T>, ms?: number): Promise<T> {
  if (ms == null) return promise;
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout((): void => reject(new Error(`peer timeout after ${ms}ms`)), ms);
    promise.then(
      (v) => {
        clearTimeout(timer);
        resolve(v);
      },
      (e: unknown) => {
        clearTimeout(timer);
        reject(e instanceof Error ? e : new Error(String(e)));
      },
    );
  });
}

function settledToError(r: PromiseSettledResult<Result<void>>): Error | null {
  if (r.status === "rejected") {
    return r.reason instanceof Error ? r.reason : new Error(String(r.reason));
  }
  if (r.value.isErr()) {
    return r.value.Err();
  }
  return null;
}

export async function teeWriter(
  peers: Peer[],
  inStream: ReadableStream<Uint8Array>,
  options?: TeeWriterOptions,
): Promise<Result<TeeWriterOk>> {
  const timeout = options?.peerTimeout;

  const beginResults = await Promise.allSettled(peers.map((p) => exception2Result(() => withTimeout(p.begin(), timeout))));
  const peerErrorByIndex = new Map<number, Error>();
  let activeStreams: PeerStream[] = [];
  let activeIndices: number[] = [];
  beginResults.forEach((r, i) => {
    if (r.status === "fulfilled" && r.value.isOk()) {
      activeStreams.push(r.value.Ok());
      activeIndices.push(i);
      return;
    }
    const err = r.status === "rejected" ? (r.reason instanceof Error ? r.reason : new Error(String(r.reason))) : r.value.Err();
    peerErrorByIndex.set(i, err);
  });

  const reader = inStream.getReader();
  while (activeStreams.length > 0) {
    const rRead = await exception2Result(() => reader.read());
    if (rRead.isErr()) {
      await Promise.allSettled(activeStreams.map((stream) => exception2Result(() => withTimeout(stream.cancel(), timeout))));
      return Result.Err(rRead.Err());
    }
    const { value, done } = rRead.Ok();
    if (done) break;
    const writeResults = await Promise.allSettled(
      activeStreams.map((stream) => exception2Result(() => withTimeout(stream.write(value), timeout))),
    );
    writeResults.forEach((r, j) => {
      const err = settledToError(r);
      if (err) peerErrorByIndex.set(activeIndices[j], err);
    });
    await Promise.allSettled(
      writeResults.flatMap((r, i) => {
        if (r.status === "rejected" || r.value.isErr())
          return exception2Result(() => withTimeout(activeStreams[i].cancel(), timeout));
        return [];
      }),
    );
    const survives = writeResults.map((r) => r.status === "fulfilled" && r.value.isOk());
    activeStreams = activeStreams.filter((_, i) => survives[i]);
    activeIndices = activeIndices.filter((_, i) => survives[i]);
  }

  if (activeStreams.length === 0) {
    // abort the input stream to stop any further processing
    await exception2Result(() => reader.cancel());
    const sorted = [...peerErrorByIndex.entries()].sort(([a], [b]) => a - b);
    const details = sorted.map(([i, e]) => `peer ${i}: ${e.message}`);
    const causes = sorted.map(([, e]) => e);
    const msg = details.length > 0 ? `all peers failed: [${details.join("; ")}]` : "all peers failed";
    return Result.Err(causes.length > 0 ? new Error(msg, { cause: causes }) : new Error(msg));
  }

  const closeErrors: Error[] = [];
  while (activeStreams.length >= 1) {
    const [winner, ...losers] = activeStreams;
    const rClose = await exception2Result(() => withTimeout(winner.close(), timeout));
    if (rClose.isOk()) {
      await Promise.allSettled(losers.map((stream) => exception2Result(() => withTimeout(stream.cancel(), timeout))));
      return Result.Ok({ peer: winner });
    }
    closeErrors.push(rClose.Err());
    // close failed — cancel this peer before moving on
    await exception2Result(() => withTimeout(winner.cancel(), timeout));
    activeStreams = losers;
  }
  const closeMsg =
    closeErrors.length > 0
      ? `all peers failed to close successfully: [${closeErrors.map((e) => e.message).join("; ")}]`
      : "all peers failed to close successfully";
  return Result.Err(closeErrors.length > 0 ? new Error(closeMsg, { cause: closeErrors }) : new Error(closeMsg));
}
