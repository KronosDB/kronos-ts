// ---------------------------------------------------------------------------
// A SUBSCRIPTION — a live read of the log, paced by the stream's window.
//
// `Subscribe` opens a stream the server fills as matching events become
// durable. The request says everything there is to say (the client sends
// nothing in the stream afterwards), and the server sends at most a window of
// bytes ahead of this reader. A frame's bytes are granted back when the consumer
// has TAKEN its events, not when the frame arrives — so the batches waiting here
// are bounded by the window, and a slow consumer holds back the server instead
// of growing a queue or being failed for it.
//
// Batches wait as the bytes they arrived in and an event is decoded at `next()`
// (see native-events.ts). A subscription whose criteria rarely match can ask for
// Progress frames: while nothing matches the server reports a position
// everything before which has been delivered, and `progress()` hands it over so
// a reader can advance its token through a quiet stretch.
// ---------------------------------------------------------------------------

import type { MessageStream, SequencedEvent, Serializer, StreamingCondition } from "@kronos-ts/core"
import { compileQuery, globalSequenceToken } from "@kronos-ts/core"
import type { NativeLink } from "./native-link.js"
import { criteriaToWire, rawBatch, writeCriteria, type RawBatch } from "./native-events.js"
import { contextStream, unexpected } from "./native-requests.js"
import type { NativeStream } from "./native-transport.js"
import {
  FLAG_END,
  FRAME_HEADER_LEN,
  FrameKind,
  decodeProgress,
  reader,
  writeLen16,
  writeStr16,
  writeU16,
  writeU32,
  writeU64,
} from "./protocol/kronosdb-protocol.js"

export type SubscriptionOptions = {
  /**
   * Ask for a Progress frame at most this often (milliseconds) while nothing
   * matches; 0 asks for none. Default: 0.
   */
  readonly progressIntervalMs?: number
  /** Event types to skip even when the query matches them. */
  readonly excludeNames?: ReadonlyArray<string>
}

/** A stream of sequenced events that also knows how far the server has read. */
export type ProgressingStream = MessageStream<SequencedEvent> & {
  /**
   * The latest position the server reported with everything matching before it
   * delivered, or undefined if it has sent none (or none was asked for).
   *
   * "Delivered" is by the time you ask: a Progress frame that arrived behind
   * events still waiting here is held back until `next()` has handed those
   * out, so the position is never ahead of what the reader has seen.
   */
  progress(): bigint | undefined
}

/** An Events frame kept as its bytes, and the window it will give back once spent. */
type Waiting = {
  readonly batch: RawBatch
  /** The frame's size on the wire; 0 for the frame that ends the stream, which spends no window. */
  readonly bytes: number
}

export function subscription(
  link: NativeLink,
  serializer: Serializer,
  context: string,
  condition: StreamingCondition,
  options: SubscriptionOptions = {},
): ProgressingStream {
  const criteria = condition.query ? criteriaToWire(compileQuery(condition.query)) : []
  const exclude = options.excludeNames ?? []
  const progressIntervalMs = options.progressIntervalMs ?? 0

  let batches: Waiting[] = []
  // Events across the waiting batches, kept so `hasNextAvailable` is a compare.
  let queued = 0
  let peeked: SequencedEvent | undefined
  let completed = false
  let streamError: Error | undefined
  let availableCallback: (() => void) | null = null
  // Progress the server reported waits here, with the number of events that had
  // arrived when it did, until that many have been taken: it says everything
  // matching before it was DELIVERED, and the reader must not move its token
  // past an event still sitting in this queue.
  let held: { readonly position: bigint; readonly after: number }[] = []
  let settled: bigint | undefined
  let received = 0
  let taken = 0
  let stream: NativeStream | undefined
  // Window the consumer has used that the stream could not take yet, because
  // the transport had not handed it over: given back as soon as it does.
  let unreported = 0

  /** The consumer took a frame of `bytes`: the stream may send that much more. */
  function release(bytes: number): void {
    if (bytes === 0) return
    if (stream === undefined) unreported += bytes
    else stream.consumed(bytes)
  }

  function finish(error?: Error): void {
    if (completed) return
    if (error !== undefined) streamError = error
    completed = true
    availableCallback?.()
  }

  function discard(): void {
    batches = []
    queued = 0
    peeked = undefined
    held = []
  }

  /** The latest reported position whose events the reader has all taken. */
  function reached(): bigint | undefined {
    while (held.length > 0 && held[0]!.after <= taken) settled = held.shift()!.position
    return settled
  }

  function pull(): SequencedEvent | undefined {
    try {
      // Batches are few (the window bounds them), so dropping a spent one off
      // the front is cheap.
      while (batches.length > 0) {
        const front = batches[0]!
        const item = front.batch.take()
        if (item !== undefined) {
          queued--
          // The last event of a frame is the moment its bytes are given back.
          if (front.batch.remaining() === 0) {
            batches.shift()
            release(front.bytes)
          }
          return item
        }
        batches.shift()
        release(front.bytes)
      }
      return undefined
    } catch (error) {
      // Bytes the server sent that this client cannot read end the subscription.
      discard()
      stream?.cancel()
      finish(error instanceof Error ? error : new Error(String(error)))
      return undefined
    }
  }

  function onFrame(kind: number, flags: number, buf: Uint8Array, start: number, end: number): void {
    if (completed) return
    // What the frame costs the window: all of it, except the frame that ends the stream.
    const bytes = (flags & FLAG_END) !== 0 ? 0 : FRAME_HEADER_LEN + end - start
    if (kind === FrameKind.Events) {
      const next = rawBatch(buf, start, end, serializer)
      const count = next.remaining()
      if (count > 0) {
        batches.push({ batch: next, bytes })
        queued += count
        received += count
        availableCallback?.()
      } else {
        release(bytes)
      }
    } else if (kind === FrameKind.Progress) {
      held.push({ position: decodeProgress(reader(buf, start, end)).position, after: received })
      // A position holds no events, so nothing is waiting on this frame: its
      // few bytes go back at once rather than whenever somebody asks `progress()`.
      release(bytes)
      // A reader that has taken everything is idle and would never look.
      availableCallback?.()
    } else {
      throw unexpected(kind, "Events or Progress")
    }
    if ((flags & FLAG_END) !== 0) finish()
  }

  contextStream(
    link,
    context,
    FrameKind.Subscribe,
    (w, id) => {
      writeU16(w, id)
      writeU64(w, condition.position < 0n ? 0n : condition.position)
      writeCriteria(w, criteria)
      writeLen16(w, exclude.length, "excluded names")
      for (const name of exclude) writeStr16(w, name, "excluded name")
      // Events per frame: 0 lets the server choose.
      writeU32(w, 0)
      writeU32(w, progressIntervalMs)
    },
    // The request carries END: a subscription has nothing to say in its stream,
    // only Window and Cancel about it. Frames are consumed as `next()` takes them.
    true,
    onFrame,
    (error) => finish(error),
    { manualWindow: true },
  ).then(
    (opened) => {
      if (completed) {
        opened.cancel()
        return
      }
      stream = opened
      if (unreported > 0) {
        opened.consumed(unreported)
        unreported = 0
      }
    },
    (error) => finish(error instanceof Error ? error : new Error(String(error))),
  )

  return {
    next() {
      let item = peeked
      if (item === undefined) item = pull()
      else peeked = undefined
      if (item !== undefined) taken++
      return item
    },

    peek() {
      peeked ??= pull()
      return peeked
    },

    hasNextAvailable() {
      return peeked !== undefined || queued > 0
    },

    isCompleted() {
      return completed && peeked === undefined && queued === 0
    },

    error() {
      return streamError
    },

    setCallback(callback: () => void) {
      availableCallback = callback
    },

    close() {
      completed = true
      stream?.cancel()
      stream = undefined
      discard()
      availableCallback = null
    },

    progress: reached,

    progressToken() {
      const position = reached()
      return position === undefined ? undefined : globalSequenceToken(position)
    },
  }
}
