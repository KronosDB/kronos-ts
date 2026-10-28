// ---------------------------------------------------------------------------
// A SUBSCRIPTION — a live read of the log, with credits.
//
// `Subscribe` opens a stream the server fills as matching events become
// durable. It never sends more events than it has been granted credits for, and
// a credit is granted back when the consumer TAKES an event, not when it
// arrives — so the batches waiting here are bounded by what was granted, and a
// slow consumer holds back the server instead of growing a queue.
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
  /** Events the server may send ahead of the consumer. Default: 500. */
  readonly credits?: number
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

export const DEFAULT_CREDITS = 500
const REFILL_THRESHOLD = 0.25

export function subscription(
  link: NativeLink,
  serializer: Serializer,
  context: string,
  condition: StreamingCondition,
  options: SubscriptionOptions = {},
): ProgressingStream {
  const credits = options.credits ?? DEFAULT_CREDITS
  if (!Number.isInteger(credits) || credits < 1 || credits > 0xffffffff) throw new RangeError("credits must be a positive integer")
  const criteria = condition.query ? criteriaToWire(compileQuery(condition.query)) : []
  const exclude = options.excludeNames ?? []
  const progressIntervalMs = options.progressIntervalMs ?? 0

  let batches: RawBatch[] = []
  // Events across the waiting batches, kept so `hasNextAvailable` is a compare.
  let queued = 0
  let peeked: SequencedEvent | undefined
  let completed = false
  let streamError: Error | undefined
  let availableCallback: (() => void) | null = null
  let outstanding = credits
  // Progress the server reported waits here, with the number of events that had
  // arrived when it did, until that many have been taken: it says everything
  // matching before it was DELIVERED, and the reader must not move its token
  // past an event still sitting in this queue.
  let held: { readonly position: bigint; readonly after: number }[] = []
  let settled: bigint | undefined
  let received = 0
  let taken = 0
  let stream: NativeStream | undefined

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
      // Batches are few (credits bound them), so dropping a spent one off the
      // front is cheap.
      while (batches.length > 0) {
        const item = batches[0]!.take()
        if (item !== undefined) {
          queued--
          return item
        }
        batches.shift()
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
    if (kind === FrameKind.Events) {
      const next = rawBatch(buf, start, end, serializer)
      const count = next.remaining()
      if (count > 0) {
        batches.push(next)
        queued += count
        received += count
        availableCallback?.()
      }
    } else if (kind === FrameKind.Progress) {
      held.push({ position: decodeProgress(reader(buf, start, end)).position, after: received })
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
      writeU32(w, credits)
      // Events per frame: 0 lets the server choose. It never sends more than
      // the credits outstanding, so the batch cap is min(its default, credits).
      writeU32(w, 0)
      writeU32(w, progressIntervalMs)
    },
    false,
    onFrame,
    (error) => finish(error),
  ).then(
    (opened) => {
      if (completed) opened.cancel()
      else stream = opened
    },
    (error) => finish(error instanceof Error ? error : new Error(String(error))),
  )

  function onConsumed(): void {
    outstanding--
    if (outstanding <= Math.floor(credits * REFILL_THRESHOLD) && !completed) {
      const grant = credits - outstanding
      outstanding += grant
      stream?.send(FrameKind.Credit, (w) => writeU32(w, grant))
    }
  }

  return {
    next() {
      let item = peeked
      if (item === undefined) item = pull()
      else peeked = undefined
      if (item !== undefined) {
        taken++
        onConsumed()
      }
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
