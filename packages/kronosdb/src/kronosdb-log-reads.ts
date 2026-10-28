// ---------------------------------------------------------------------------
// WHAT THE NATIVE LOG ANSWERS BEYOND THE `EventStore` SHAPE.
//
// The framework's `EventStore` asks a log to source, append, stream and say
// where its head is. The native protocol answers four more things, and none of
// them belongs in that shape — a Postgres log has no Progress frame and no
// notion of "first position still stored" — so they are functions beside the
// store, each of ALL its real arguments, for the host that wants them.
// ---------------------------------------------------------------------------

import type { StreamingCondition } from "@kronos-ts/core"
import type { KronosDbConnectionHandle } from "./kronosdb.js"
import { subscription, type ProgressingStream, type SubscriptionOptions } from "./kronosdb-subscription.js"
import { contextRequest, unexpected } from "./native-requests.js"
import {
  FrameKind,
  decodePositionAt,
  decodeTags,
  decodeTail,
  reader,
  writeMillis,
  writeU16,
  writeU64,
} from "./protocol/kronosdb-protocol.js"

type Kdb = Pick<KronosDbConnectionHandle, "connection" | "serializer">

/**
 * A live read of `context`'s log that can also report progress.
 *
 * `kronosDbEventStore(...).open(condition)` is this with the defaults. Pass
 * `progressIntervalMs` and, while no event matches the query, the server keeps
 * reporting how far it has looked — `progress()` is that position, everything
 * matching before it already delivered — so a selective reader can advance its
 * token through a quiet stretch instead of re-reading it after a restart.
 */
export function kronosDbSubscription(
  kdb: Kdb,
  context: string,
  condition: StreamingCondition,
  options: SubscriptionOptions = {},
): ProgressingStream {
  return subscription(kdb.connection.native, kdb.serializer, context, condition, options)
}

/** The first position `context`'s log still stores — above 0 once old segments have been dropped. */
export function kronosDbTail(kdb: Kdb, context: string): Promise<bigint> {
  return contextRequest(
    kdb.connection.native,
    context,
    FrameKind.GetTail,
    (w, id) => writeU16(w, id),
    (kind, buf, start, end) => {
      if (kind !== FrameKind.Tail) throw unexpected(kind, "Tail")
      return decodeTail(reader(buf, start, end)).position
    },
  )
}

/** The tags of the event at `position`, as text. */
export function kronosDbTagsAt(
  kdb: Kdb,
  context: string,
  position: bigint,
): Promise<ReadonlyArray<{ readonly key: string; readonly value: string }>> {
  const text = new TextDecoder()
  return contextRequest(
    kdb.connection.native,
    context,
    FrameKind.GetTags,
    (w, id) => {
      writeU16(w, id)
      writeU64(w, position)
    },
    (kind, buf, start, end) => {
      if (kind !== FrameKind.Tags) throw unexpected(kind, "Tags")
      return decodeTags(reader(buf, start, end)).tags.map((tag) => ({
        key: text.decode(tag.key),
        value: text.decode(tag.value),
      }))
    },
  )
}

/**
 * The position of the first event at or after `at` (a Date, or epoch
 * milliseconds), or undefined when no event is that recent.
 */
export function kronosDbPositionAt(kdb: Kdb, context: string, at: Date | number): Promise<bigint | undefined> {
  const millis = typeof at === "number" ? at : at.getTime()
  return contextRequest(
    kdb.connection.native,
    context,
    FrameKind.GetPositionAt,
    (w, id) => {
      writeU16(w, id)
      writeMillis(w, millis)
    },
    (kind, buf, start, end) => {
      if (kind !== FrameKind.PositionAt) throw unexpected(kind, "PositionAt")
      return decodePositionAt(reader(buf, start, end)).position
    },
  )
}
