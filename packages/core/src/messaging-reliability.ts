/** Limits reject excess work immediately: queued children must never wait for parents' slots. */
export type MessagingLimits = {
  /** Per bus/transport. Default: 128. Includes handlers still running after caller timeout. */
  maxConcurrentHandlers?: number
  /** Per bus/transport. Default: 1024. */
  maxPendingRequests?: number
  /** Optional diagnostics observer. Observer failures cannot fail messaging. */
  observe?: (snapshot: MessagingActivity) => void
}

export type MessagingActivity = {
  name: string
  active: number
  peak: number
  accepted: number
  rejected: number
  completed: number
  limit: number
}

export class MessagingOverloadedError extends Error {
  constructor(name: string, limit: number) {
    super(`${name} overloaded: capacity ${limit} exhausted`)
    this.name = "MessagingOverloadedError"
  }
}

export function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0)
    throw new RangeError(`${name} must be a positive safe integer`)
  return value
}

/** A counter, deliberately without a waiting queue or completion-based receive credits. */
export function messagingAdmission(
  name: string,
  limit: number,
  observe?: MessagingLimits["observe"],
) {
  positiveInteger(limit, name)
  const identifiers = new Set<string>()
  const state: MessagingActivity = {
    name,
    limit,
    active: 0,
    peak: 0,
    accepted: 0,
    rejected: 0,
    completed: 0,
  }
  function report() {
    try {
      observe?.({ ...state })
    } catch {
      /* Diagnostics must not change delivery. */
    }
  }
  return {
    get snapshot(): MessagingActivity {
      return { ...state }
    },
    enter(identifier?: string): { end(): void } {
      if (identifier !== undefined && identifiers.has(identifier)) {
        state.rejected++
        report()
        throw new Error(`Request ${identifier} is already pending`)
      }
      if (state.active >= limit) {
        state.rejected++
        report()
        throw new MessagingOverloadedError(name, limit)
      }
      if (identifier !== undefined) identifiers.add(identifier)
      state.active++
      state.accepted++
      state.peak = Math.max(state.peak, state.active)
      report()
      let ended = false
      return {
        end() {
          if (ended) return
          ended = true
          if (identifier !== undefined) identifiers.delete(identifier)
          state.active--
          state.completed++
          report()
        },
      }
    },
  }
}

/** Bound an await without losing observation of a late rejection. Does not cancel user code. */
export async function withMessagingTimeout<T>(
  work: PromiseLike<T>,
  timeoutMs: number,
  description: string,
): Promise<T> {
  positiveInteger(timeoutMs, "timeoutMs")
  if (timeoutMs > 2_147_483_647) throw new RangeError("timeoutMs exceeds the timer range")
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${description} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        )
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

/** A real gRPC cancellation signal, independent of server processing instructions. */
export function messagingDeadline(timeoutMs: number): {
  signal: AbortSignal
  cancel(): void
  close(): void
} {
  positiveInteger(timeoutMs, "timeoutMs")
  if (timeoutMs > 2_147_483_647) throw new RangeError("timeoutMs exceeds the timer range")
  const controller = new AbortController()
  const timer = setTimeout(
    () => controller.abort(new Error(`Messaging deadline exceeded after ${timeoutMs}ms`)),
    timeoutMs,
  )
  return {
    signal: controller.signal,
    cancel() {
      controller.abort()
    },
    close() {
      clearTimeout(timer)
    },
  }
}
