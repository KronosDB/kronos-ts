/**
 * A read that never answers is cancelled and asked once more.
 *
 * WHY THIS EXISTS. Under Bun, a gRPC call whose response has fully arrived —
 * headers, the message, trailers carrying an OK status, the HTTP/2 stream
 * closed cleanly — can still never complete on the client. grpc-js releases
 * an OK status only once the stream's `end` event has fired, and Bun's http2
 * client intermittently never emits it (Node does). The Axon integration
 * suites hit it about one run in four on a loaded machine, on `source` and on
 * the snapshot store's `getLast`; the command handler above the read then
 * never replies, and Axon Server cancels the command at its own 300 s timeout.
 *
 * A deadline is the one lever this side of grpc-js: a cancelled call DOES
 * surface, because a non-OK status needs no `end`. The reads this guards are
 * idempotent, so the answer to a lost one is simply to ask again. A second
 * loss is reported rather than retried forever.
 */
export async function boundedRead<T>(
  deadlineMs: number,
  read: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    const cancel = new AbortController()
    const timer = setTimeout(() => cancel.abort(), deadlineMs)
    try {
      return await read(cancel.signal)
    } catch (err) {
      if (!cancel.signal.aborted) throw err
      if (attempt === 2) {
        throw new Error(
          `Axon Server read did not complete within ${deadlineMs} ms, twice in a row`,
          { cause: err },
        )
      }
      console.warn(
        `Axon Server read did not complete within ${deadlineMs} ms; cancelled it and asking again ` +
          "(a known Bun http2 client fault: the stream's end event is lost, see bounded-read.ts)",
      )
    } finally {
      clearTimeout(timer)
    }
  }
}
