import { Metadata } from "nice-grpc"
import type { AxonServerStoreSource } from "./connection.js"

/**
 * A context-scoped view of the ONE shared connection.
 *
 * Contexts are Axon Server's tenancy boundary, and they are a per-CALL header —
 * not a per-channel property. `AxonIQ-Context` selects the context on every
 * outbound stream and RPC, so `axonServerEventStore(conn, "tenant-a")` and
 * `axonServerEventStore(conn, "tenant-b")` are two views over one gRPC channel,
 * exactly as one connection served one context before. Nothing about the
 * channel, the codec or the drain latch differs between them; only this header
 * does, which is why the whole difference fits in one small record.
 */
export type AxonServerContextView = AxonServerStoreSource & {
  /** The Axon Server context every call through this view addresses. */
  readonly context: string
  /**
   * The gRPC metadata headers Axon Server requires. `AxonIQ-Context` is
   * mandatory (it identifies the context); `AxonIQ-Access-Token` is optional
   * auth. Both must be attached to every outbound stream/RPC — preserved
   * verbatim from the legacy enhancer.
   *
   * A function rather than a value so a caller that opens one long-lived stream
   * can hoist it, and one that makes many unary calls can hand each its own.
   */
  metadata(): Metadata
}

/** Scope a connection to one Axon Server context. */
export function contextView(source: AxonServerStoreSource, context: string): AxonServerContextView {
  const { connection, serializer } = source
  return {
    connection,
    serializer,
    context,
    metadata() {
      const metadata = new Metadata()
      metadata.set("AxonIQ-Context", context)
      if (connection.config.token) {
        metadata.set("AxonIQ-Access-Token", connection.config.token)
      }
      return metadata
    },
  }
}
