import type { Channel, ChannelModel } from "amqplib"

/** Establishes a raw AMQP connection. Swapped for a fake in tests. */
export type AmqpConnect = (url: string) => Promise<ChannelModel>

/**
 * A lazily-established AMQP connection, shared by the command and query
 * transports so a Kronos process opens a single connection to the broker.
 */
export interface AmqpConnection {
  /**
   * Open a fresh channel on the (lazily established) connection. Each transport
   * takes its own channel so prefetch and consumer state stay isolated.
   */
  channel(): Promise<Channel>
  /** Close the underlying connection. Idempotent — safe to call more than once. */
  close(): Promise<void>
}

async function defaultAmqpConnect(url: string): Promise<ChannelModel> {
  const amqp = (await import("amqplib")) as { connect(url: string): Promise<ChannelModel> }
  return amqp.connect(url)
}

/**
 * Create a shared AMQP connection. The connection opens on the first
 * `channel()` call and is reused for every channel thereafter.
 *
 * Ownership: whoever calls {@link createAmqpConnection} owns the connection and
 * is responsible for calling `close()`. Transports borrow channels and only
 * close their own channels.
 */
export function createAmqpConnection(
  url: string,
  connect: AmqpConnect = defaultAmqpConnect,
): AmqpConnection {
  let connection: Promise<ChannelModel> | undefined

  return {
    async channel() {
      connection ??= connect(url)
      return (await connection).createChannel()
    },
    async close() {
      const pending = connection
      connection = undefined
      if (pending) await (await pending).close().catch(() => {})
    },
  }
}
