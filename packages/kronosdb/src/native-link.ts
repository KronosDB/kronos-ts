// ---------------------------------------------------------------------------
// THE NATIVE LINK — the connection's one socket, as a store, a bus or the
// platform stream sees it.
//
// A transport is one socket and dies with it. The link is what a store holds
// instead: "whatever socket is live right now", opened the first time anyone
// needs it and opened again after it dies. It owns three decisions the
// transport must not make for itself:
//
//   WHEN TO CONNECT. Lazily, the first time anything needs the socket, so
//   building a connection costs nothing and a process that never talks to
//   KronosDB never opens one.
//
//   WHICH ADDRESS. The configured servers in order, starting from the last one
//   that worked; a failed connect tries the next before giving up.
//
//   WHAT TO DO WITH A DRAINING SERVER. A transport that received GoAway keeps
//   serving its open streams and is skipped for new work, so the next request
//   opens a fresh socket — to the same address, which by then is the server's
//   replacement or another node behind the same name.
//
// Reconnection is deliberately NOT a loop in here. A request that finds the
// socket gone gets one fresh attempt (per address) and reports its failure; the
// caller's own retry — a processor's backoff, a command's resilience wrapper —
// decides whether to try again. Anything running longer than a request hears
// about the loss through `onClose`.
// ---------------------------------------------------------------------------

import { AuthenticationError, ConnectionFailedError } from "./errors.js"
import { openNativeTransport, type NativeTransport, type NativeTransportOptions } from "./native-transport.js"

export type NativeLinkOptions = {
  /** `host:port` of each server, in the order to try them. */
  readonly addresses: ReadonlyArray<string>
  readonly tls?: NativeTransportOptions["tls"]
  readonly clientName: string
  readonly clientId: string
  readonly token: string
  readonly connectTimeoutMs?: number
}

export type NativeLink = {
  /** The live transport, or undefined when there is none to use right now. */
  current(): NativeTransport | undefined
  /** The live transport, connecting first if there is none. */
  ready(): Promise<NativeTransport>
  /** Called with each transport once its handshake completes: the first, and every replacement. */
  onConnect(listener: (transport: NativeTransport) => void): void
  /** Called when a transport closes, with the reason unless the link asked for it. */
  onClose(listener: (error?: Error) => void): void
  /**
   * Make sure there is a live transport if there ever was one. A healthy socket
   * is left alone: it has its own liveness check, and a hiccup in one stream is
   * no reason to drop the others.
   */
  restart(): Promise<void>
  /** Close every socket. The link refuses to connect again. */
  close(): void
}

function splitAddress(address: string): { host: string; port: number } {
  const colon = address.lastIndexOf(":")
  if (colon < 0) throw new RangeError(`Server address '${address}' must be host:port`)
  const host = address.slice(0, colon).replace(/^\[(.*)\]$/, "$1")
  const port = Number(address.slice(colon + 1))
  if (!Number.isInteger(port) || port <= 0 || port > 65535) throw new RangeError(`Server address '${address}' has no valid port`)
  return { host, port }
}

export function nativeLink(options: NativeLinkOptions): NativeLink {
  const addresses = options.addresses.map(splitAddress)
  if (addresses.length === 0) throw new RangeError("A native link needs at least one server address")

  let transport: NativeTransport | undefined
  let connecting: Promise<NativeTransport> | undefined
  let closedForGood = false
  let everConnected = false
  let preferred = 0
  const open = new Set<NativeTransport>()
  const connectListeners: Array<(transport: NativeTransport) => void> = []
  const closeListeners: Array<(error?: Error) => void> = []

  function current(): NativeTransport | undefined {
    return transport !== undefined && !transport.closed && !transport.draining ? transport : undefined
  }

  function attach(opened: NativeTransport): void {
    transport = opened
    everConnected = true
    open.add(opened)
    opened.onClose((error) => {
      open.delete(opened)
      if (transport === opened) transport = undefined
      for (const listener of closeListeners) {
        try {
          listener(error)
        } catch {
          // Listener failures are isolated.
        }
      }
    })
    for (const listener of connectListeners) {
      try {
        listener(opened)
      } catch {
        // Listener failures are isolated.
      }
    }
  }

  async function connectOnce(): Promise<NativeTransport> {
    let last: unknown
    for (let attempt = 0; attempt < addresses.length; attempt++) {
      const index = (preferred + attempt) % addresses.length
      const { host, port } = addresses[index]!
      try {
        const opened = await openNativeTransport({
          host,
          port,
          tls: options.tls,
          clientName: options.clientName,
          clientId: options.clientId,
          token: options.token,
          connectTimeoutMs: options.connectTimeoutMs,
        })
        if (closedForGood) {
          opened.close()
          throw new ConnectionFailedError("The KronosDB connection is closed")
        }
        preferred = index
        attach(opened)
        return opened
      } catch (error) {
        // Wrong credentials are wrong on every node; the next address cannot help.
        if (error instanceof AuthenticationError || closedForGood) throw error
        last = error
      }
    }
    throw last
  }

  function ready(): Promise<NativeTransport> {
    const live = current()
    if (live !== undefined) return Promise.resolve(live)
    if (closedForGood) return Promise.reject(new ConnectionFailedError("The KronosDB connection is closed"))
    connecting ??= connectOnce().finally(() => {
      connecting = undefined
    })
    return connecting
  }

  return {
    current,
    ready,
    onConnect(listener) {
      connectListeners.push(listener)
    },
    onClose(listener) {
      closeListeners.push(listener)
    },
    async restart() {
      if (closedForGood || !everConnected || current() !== undefined) return
      await ready()
    },
    close() {
      closedForGood = true
      for (const opened of [...open]) opened.close()
      open.clear()
      transport = undefined
    },
  }
}
