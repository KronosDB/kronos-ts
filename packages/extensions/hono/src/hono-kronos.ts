import type { App, Extension } from "@kronos-ts/core"
import type { CommandGateway, QueryGateway } from "@kronos-ts/messaging"

/**
 * Kronos gateways stored in Hono context variables.
 */
export interface KronosContext {
  readonly commandGateway: CommandGateway
  readonly queryGateway: QueryGateway
}

/**
 * Hono context variable key for Kronos gateways.
 */
export const KRONOS_CONTEXT_KEY = "kronos"

/**
 * Minimal Hono app type to avoid hard dependency.
 */
interface HonoApp {
  use(path: string, ...handlers: any[]): any
}

/**
 * Integrates a Hono application with Kronos as a native Extension.
 *
 * On `serve` start: registers a middleware that sets gateways on every request context.
 *
 * Works across all Hono runtimes: Node.js, Bun, Deno, Cloudflare Workers.
 *
 * ```typescript
 * import { Hono } from "hono"
 * import { kronos } from "@kronos-ts/core"
 * import { withHono, getKronos } from "@kronos-ts/hono"
 *
 * const app = new Hono()
 *
 * app.post("/courses", async (c) => {
 *   const { commandGateway } = getKronos(c)
 *   await commandGateway.send(CreateCourse, await c.req.json())
 *   return c.json({ status: "created" }, 201)
 * })
 *
 * await kronos()
 *   .states(Course)
 *   .commands(createCourse)
 *   .use(withHono(app))
 *   .start()
 * ```
 */
export function withHono(app: HonoApp): Extension {
  return (kronosApp: App) => {
    kronosApp.onStart("serve", async () => {
      const kronosCtx: KronosContext = {
        commandGateway: kronosApp.commandGateway,
        queryGateway: kronosApp.queryGateway,
      }
      app.use("*", async (c: any, next: () => Promise<void>) => {
        c.set(KRONOS_CONTEXT_KEY, kronosCtx)
        await next()
      })
    })
  }
}


/**
 * Gets the Kronos gateways from a Hono context.
 *
 * ```typescript
 * app.post("/courses", async (c) => {
 *   const { commandGateway } = getKronos(c)
 *   await commandGateway.send(CreateCourse, await c.req.json())
 *   return c.json({ status: "created" }, 201)
 * })
 * ```
 */
export function getKronos(c: { get: (key: string) => any }): KronosContext {
  const kronos = c.get(KRONOS_CONTEXT_KEY)
  if (!kronos) {
    throw new Error("Kronos not initialized. Register withHono() and call start() before handling requests.")
  }
  return kronos as KronosContext
}
