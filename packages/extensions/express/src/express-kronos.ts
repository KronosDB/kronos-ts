import { ComponentKeys, type ConfigurationEnhancer } from "@kronos-ts/common"
import type { CommandGateway, QueryGateway } from "@kronos-ts/messaging"

/**
 * Kronos gateways available on `app.locals.kronos` and `req.app.locals.kronos`.
 */
export interface KronosLocals {
  readonly commandGateway: CommandGateway
  readonly queryGateway: QueryGateway
}

/**
 * Express application type with Kronos locals.
 * Minimal type — avoids importing Express as a hard dependency.
 */
interface ExpressApp {
  locals: Record<string, any>
  listen(port: number, callback?: () => void): any
}

/**
 * Integrates an Express application with Kronos as a ConfigurationEnhancer.
 *
 * On start: bridges gateways into `app.locals.kronos` and starts listening.
 * On stop: closes the server gracefully.
 *
 * ```typescript
 * import express from "express"
 * import { legacyKronos } from "@kronos-ts/eventsourcing"
 * import { withExpress, getKronos } from "@kronos-ts/express"
 *
 * const app = express()
 *
 * app.post("/courses", async (req, res) => {
 *   const { commandGateway } = getKronos(req)
 *   await commandGateway.send(CreateCourse, req.body)
 *   res.status(201).end()
 * })
 *
 * await legacyKronos()
 *   .register(courses)
 *   .register(withExpress(app, { port: 3000 }))
 *   .start()
 * ```
 */
export function withExpress(
  app: ExpressApp,
  options?: { port?: number },
): ConfigurationEnhancer {
  let server: any

  return {
    enhance() {},
    async onStart(config) {
      const locals: KronosLocals = {
        commandGateway: config.getComponent<CommandGateway>(ComponentKeys.COMMAND_GATEWAY),
        queryGateway: config.getComponent<QueryGateway>(ComponentKeys.QUERY_GATEWAY),
      }
      app.locals.kronos = locals
      server = app.listen(options?.port ?? 3000)
    },
    async onStop() {
      if (server) server.close()
    },
  }
}


/**
 * Gets the Kronos gateways from an Express request.
 * Convenience helper for typed access in route handlers.
 *
 * ```typescript
 * app.post("/courses", async (req, res) => {
 *   const { commandGateway } = getKronos(req)
 *   await commandGateway.send(CreateCourse, req.body)
 *   res.status(201).end()
 * })
 * ```
 */
export function getKronos(req: { app: { locals: Record<string, any> } }): KronosLocals {
  const kronos = req.app.locals.kronos
  if (!kronos) {
    throw new Error("Kronos not initialized. Register withExpress() and call start() before handling requests.")
  }
  return kronos as KronosLocals
}
