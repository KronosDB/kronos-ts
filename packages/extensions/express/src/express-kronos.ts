import type { App, Extension } from "@kronos-ts/core"
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
 * Integrates an Express application with Kronos as a native Extension.
 *
 * On `serve` start: bridges gateways into `app.locals.kronos` and starts listening.
 * On `serve` stop: closes the server gracefully.
 *
 * ```typescript
 * import express from "express"
 * import { kronos } from "@kronos-ts/core"
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
 * await kronos()
 *   .entities(CourseEntity)
 *   .commands(createCourse)
 *   .use(withExpress(app, { port: 3000 }))
 *   .start()
 * ```
 */
export function withExpress(
  app: ExpressApp,
  options?: { port?: number },
): Extension {
  let server: any
  return (kronosApp: App) => {
    kronosApp.onStart("serve", async () => {
      const locals: KronosLocals = {
        commandGateway: kronosApp.commandGateway,
        queryGateway: kronosApp.queryGateway,
      }
      app.locals.kronos = locals
      server = app.listen(options?.port ?? 3000)
    })
    kronosApp.onStop("serve", async () => {
      if (server) {
        // Express server.close is callback-style; wrap in Promise for async stop.
        await new Promise<void>((resolve, reject) => {
          server.close((err: any) => (err ? reject(err) : resolve()))
        })
      }
    })
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
