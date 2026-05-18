import type { App, Extension } from "@kronos-ts/core"
import type { CommandGateway, QueryGateway } from "@kronos-ts/messaging"

/**
 * Kronos gateways available on the Fastify instance via `fastify.kronos`.
 */
export interface KronosDecorator {
  readonly commandGateway: CommandGateway
  readonly queryGateway: QueryGateway
}

/**
 * Minimal Fastify instance type to avoid hard dependency.
 */
interface FastifyInstance {
  decorate(name: string, value: any): void
  addHook(name: string, fn: (...args: any[]) => any): void
  close(): Promise<void>
}

/**
 * Integrates a Fastify instance with Kronos as a native Extension.
 *
 * On `serve` start: decorates the Fastify instance with `fastify.kronos` for gateway access.
 * On `serve` stop: closes the Fastify server gracefully.
 *
 * ```typescript
 * import Fastify from "fastify"
 * import { kronos } from "@kronos-ts/core"
 * import { withFastify } from "@kronos-ts/fastify"
 *
 * const fastify = Fastify()
 *
 * fastify.post("/courses", async (req, reply) => {
 *   await fastify.kronos.commandGateway.send(CreateCourse, req.body)
 *   reply.code(201).send()
 * })
 *
 * await kronos()
 *   .states(Course)
 *   .commands(createCourse)
 *   .use(withFastify(fastify))
 *   .start()
 *
 * await fastify.listen({ port: 3000 })
 * ```
 *
 * With TypeScript declaration merging for type-safe access:
 * ```typescript
 * declare module "fastify" {
 *   interface FastifyInstance {
 *     kronos: KronosDecorator
 *   }
 * }
 * ```
 */
export function withFastify(fastify: FastifyInstance): Extension {
  return (kronosApp: App) => {
    kronosApp.onStart("serve", async () => {
      const decorator: KronosDecorator = {
        commandGateway: kronosApp.commandGateway,
        queryGateway: kronosApp.queryGateway,
      }
      fastify.decorate("kronos", decorator)
    })
    kronosApp.onStop("serve", async () => {
      await fastify.close()
    })
  }
}
