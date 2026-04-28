import { ComponentKeys, type ConfigurationEnhancer } from "@kronos-ts/common"
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
 * Integrates a Fastify instance with Kronos as a ConfigurationEnhancer.
 *
 * On start: decorates the Fastify instance with `fastify.kronos` for gateway access.
 * On stop: closes the Fastify server gracefully.
 *
 * ```typescript
 * import Fastify from "fastify"
 * import { legacyKronos } from "@kronos-ts/eventsourcing"
 * import { withFastify } from "@kronos-ts/fastify"
 *
 * const fastify = Fastify()
 *
 * fastify.post("/courses", async (req, reply) => {
 *   await fastify.kronos.commandGateway.send(CreateCourse, req.body)
 *   reply.code(201).send()
 * })
 *
 * await legacyKronos()
 *   .register(courses)
 *   .register(withFastify(fastify))
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
export function withFastify(fastify: FastifyInstance): ConfigurationEnhancer {
  return {
    enhance() {},
    async onStart(config) {
      const decorator: KronosDecorator = {
        commandGateway: config.getComponent<CommandGateway>(ComponentKeys.COMMAND_GATEWAY),
        queryGateway: config.getComponent<QueryGateway>(ComponentKeys.QUERY_GATEWAY),
      }
      fastify.decorate("kronos", decorator)
    },
    async onStop() {
      await fastify.close()
    },
  }
}

