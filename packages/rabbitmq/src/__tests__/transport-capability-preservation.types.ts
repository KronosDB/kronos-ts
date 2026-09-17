/**
 * THE TRANSPORT LEG of the anti-laundering probe.
 *
 * A transport is a same-seam wrapper — a bus in, a bus out — so whatever
 * capability the bus underneath carried has to come out the other side. The
 * probe below builds one with a LOCAL branded task, `Traced`, standing in for
 * any composed unit-of-work capability a host might mint: a chain built from
 * `() => Object.assign(unitOfWork(), { probe: true as const })` mints `Traced`
 * units of work, and a bus typed `CommandBus<Traced>` only fits behind a
 * transport that still says so.
 *
 * This lives here rather than in `integrationtests` because that package does
 * not depend on `@kronos-ts/rabbitmq`, and a probe is not worth a dependency.
 *
 * Nothing here runs; it is judged by `bunx tsc --noEmit` through the root
 * `tsconfig.json` `files` array, and the broker handles are stood in for.
 */
import {
  interceptingCommandBus,
  interceptingQueryBus,
  localCommandBus,
  localQueryBus,
  unitOfWork,
  type CommandBus,
  type CommandMessage,
  type Intercept,
  type QueryBus,
  type QueryMessage,
  type UnitOfWork,
} from "@kronos-ts/core"
import { rabbitMqCommandBus, type RabbitMqCommandBusSource } from "../command-bus.js"
import { rabbitMqQueryBus, type RabbitMqQueryBusSource } from "../query-bus.js"

declare const rabbit: RabbitMqCommandBusSource
declare const rabbitQueries: RabbitMqQueryBusSource
declare const intercept: Intercept<CommandMessage>
declare const interceptQuery: Intercept<QueryMessage>

/** A stand-in composed capability — any host-branded task would do. */
type Traced = UnitOfWork & { readonly probe: true }
const tracedUow = (): Traced => Object.assign(unitOfWork(), { probe: true as const })

/**
 * THE FULL CHAIN, in the order a distributed host writes it: a branded
 * factory at the bottom, the local bus over it, the transport over that, and
 * interception OUTERMOST so it covers both branches of the transport's
 * local-vs-remote fork.
 */
export const chainKeepsTraced: CommandBus<Traced> = interceptingCommandBus(
  rabbitMqCommandBus(localCommandBus(tracedUow), rabbit),
  intercept,
)

export const queryChainKeepsTraced: QueryBus<Traced> = interceptingQueryBus(
  rabbitMqQueryBus(localQueryBus(tracedUow), rabbitQueries),
  interceptQuery,
)

/** The transport on its own, so the claim is about the transport and not the wrap. */
export const transportAloneKeepsTraced: CommandBus<Traced> = rabbitMqCommandBus(
  localCommandBus(tracedUow),
  rabbit,
)

/** A BARE chain is still bare, so none of the above is vacuous. */
// @ts-expect-error — this chain mints plain units of work, all the way down
export const bareChainIsNotTraced: CommandBus<Traced> = interceptingCommandBus(
  rabbitMqCommandBus(localCommandBus(unitOfWork), rabbit),
  intercept,
)
