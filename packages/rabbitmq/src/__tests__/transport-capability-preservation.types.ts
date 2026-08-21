/**
 * THE TRANSPORT LEG of the anti-laundering probe.
 *
 * A transport is a same-seam wrapper — a bus in, a bus out — so whatever
 * capability the bus underneath carried has to come out the other side. The one
 * that exists today is the CORRELATION demand: a chain built from
 * `() => correlating(unitOfWork())` mints correlating units of work, and a
 * handler annotated `ctx: HandlerContext<CorrelatingUnitOfWork>` only fits
 * behind a bus that still says so.
 *
 * This lives here rather than in `integrationtests` because that package does
 * not depend on `@kronos-ts/rabbitmq`, and a probe is not worth a dependency.
 *
 * Nothing here runs; it is judged by `bunx tsc --noEmit` through the root
 * `tsconfig.json` `files` array, and the broker handles are stood in for.
 */
import {
  correlating,
  interceptingCommandBus,
  interceptingQueryBus,
  localCommandBus,
  localQueryBus,
  unitOfWork,
  type CommandBus,
  type CommandMessage,
  type CorrelatingUnitOfWork,
  type Intercept,
  type QueryBus,
  type QueryMessage,
} from "@kronos-ts/core"
import { rabbitMqCommandBus, type RabbitMqCommandBusSource } from "../command-bus.js"
import { rabbitMqQueryBus, type RabbitMqQueryBusSource } from "../query-bus.js"

declare const rabbit: RabbitMqCommandBusSource
declare const rabbitQueries: RabbitMqQueryBusSource
declare const intercept: Intercept<CommandMessage>
declare const interceptQuery: Intercept<QueryMessage>

const correlatingUow = () => correlating(unitOfWork())

/**
 * THE FULL CHAIN, in the order a distributed host writes it: a correlating
 * factory at the bottom, the local bus over it, the transport over that, and
 * interception OUTERMOST so it covers both branches of the transport's
 * local-vs-remote fork.
 */
export const chainKeepsCorrelation: CommandBus<CorrelatingUnitOfWork> = interceptingCommandBus(
  rabbitMqCommandBus(localCommandBus(correlatingUow), rabbit),
  intercept,
)

export const queryChainKeepsCorrelation: QueryBus<CorrelatingUnitOfWork> = interceptingQueryBus(
  rabbitMqQueryBus(localQueryBus(correlatingUow), rabbitQueries),
  interceptQuery,
)

/** The transport on its own, so the claim is about the transport and not the wrap. */
export const transportAloneKeepsCorrelation: CommandBus<CorrelatingUnitOfWork> = rabbitMqCommandBus(
  localCommandBus(correlatingUow),
  rabbit,
)

/** A BARE chain is still bare, so none of the above is vacuous. */
// @ts-expect-error — this chain mints plain units of work, all the way down
export const bareChainIsNotCorrelating: CommandBus<CorrelatingUnitOfWork> = interceptingCommandBus(
  rabbitMqCommandBus(localCommandBus(unitOfWork), rabbit),
  intercept,
)
