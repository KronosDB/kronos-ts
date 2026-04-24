import {
  ComponentKeys,
  qualifiedNameToString,
  generateIdentifier,
  resourceKey,
  type Configuration,
  type Module,
  type Metadata,
  type ResourceKey,
} from "@kronos-ts/common"
import type { CommandHandlerDefinition } from "./command-handler.js"
import type { CommandBus } from "./command-bus.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"
import { CORRELATION_DATA_KEY } from "./correlation-data.js"
import { getResource, computeIfAbsent } from "./processing-state.js"
import type { CommandMessage, EventMessage } from "./message.js"
import type { EventDescriptor } from "./descriptor.js"
import type { EventCriteria } from "./event-criteria.js"
import type { LoadFunction, AppendFunction } from "./handler.js"
import type { ProcessingContext } from "./processing-context.js"

// ---------------------------------------------------------------------------
// Resource keys for ProcessingContext-scoped state
// ---------------------------------------------------------------------------

/** Buffered events waiting to be flushed at PREPARE_COMMIT. */
const BUFFERED_EVENTS_KEY: ResourceKey<EventMessage[]> = resourceKey("bufferedEvents")

/** Sourcing info from load() calls, used to build append condition. */
const SOURCING_INFOS_KEY: ResourceKey<Array<{ criteria: EventCriteria; markerPosition: bigint }>> =
  resourceKey("sourcingInfos")

/** Entity cache: prevents duplicate load() calls within same UnitOfWork. */
const ENTITY_CACHE_KEY: ResourceKey<Map<string, Promise<unknown>>> = resourceKey("entityCache")

/** Entity module references keyed by cache key, used to apply evolvers on append. */
const ENTITY_MODULES_KEY: ResourceKey<Map<string, { entity: any; id: unknown }>> = resourceKey("entityModules")

// ---------------------------------------------------------------------------
// Command invocation — builds handler context from ProcessingContext
// ---------------------------------------------------------------------------

/**
 * Creates a command handler invocation function that uses the ProcessingContext
 * for state caching, event buffering, and lifecycle-aware event flushing.
 */
function createCommandInvocation(
  handler: CommandHandlerDefinition<any, any>,
  config: Configuration,
) {
  return async (message: CommandMessage, ctx: ProcessingContext): Promise<unknown> => {
    const rawStateManager = config.hasComponent(ComponentKeys.STATE_MANAGER)
      ? config.getComponent<{ load: (entity: any, id: any) => Promise<{ state: any; sourcingInfo: { criteria: EventCriteria; markerPosition: bigint } }> }>(ComponentKeys.STATE_MANAGER)
      : null

    // Load function with entity caching per ProcessingContext
    const trackingLoad: LoadFunction = async <S>(entity: { name: string }, id: unknown): Promise<S> => {
      if (!rawStateManager) throw new Error("No state manager configured")

      const cache = computeIfAbsent(ENTITY_CACHE_KEY, () => new Map())
      const cacheKey = `${entity.name}:${String(id)}`

      if (!cache.has(cacheKey)) {
        cache.set(cacheKey, rawStateManager.load(entity, id))
        // Store entity module reference for evolver lookups during append
        const modules = computeIfAbsent(ENTITY_MODULES_KEY, () => new Map())
        modules.set(cacheKey, { entity, id })
      }

      const result = await cache.get(cacheKey)!
      const loadResult = result as { state: any; sourcingInfo: { criteria: EventCriteria; markerPosition: bigint } }

      // Track sourcing info for append condition
      const infos = computeIfAbsent(SOURCING_INFOS_KEY, () => [])
      infos.push(loadResult.sourcingInfo)

      return loadResult.state as S
    }

    // Append function — buffers events in ProcessingContext.
    // Tags are derived from the descriptor's tags function at creation time
    // (TS equivalent of Java's @EventTag annotations resolved via reflection).
    // The TagResolver can enrich with additional tags before storage.
    const appendFn: AppendFunction = ((
      eventDescriptor: EventDescriptor<any>,
      eventPayload: unknown,
      eventMetadata?: Metadata,
    ) => {
      const events = computeIfAbsent(BUFFERED_EVENTS_KEY, () => [])
      const tags = eventDescriptor.tags ? eventDescriptor.tags(eventPayload) : []
      const eventMessage: EventMessage = {
        identifier: generateIdentifier(),
        name: eventDescriptor.name,
        version: eventDescriptor.version,
        payload: eventPayload,
        metadata: eventMetadata ?? message.metadata,
        timestamp: Date.now(),
        tags,
      }
      events.push(eventMessage)

      // Update cached entity state by applying matching evolvers
      const cache = getResource(ENTITY_CACHE_KEY)
      const modules = getResource(ENTITY_MODULES_KEY)
      if (cache && modules) {
        const eventType = qualifiedNameToString(eventDescriptor.name)
        for (const [cacheKey, { entity, id }] of modules) {
          const cachedPromise = cache.get(cacheKey)
          if (!cachedPromise) continue
          // Only update if the entity has evolvers matching this event
          const evolvers = (entity as any).evolvers as ReadonlyArray<{ descriptor: { name: any }; evolve: (s: any, e: any, id: any) => any }> | undefined
          if (!evolvers) continue
          for (const evolver of evolvers) {
            if (qualifiedNameToString(evolver.descriptor.name) === eventType) {
              // Replace cached promise with an updated one that applies the evolver
              cache.set(cacheKey, cachedPromise.then((result: any) => ({
                ...result,
                state: evolver.evolve(result.state, eventPayload, id),
              })))
              break
            }
          }
        }
      }
    }) as AppendFunction

    // Register event flush in PREPARE_COMMIT phase
    ctx.onPrepareCommit(async () => {
      const buffered = getResource(BUFFERED_EVENTS_KEY)
      if (!buffered || buffered.length === 0) return
      if (!config.hasComponent(ComponentKeys.EVENT_STORE)) return

      const eventStore = config.getComponent<{ append: (events: ReadonlyArray<EventMessage>, condition?: any) => Promise<unknown> }>(ComponentKeys.EVENT_STORE)

      // Enrich events with correlation data from ProcessingContext
      // (set by the CorrelationDataHandlerInterceptor during handler execution)
      const correlationData = ctx.get(CORRELATION_DATA_KEY)
      const enrichedEvents = correlationData
        ? buffered.map(event => ({
            ...event,
            metadata: { ...event.metadata, ...correlationData },
          }))
        : buffered

      // Resolve tags via TagResolver (if configured)
      const tagResolver = config.getOptionalComponent<{ resolve: (event: EventMessage) => Array<{ key: string; value: string }> }>(ComponentKeys.TAG_RESOLVER)
      const resolvedEvents = tagResolver
        ? enrichedEvents.map(event => ({
            ...event,
            tags: [...event.tags, ...tagResolver.resolve(event)],
          }))
        : enrichedEvents
      const sourcingInfos = getResource(SOURCING_INFOS_KEY) ?? []

      let appendCondition: any = undefined
      if (sourcingInfos.length > 0) {
        const combinedCriteria = sourcingInfos.length === 1
          ? sourcingInfos[0]!.criteria
          : { kind: "either" as const, criteria: sourcingInfos.map((s) => s.criteria) }

        const maxMarker = sourcingInfos.reduce(
          (max, s) => s.markerPosition > max ? s.markerPosition : max,
          -1n,
        )

        const finalCriteria = handler.appendCondition
          ? handler.appendCondition(message.payload, combinedCriteria)
          : combinedCriteria

        appendCondition = {
          criteria: finalCriteria,
          marker: { position: maxMarker },
        }
      }

      await eventStore.append(resolvedEvents, appendCondition)
    })

    // Call user's handler
    const context = {
      load: trackingLoad,
      append: appendFn,
      metadata: message.metadata,
      processingContext: ctx,
    }

    return handler.handler(message.payload, context)
  }
}

/**
 * A module that registers command handlers with the command bus.
 *
 * ```
 * commandHandlingModule("course-commands", [createCourse, changeCourseCapacity])
 * ```
 */
export function commandHandlingModule(
  moduleName: string,
  handlers: ReadonlyArray<CommandHandlerDefinition<any, any>>,
): Module {
  return {
    name: moduleName,

    initialize(config: Configuration) {
      const bus = config.getComponent<CommandBus>(ComponentKeys.COMMAND_BUS)
      const enhancer = config.getOptionalComponent<HandlerEnhancerDefinition>(
        ComponentKeys.HANDLER_ENHANCER_DEFINITIONS,
      )

      for (const handler of handlers) {
        const commandName = qualifiedNameToString(handler.descriptor.name)
        let invocation = createCommandInvocation(handler, config)

        if (enhancer) {
          invocation = enhancer.wrapHandler(invocation, {
            messageType: "command",
            messageName: commandName,
            handlerGroup: moduleName,
          })
        }

        bus.subscribe(commandName, invocation)
      }
    },
  }
}
