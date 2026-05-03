// transitional: Phase 9 deletes — pulls legacy ConfigurationEnhancer surface from
// the bridge until this extension is migrated to (app: App) => void.
import {
  ComponentKeys,
  type ComponentRegistry,
  type ConfigurationEnhancer,
} from "@kronos-ts/core/legacy-enhancer-bridge"
import {
  type CommandBus,
  tracingHandlerEnhancerDefinition,
  createTracingCommandBus,
} from "@kronos-ts/messaging"
import {
  createOpenTelemetrySpanFactory,
  type OpenTelemetrySpanFactoryOptions,
} from "./opentelemetry-span-factory.js"

/**
 * Creates a ConfigurationEnhancer that integrates OpenTelemetry tracing
 * into the Kronos framework.
 *
 * Registers:
 * - `SpanFactory` as a component (OpenTelemetry implementation)
 * - `TracingCommandBus` decorator that wraps command dispatch and handling with spans
 * - `HandlerEnhancerDefinition` that wraps event/query handlers with tracing spans
 *
 * This follows the Kronos architecture: command tracing uses the `TracingCommandBus`
 * decorator (not monitors), while event handler tracing uses
 * `TracingHandlerEnhancerDefinition`.
 *
 * Usage:
 * ```typescript
 * // transitional — the (app: App) => void shape lands in Phase 9 (EXT-04);
 * // until then production callers register the legacy enhancer via .use().
 * await kronos()
 *   .use(openTelemetryEnhancer())
 *   .start()
 * ```
 */
export function openTelemetryEnhancer(
  options: OpenTelemetrySpanFactoryOptions = {},
): ConfigurationEnhancer {
  return {
    order: -50,

    enhance(registry: ComponentRegistry) {
      const spanFactory = createOpenTelemetrySpanFactory(options)

      // Register SpanFactory as a component
      registry.register("spanFactory", () => spanFactory)

      // Register TracingCommandBus decorator (bus decorator, not monitor)
      registry.registerDecorator<CommandBus>(
        ComponentKeys.COMMAND_BUS,
        -200,
        (_config, _name, delegate) => {
          return createTracingCommandBus(delegate, spanFactory)
        },
      )

      // Register tracing handler enhancer for event/query handler tracing
      registry.register(
        ComponentKeys.HANDLER_ENHANCER_DEFINITIONS,
        () => tracingHandlerEnhancerDefinition(spanFactory),
      )
    },
  }
}
