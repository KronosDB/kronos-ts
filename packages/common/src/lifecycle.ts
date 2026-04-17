/**
 * Phases for startup and shutdown ordering.
 *
 * Start handlers execute in ascending phase order.
 * Shutdown handlers execute in descending phase order.
 *
 * This ensures external connections are established before handlers register,
 * and handlers are deregistered before connections close.
 *
 * Aligned with Kronos Framework's `Phase`.
 */
export const LifecyclePhase = {
  /** Start/shutdown external connections (databases, Axon Server, etc.) */
  EXTERNAL_CONNECTIONS: -1000,
  /** Outbound event connectors. */
  OUTBOUND_EVENT_CONNECTORS: -10,
  /** Register local message handlers with buses. */
  LOCAL_MESSAGE_HANDLER_REGISTRATIONS: 0,
  /** Outbound command/query connectors. */
  OUTBOUND_COMMAND_CONNECTORS: 0,
  /** Inbound command connector (accept commands from external). */
  INBOUND_COMMAND_CONNECTOR: 500,
  /** Inbound query connector (accept queries from external). */
  INBOUND_QUERY_CONNECTOR: 500,
  /** Inbound event connectors (event processors). */
  INBOUND_EVENT_CONNECTORS: 1000,
  /** Instruction components (processor control, etc.). */
  INSTRUCTION_COMPONENTS: 1010,
} as const

export type LifecyclePhaseValue = number

/**
 * Handler type for lifecycle callbacks. Optionally receives the built
 * configuration when executed during start/shutdown.
 */
export type LifecycleHandler = (config?: any) => Promise<void> | void

/**
 * Registry for lifecycle handlers that execute during application
 * startup and shutdown in defined phases.
 *
 * Handlers within the same phase execute concurrently.
 * Phases execute sequentially in order (ascending for start, descending for stop).
 *
 * Aligned with AF5's `LifecycleRegistry`.
 */
export interface LifecycleRegistry {
  /**
   * Register a handler to execute during startup at the given phase.
   * The handler optionally receives the built Configuration.
   */
  onStart(phase: LifecyclePhaseValue, handler: LifecycleHandler): void

  /**
   * Register a handler to execute during shutdown at the given phase.
   * Shutdown phases execute in descending order (reverse of start).
   * The handler optionally receives the built Configuration.
   */
  onShutdown(phase: LifecyclePhaseValue, handler: LifecycleHandler): void
}

/**
 * Options for creating a lifecycle registry.
 */
export interface LifecycleRegistryOptions {
  /**
   * Timeout in milliseconds for each lifecycle phase.
   * If a phase exceeds this duration, a warning is logged and execution continues.
   * Default: 5000 (5 seconds), aligned with Java's default phase timeout.
   */
  phaseTimeoutMs?: number
}

/**
 * Creates a lifecycle registry that collects handlers and executes
 * them in phase order.
 *
 * @param options Optional configuration for the lifecycle registry.
 */
export function createLifecycleRegistry(options?: LifecycleRegistryOptions): LifecycleRegistry & {
  /** Execute all start handlers in ascending phase order. */
  start(config?: any): Promise<void>
  /** Execute all shutdown handlers in descending phase order. */
  shutdown(config?: any): Promise<void>
} {
  const phaseTimeoutMs = options?.phaseTimeoutMs ?? 5000
  const startHandlers = new Map<number, Array<LifecycleHandler>>()
  const shutdownHandlers = new Map<number, Array<LifecycleHandler>>()

  function addHandler(
    map: Map<number, Array<LifecycleHandler>>,
    phase: number,
    handler: LifecycleHandler,
  ) {
    if (!map.has(phase)) map.set(phase, [])
    map.get(phase)!.push(handler)
  }

  async function executePhases(
    map: Map<number, Array<LifecycleHandler>>,
    ascending: boolean,
    config?: any,
  ) {
    const phases = [...map.keys()].sort((a, b) => ascending ? a - b : b - a)
    for (const phase of phases) {
      const handlers = map.get(phase)!
      // Handlers within same phase execute concurrently, with a timeout
      const phasePromise = Promise.all(handlers.map(h => Promise.resolve(h(config))))

      const timeoutPromise = new Promise<"timeout">((resolve) =>
        setTimeout(() => resolve("timeout"), phaseTimeoutMs),
      )

      const result = await Promise.race([
        phasePromise.then(() => "done" as const),
        timeoutPromise,
      ])

      if (result === "timeout") {
        console.warn(
          `Lifecycle phase ${phase} exceeded timeout of ${phaseTimeoutMs}ms. ` +
          `Continuing to next phase.`,
        )
        // Wait for the phase to actually complete (don't abandon it)
        // but don't block subsequent phases
      }
    }
  }

  return {
    onStart(phase, handler) {
      addHandler(startHandlers, phase, handler)
    },

    onShutdown(phase, handler) {
      addHandler(shutdownHandlers, phase, handler)
    },

    async start(config?: any) {
      await executePhases(startHandlers, true, config)
    },

    async shutdown(config?: any) {
      await executePhases(shutdownHandlers, false, config)
    },
  }
}
