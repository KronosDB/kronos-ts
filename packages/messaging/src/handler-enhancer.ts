/**
 * Metadata about a handler being enhanced. Allows enhancers to
 * selectively wrap based on handler type, message name, etc.
 */
export interface HandlerMetadata {
  /** The type of message this handler processes. */
  readonly messageType: "command" | "event" | "query"
  /** The qualified name of the message (e.g., "university.courses.CreateCourse"). */
  readonly messageName: string
  /** The name of the handler group or module (e.g., "course-commands"). */
  readonly handlerGroup: string
}

/**
 * Wraps message handler functions at registration time to add cross-cutting
 * concerns. Different from interceptors — interceptors wrap dispatch,
 * enhancers wrap the handler itself.
 *
 * Applied once at handler discovery/registration time, not per-invocation.
 * This makes enhancers ideal for:
 * - Tracing spans per-handler
 * - Security checks
 * - Timeout enforcement
 * - Caching
 *
 * @example
 * ```ts
 * const timingEnhancer: HandlerEnhancerDefinition = {
 *   wrapHandler(handler, metadata) {
 *     return async (...args) => {
 *       const start = performance.now()
 *       try {
 *         return await handler(...args)
 *       } finally {
 *         console.log(`${metadata.messageName} took ${performance.now() - start}ms`)
 *       }
 *     }
 *   },
 * }
 * ```
 */
export interface HandlerEnhancerDefinition {
  /**
   * Wrap a handler function. Return the original handler to skip enhancement.
   * The returned function must have the same signature as the input.
   */
  wrapHandler<T extends (...args: any[]) => any>(
    handler: T,
    metadata: HandlerMetadata,
  ): T
}

/**
 * Combines multiple handler enhancer definitions into a single one.
 * Enhancers are applied in order — the first enhancer wraps outermost.
 */
export function multiHandlerEnhancerDefinition(
  enhancers: ReadonlyArray<HandlerEnhancerDefinition>,
): HandlerEnhancerDefinition {
  return {
    wrapHandler<T extends (...args: any[]) => any>(handler: T, metadata: HandlerMetadata): T {
      let wrapped = handler
      // Apply in reverse order so first enhancer is outermost
      for (let i = enhancers.length - 1; i >= 0; i--) {
        wrapped = enhancers[i]!.wrapHandler(wrapped, metadata)
      }
      return wrapped
    },
  }
}
