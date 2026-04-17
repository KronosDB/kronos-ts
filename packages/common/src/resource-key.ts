/**
 * A type-safe key for storing and retrieving resources from a ProcessingContext.
 *
 * Uses Symbol for identity — two keys with the same label are still distinct,
 * preventing accidental collisions between framework and user code.
 * The phantom type parameter `T` ensures compile-time safety.
 */
export interface ResourceKey<T> {
  readonly symbol: symbol
  readonly label: string
  readonly _phantom?: T
}

/**
 * Creates a new resource key with the given label.
 * Each call produces a unique key (Symbol-based identity).
 * The type parameter determines what type of value can be stored under this key.
 */
export function resourceKey<T>(label: string): ResourceKey<T> {
  return { symbol: Symbol(label), label } as ResourceKey<T>
}
