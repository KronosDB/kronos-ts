/**
 * Common interface for all event processors.
 * Aligned with AF5's `EventProcessor`.
 */
export interface EventProcessor {
  readonly name: string
  readonly running: boolean
  start(): Promise<void>
  stop(): void
}
