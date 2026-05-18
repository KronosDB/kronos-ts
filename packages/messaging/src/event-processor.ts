/**
 * Common interface for all event processors.
 */
export interface EventProcessor {
  readonly name: string
  readonly running: boolean
  start(): Promise<void>
  stop(): void
}
