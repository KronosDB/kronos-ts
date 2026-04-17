/**
 * Generates a unique identifier for messages.
 * Uses `crypto.randomUUID()` which is available in Node 19+, Bun, Deno,
 * and all modern browsers.
 */
export function generateIdentifier(): string {
  return crypto.randomUUID()
}
