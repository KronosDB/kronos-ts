import type { ModuleApi } from "./module.js"

// ---------------------------------------------------------------------------
// Slices — the vertical-slice unit, with app-extensible metadata.
//
// A slice is a named registration unit inside a module: one cohesive feature
// (typically one file) that wires its command handlers, states, projections
// and processors through the module's dep-typed context. The framework owns
// what is uniform about slices — identity, uniqueness, collection, iteration —
// and deliberately does NOT own their payload: `Meta` is a caller-defined
// generic, so an application decides what "makes it down to a slice" (an RPC
// contract fragment, docs, permissions, feature flags, …) and reads it back
// through the `slices()` accessor to build its own edges.
// ---------------------------------------------------------------------------

/** A defined slice: registration behaviour + app-defined metadata. */
export interface Slice<Deps = Record<never, never>, Meta = undefined> {
  readonly kind: "slice"
  readonly name: string
  readonly meta: Meta
  readonly register: (m: ModuleApi<Deps>) => void
}

/** A slice as recorded on the app after registration, for host iteration. */
export interface RegisteredSlice {
  readonly name: string
  /** Name of the module the slice was registered through. */
  readonly module: string
  readonly meta: unknown
}

/** Thrown when two slices register under the same name. */
export class DuplicateSliceNameError extends Error {
  constructor(name: string, firstModule: string, secondModule: string) {
    super(
      `Slice name "${name}" is registered twice (via module "${firstModule}" and ` +
        `module "${secondModule}"). Slice names are the app-wide identity hosts ` +
        `iterate over — rename one of the slices.`,
    )
    this.name = "DuplicateSliceNameError"
  }
}

/**
 * Define a slice. `Deps` is inferred from the `register` callback's parameter
 * annotation; `Meta` from the `meta` value — neither needs to be spelled out:
 *
 * ```ts
 * export const openTicket = defineSlice({
 *   name: "open-ticket",
 *   meta: { rpc: { tickets: { open: openTicketProcedure } } },
 *   register: (m: ModuleApi<SupportDeps>) => {
 *     m.states(TicketExistence)
 *     m.commandHandler(OpenTicket, async ({ payload }, ctx) => {
 *       await ctx.db.insert(messageBodies).values({ ... })
 *       ctx.append(TicketOpened, { ticketId: payload.ticketId })
 *     })
 *   },
 * })
 *
 * // module composes slices; the host reads their meta back:
 * const support = defineModule<SupportDeps>("support", (m) => {
 *   m.slices(openTicket, closeTicket, ticketList)
 * })
 * const app = kronos().use(support({ db, storage }))
 * const rpcFragments = app.slices().map((s) => (s.meta as SupportSliceMeta).rpc)
 * ```
 */
export function defineSlice<Deps = Record<never, never>, Meta = undefined>(opts: {
  name: string
  meta?: Meta
  register: (m: ModuleApi<Deps>) => void
}): Slice<Deps, Meta> {
  return {
    kind: "slice",
    name: opts.name,
    meta: opts.meta as Meta,
    register: opts.register,
  }
}
