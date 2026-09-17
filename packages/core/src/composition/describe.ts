// ---------------------------------------------------------------------------
// WRAPPERS THAT SAY WHAT THEY DO.
//
// A handler wrapper is an opaque function. The compiler can check that a
// wrapper ERASES the capability it supplies, but it cannot see ORDER between
// wrappers unless a wrapper says something about itself — and a host that
// casts (or writes JavaScript) has no compiler at all. So a wrapper marks the
// function it returns with a small description: its name, what it supplies to
// the context, what it uses from the context, what it stamps onto the message
// and what it reads off the message. `kronos()` walks the chain at boot and
// refuses to start on a chain that cannot work; the same description carries a
// phantom type, so a typed host gets the identical refusal from the compiler.
//
// There is no registry of capability names. The strings are the wrapper's own,
// and a wrapper nobody described is simply transparent to the walk.
// ---------------------------------------------------------------------------

/** The key a description lives under. A symbol, so it can never collide with a handler's own properties. */
export const DESCRIPTION: unique symbol = Symbol.for("kronos.wrapper.description")

/** What a wrapper says about itself. Every list is optional and free-form. */
export type WrapperDescription = {
  /** How the wrapper names itself in a boot error — usually its function name. */
  readonly name: string
  /** Context capabilities this wrapper ADDS (`"db"`, `"log"`, `"trace"`). */
  readonly supplies?: readonly string[]
  /** Context capabilities this wrapper NEEDS from a wrapper outside it. */
  readonly uses?: readonly string[]
  /** Things this wrapper WRITES onto the message before handing it inward (`"message.traceparent"`). */
  readonly stamps?: readonly string[]
  /** Things this wrapper READS off the message; a wrapper inside it must not stamp them. */
  readonly reads?: readonly string[]
  /**
   * Per-capability fix lines for the `uses` rule, printed after the arrow in
   * the boot error. Optional; the walk has a generic sentence otherwise.
   */
  readonly hints?: Readonly<Record<string, string>>
  /** The function this wrapper wrapped — the next link of the chain. */
  readonly next: unknown
}

/**
 * The description a wrapper carries in its TYPE. Phantom: the key is optional
 * and nothing reads it through the type at runtime. `next` is the wrapped
 * function's type, so a rule can look through the whole chain.
 */
export type Described<D extends WrapperDescription> = {
  readonly [DESCRIPTION]?: D
}

/** The description of a described function, or `never`. */
export type DescriptionOf<F> = F extends { readonly [DESCRIPTION]?: infer D }
  ? D extends WrapperDescription
    ? D
    : never
  : never

type Lists<D, Field extends "supplies" | "uses" | "stamps" | "reads", Key extends string> =
  D extends { readonly [P in Field]?: readonly (infer K)[] } ? (Key extends K ? true : false) : false

/**
 * Does `F`, or anything it wraps, declare `key` under `field`? A wrapper that
 * owns an order rule asks this of the function it is about to wrap:
 *
 * ```ts
 * type StampedInside<H> = DeclaresInside<H, "stamps", "message.metadata"> extends true ? "move it outside" : unknown
 * correlatingHandler<H>(next: H & StampedInside<H>)
 * ```
 */
export type DeclaresInside<F, Field extends "supplies" | "uses" | "stamps" | "reads", Key extends string> =
  [DescriptionOf<F>] extends [never]
    ? false
    : Lists<DescriptionOf<F>, Field, Key> extends true
      ? true
      : DeclaresInside<DescriptionOf<F>["next"], Field, Key>

/**
 * Mark `fn` with `description`. Returns the same function, typed as also
 * carrying the description.
 *
 * ```ts
 * return describe(wrapped, { name: "drizzleHandler", supplies: ["db"], next })
 * ```
 *
 * The description is attached as a non-enumerable property under
 * {@link DESCRIPTION}; nothing about the function's behaviour changes.
 */
export function describe<F extends (...args: never[]) => unknown, const D extends WrapperDescription>(
  fn: F,
  description: D,
): F & Described<D> {
  Object.defineProperty(fn, DESCRIPTION, { value: description, enumerable: false, writable: false })
  return fn as F & Described<D>
}

/** The description on `fn`, if it has one. */
export function descriptionOf(fn: unknown): WrapperDescription | undefined {
  if (typeof fn !== "function") return undefined
  return (fn as { [DESCRIPTION]?: WrapperDescription })[DESCRIPTION]
}

/** The described links of a chain, OUTERMOST FIRST. Undescribed links are transparent and skipped. */
export function chainOf(fn: unknown): WrapperDescription[] {
  const links: WrapperDescription[] = []
  let current: unknown = fn
  let guard = 0
  while (current !== undefined && guard++ < 256) {
    const description = descriptionOf(current)
    if (description === undefined) break
    links.push(description)
    current = description.next
  }
  return links
}

/**
 * Every way a described chain cannot work, as the sentences a boot error
 * prints. Empty when the chain is fine.
 *
 * Three rules, all read off the descriptions alone:
 *
 * 1. A wrapper that `uses` a capability needs a wrapper OUTSIDE it that
 *    `supplies` it — outer wrappers run first, so only they can have added
 *    anything to the context by the time the inner one looks.
 * 2. A capability supplied twice means the inner supplier shadows the outer;
 *    one of them is dead.
 * 3. A wrapper that `reads` something off the message must have anything that
 *    `stamps` it OUTSIDE, or it reads the message before the stamp.
 */
export function chainProblems(chain: readonly WrapperDescription[]): string[] {
  const problems: string[] = []
  const suppliedBy = new Map<string, WrapperDescription>()

  chain.forEach((link, index) => {
    for (const used of link.uses ?? []) {
      if (!suppliedBy.has(used)) {
        const hint =
          link.hints?.[used] ??
          `Put the wrapper that supplies "${used}" outside ${link.name}, so it runs first.`
        problems.push(`${link.name} uses "${used}", but nothing outside it supplies it.\n  → ${hint}`)
      }
    }
    for (const supplied of link.supplies ?? []) {
      const earlier = suppliedBy.get(supplied)
      if (earlier !== undefined) {
        problems.push(
          `"${supplied}" is supplied twice (${earlier.name}, then ${link.name}); the inner ${link.name} shadows the outer one.\n` +
            `  → Remove one of them.`,
        )
      } else {
        suppliedBy.set(supplied, link)
      }
    }
    for (const read of link.reads ?? []) {
      for (const inner of chain.slice(index + 1)) {
        if (inner.stamps?.includes(read)) {
          problems.push(
            `${inner.name} stamps ${read}, but ${link.name} reads it outside ${inner.name}, ` +
              `so ${link.name} sees the message before the stamp.\n` +
              `  → Move ${inner.name} outside ${link.name}.`,
          )
        }
      }
    }
  })

  return problems
}

/**
 * The boot error for an entry whose chain has problems, or `undefined`.
 * `label` is how the host named the entry.
 */
export function chainError(label: string, fn: unknown): Error | undefined {
  const chain = chainOf(fn)
  const problems = chainProblems(chain)
  if (problems.length === 0) return undefined
  const shown = chain.map((link) => link.name).join(" → ")
  return new Error(
    `kronos: entry "${label}" is not configured properly.\n` +
      `  chain: ${shown}\n` +
      problems.map((problem) => `  ${problem}`).join("\n"),
  )
}
