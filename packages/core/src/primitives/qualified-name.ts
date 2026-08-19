/**
 * A structured message name consisting of a namespace and a local name.
 * Aligns with Axon Server's wire format where messages are routed by name.
 *
 * The namespace typically represents the bounded context or module,
 * while the name identifies the specific message type.
 */
export type QualifiedName = {
  readonly namespace: string
  readonly name: string
}

/**
 * Creates a {@link QualifiedName} from a namespace and name.
 */
export function qn(namespace: string, name: string): QualifiedName {
  return { namespace, name }
}

/**
 * Serializes a {@link QualifiedName} to its wire format: `"namespace.name"`.
 */
export function qualifiedNameToString(qn: QualifiedName): string {
  return `${qn.namespace}.${qn.name}`
}

/**
 * Parses a dot-separated string into a {@link QualifiedName}.
 * Splits on the last dot, so `"a.b.c"` becomes `{ namespace: "a.b", name: "c" }`.
 */
export function qualifiedNameFromString(fqn: string): QualifiedName {
  const lastDot = fqn.lastIndexOf(".")
  if (lastDot === -1) {
    return qn("", fqn)
  }
  return qn(fqn.slice(0, lastDot), fqn.slice(lastDot + 1))
}

/**
 * Returns true if two {@link QualifiedName}s are equal.
 */
export function qualifiedNamesEqual(a: QualifiedName, b: QualifiedName): boolean {
  return a.namespace === b.namespace && a.name === b.name
}
