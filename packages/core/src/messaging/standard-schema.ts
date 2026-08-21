/**
 * THE STANDARD SCHEMA INTERFACE — vendored, types only.
 *
 * This is a hand-transcription of the published `@standard-schema/spec`
 * package (https://github.com/standard-schema/standard-schema, v1), which
 * ships nothing but this contract: a single well-known property, `~standard`,
 * that a schema library puts on its schemas so anybody can validate with them
 * and infer from them without knowing which library made them.
 *
 * It is VENDORED rather than depended on because the whole point of the move is
 * that core has no schema library in its dependency list, and a types-only
 * dependency is still a dependency — one that would have to resolve at install
 * time for a package whose runtime never touches it. Ninety lines of contract
 * are cheaper than that, and the contract is frozen: the version field IS the
 * compatibility promise, so a v1 schema written today is a v1 schema forever.
 *
 * The spec is published as `interface` declarations inside a namespace. Here it
 * is `type` aliases at the top level, because the `interface` keyword appears
 * nowhere in this codebase — a shape is a type alias of fields and a single
 * operation is a bare arrow. Structural typing makes the two spellings the same
 * type, which is exactly why a zod, valibot or arktype schema satisfies this
 * one without either side having heard of the other.
 *
 * ```ts
 * import { z } from "zod"                  // or valibot, or arktype, or yours
 * command({ name: qn("university", "CreateCourse"), payload: z.object({ … }) })
 * ```
 */

/**
 * A schema, from any library that speaks the standard.
 *
 * `Input` is what it accepts, `Output` what it produces — they differ only for
 * schemas that transform. Descriptor payload inference reads `Output`.
 */
export type StandardSchemaV1<Input = unknown, Output = Input> = {
  /** The standard properties, under the well-known key. */
  readonly "~standard": StandardSchemaProps<Input, Output>
}

/** Everything the standard asks a schema to expose. */
export type StandardSchemaProps<Input = unknown, Output = Input> = {
  /** The version number of the standard. Always `1` for this contract. */
  readonly version: 1
  /** The vendor name of the schema library — `"zod"`, `"valibot"`, … */
  readonly vendor: string
  /**
   * Validate an unknown value.
   *
   * MAY return a promise: some libraries have async refinements. Every call
   * site here is explicit about which it can take — a serializer's
   * `deserialize` is synchronous by contract, so it rejects a promise with an
   * error naming the schema rather than silently handing back a thenable.
   */
  readonly validate: (
    value: unknown,
  ) => StandardSchemaResult<Output> | Promise<StandardSchemaResult<Output>>
  /**
   * The inferred types, carried as a phantom field: present in the TYPE, never
   * read at runtime. This is where {@link InferOutput} reads from.
   */
  readonly types?: StandardSchemaTypes<Input, Output> | undefined
}

/** What {@link StandardSchemaProps.validate} answers. */
export type StandardSchemaResult<Output> =
  | StandardSchemaSuccess<Output>
  | StandardSchemaFailure

/** Validation succeeded — `issues` is absent, which is how the union narrows. */
export type StandardSchemaSuccess<Output> = {
  /** The typed output value. */
  readonly value: Output
  /** The non-existent issues. */
  readonly issues?: undefined
}

/** Validation failed. */
export type StandardSchemaFailure = {
  /** The issues of failed validation. Non-empty by construction. */
  readonly issues: ReadonlyArray<StandardSchemaIssue>
}

/** One thing that was wrong with the value. */
export type StandardSchemaIssue = {
  /** The error message of the issue. */
  readonly message: string
  /** Where in the value it was wrong, if the library says. */
  readonly path?: ReadonlyArray<PropertyKey | StandardSchemaPathSegment> | undefined
}

/** A path element, for libraries that carry more than a key. */
export type StandardSchemaPathSegment = {
  /** The key representing a path segment. */
  readonly key: PropertyKey
}

/** The phantom type carrier. Never constructed. */
export type StandardSchemaTypes<Input = unknown, Output = Input> = {
  /** The input type of the schema. */
  readonly input: Input
  /** The output type of the schema. */
  readonly output: Output
}

/**
 * What a schema ACCEPTS — the pre-transform side.
 */
export type InferInput<Schema extends StandardSchemaV1> =
  NonNullable<Schema["~standard"]["types"]>["input"]

/**
 * What a schema PRODUCES — the standard's `z.infer`.
 *
 * This is the inference every descriptor uses: `payload: z.object({ id:
 * z.string() })` makes a handler's `message.payload` exactly `{ id: string }`,
 * and the same is true of the valibot or arktype spelling of that schema.
 */
export type InferOutput<Schema extends StandardSchemaV1> =
  NonNullable<Schema["~standard"]["types"]>["output"]
