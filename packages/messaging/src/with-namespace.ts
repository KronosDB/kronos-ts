import { qn, type Tag } from "@kronos-ts/common"
import {
  command as createCommand,
  event as createEvent,
  query as createQuery,
} from "./descriptor.js"
import type { z } from "zod"

/**
 * Creates a namespace-scoped factory for message descriptors.
 * Reduces repetition when defining many messages in the same bounded context.
 *
 * ```typescript
 * const ns = withNamespace("university.courses")
 *
 * const CreateCourse = ns.command("CreateCourse", {
 *   payload: z.object({ courseId: z.string(), name: z.string() }),
 * })
 *
 * const CourseCreated = ns.event("CourseCreated", {
 *   payload: z.object({ courseId: z.string(), name: z.string() }),
 *   tags: (p) => [tag("courseId", p.courseId)],
 * })
 *
 * const GetCourse = ns.query("GetCourse", {
 *   payload: z.object({ courseId: z.string() }),
 * })
 * ```
 */
export function withNamespace(namespace: string) {
  function command<P extends z.ZodType>(name: string, def: {
    payload: P
    version?: string
    routingKey?: string
  }): ReturnType<typeof createCommand<P>>
  function command<P extends z.ZodType, R extends z.ZodType>(name: string, def: {
    payload: P
    result: R
    version?: string
    routingKey?: string
  }): ReturnType<typeof createCommand<P, R>>
  function command(name: string, def: { payload: z.ZodType }) {
    return createCommand({ name: qn(namespace, name), ...def } as never)
  }

  function query<P extends z.ZodType>(name: string, def: {
    payload: P
    version?: string
  }): ReturnType<typeof createQuery<P>>
  function query<P extends z.ZodType, R extends z.ZodType>(name: string, def: {
    payload: P
    result: R
    version?: string
  }): ReturnType<typeof createQuery<P, R>>
  function query(name: string, def: { payload: z.ZodType }) {
    return createQuery({ name: qn(namespace, name), ...def } as never)
  }

  return {
    /** Create a command descriptor in this namespace. */
    command,

    /** Create an event descriptor in this namespace. */
    event<P extends z.ZodType>(name: string, def: {
      payload: P
      version?: string
      tags?: (payload: z.infer<P>) => Tag[]
    }) {
      return createEvent({ name: qn(namespace, name), ...def })
    },

    /** Create a query descriptor in this namespace. */
    query,
  }
}
