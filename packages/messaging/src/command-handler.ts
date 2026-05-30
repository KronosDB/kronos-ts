import type { z } from "zod"
import type { CommandDescriptor } from "./descriptor.js"
import type { EventCriteria } from "./event-criteria.js"
import type { CommandMessage } from "./message.js"

/**
 * A registered command handler — pairs a command descriptor with its handler function.
 * When the descriptor has a result schema, the handler must return that type.
 */
export interface CommandHandlerDefinition<
  P extends z.ZodType = z.ZodType,
  R extends z.ZodType | undefined = undefined,
> {
  readonly kind: "command-handler"
  readonly descriptor: CommandDescriptor<P, R>
  readonly handler: (
    message: CommandMessage<z.infer<P>>,
  ) => R extends z.ZodType ? Promise<z.infer<R>> | z.infer<R> : Promise<void> | void
  readonly appendCondition?: (
    message: CommandMessage<z.infer<P>>,
    sourcedCriteria: EventCriteria,
  ) => EventCriteria
}

/**
 * Defines a command handler.
 *
 * Void command (no result on descriptor):
 * ```
 * commandHandler(ChangeCourseCapacity, async ({ payload, metadata }) => {
 *   const course = await load(Course, { courseId: payload.courseId })
 *   append(CourseCapacityChanged, { courseId: payload.courseId, capacity: payload.capacity })
 * })
 * ```
 *
 * Typed result command (result on descriptor):
 * ```
 * const CreateCourse = command({
 *   name: qn("university", "CreateCourse"),
 *   payload: z.object({ courseId: z.string(), name: z.string() }),
 *   result: z.object({ courseId: z.string() }),
 * })
 *
 * commandHandler(CreateCourse, async ({ payload, metadata }) => {
 *   append(CourseCreated, { ... })
 *   return { courseId: payload.courseId }  // ← must match descriptor's result schema
 * })
 * ```
 *
 * With append condition override:
 * ```
 * commandHandler(CreateCourse, {
 *   handler: async ({ payload, metadata }) => { ... },
 *   appendCondition: (command, criteria) => criteria,
 * })
 * ```
 */
export function commandHandler<P extends z.ZodType>(
  descriptor: CommandDescriptor<P, undefined>,
  handler: (message: CommandMessage<z.infer<P>>) => Promise<void> | void,
): CommandHandlerDefinition<P, undefined>

export function commandHandler<P extends z.ZodType, R extends z.ZodType>(
  descriptor: CommandDescriptor<P, R>,
  handler: (message: CommandMessage<z.infer<P>>) => Promise<z.infer<R>> | z.infer<R>,
): CommandHandlerDefinition<P, R>

export function commandHandler<P extends z.ZodType>(
  descriptor: CommandDescriptor<P, undefined>,
  options: {
    handler: (message: CommandMessage<z.infer<P>>) => Promise<void> | void
    appendCondition?: (
      message: CommandMessage<z.infer<P>>,
      sourcedCriteria: EventCriteria,
    ) => EventCriteria
  },
): CommandHandlerDefinition<P, undefined>

export function commandHandler<P extends z.ZodType, R extends z.ZodType>(
  descriptor: CommandDescriptor<P, R>,
  options: {
    handler: (message: CommandMessage<z.infer<P>>) => Promise<z.infer<R>> | z.infer<R>
    appendCondition?: (
      message: CommandMessage<z.infer<P>>,
      sourcedCriteria: EventCriteria,
    ) => EventCriteria
  },
): CommandHandlerDefinition<P, R>

export function commandHandler(
  descriptor: CommandDescriptor,
  handlerOrOptions: any,
): CommandHandlerDefinition {
  if (typeof handlerOrOptions === "function") {
    return { kind: "command-handler", descriptor, handler: handlerOrOptions }
  }
  return {
    kind: "command-handler",
    descriptor,
    handler: handlerOrOptions.handler,
    appendCondition: handlerOrOptions.appendCondition,
  }
}
