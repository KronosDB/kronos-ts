import { index, integer, pgTable, primaryKey, text, varchar } from "drizzle-orm/pg-core"

/**
 * The token table this adapter owns.
 *
 * Not a parameter: the columns are not the caller's choice — `drizzleTokenStore`
 * reads and writes exactly these names — so passing a table reference in only
 * offered the chance to pass the WRONG one. It stays exported because a
 * migration generator needs the schema:
 *
 * ```ts
 * // drizzle.config.ts / schema barrel
 * export { kronosTokenEntries } from "@kronos-ts/drizzle"
 * ```
 */
export const kronosTokenEntries = pgTable(
  "kronos_token_entries",
  {
    processorName: varchar("processor_name", { length: 255 }).notNull(),
    segment: integer("segment").notNull(),
    mask: integer("mask").notNull().default(0),
    tokenType: varchar("token_type", { length: 255 }),
    token: varchar("token", { length: 10000 }),
    timestamp: varchar("timestamp", { length: 255 }),
    owner: varchar("owner", { length: 255 }),
  },
  (t) => [primaryKey({ columns: [t.processorName, t.segment] })],
)

/**
 * The dead-letter table this adapter owns.
 *
 * It is not a parameter any more. The columns are not the caller's choice —
 * `drizzleDeadLetterQueue` reads and writes exactly these names — so passing a
 * table reference in only offered the chance to pass the WRONG one. It stays
 * exported because a migration generator needs the schema:
 *
 * ```ts
 * // drizzle.config.ts / schema barrel
 * export { kronosDeadLetters } from "@kronos-ts/drizzle"
 * ```
 */
export const kronosDeadLetters = pgTable(
  "kronos_dead_letters",
  {
    deadLetterId: varchar("dead_letter_id", { length: 255 }).primaryKey(),
    processingGroup: varchar("processing_group", { length: 255 }).notNull(),
    sequenceIdentifier: varchar("sequence_identifier", { length: 255 }).notNull(),
    sequenceIndex: integer("sequence_index").notNull(),
    message: text("message").notNull(),
    causeType: varchar("cause_type", { length: 255 }),
    causeMessage: text("cause_message"),
    diagnostics: text("diagnostics").notNull(),
    enqueuedAt: varchar("enqueued_at", { length: 32 }).notNull(),
    lastTouched: varchar("last_touched", { length: 32 }).notNull(),
    processingStarted: varchar("processing_started", { length: 32 }),
  },
  (t) => [index("kronos_dl_seq").on(t.processingGroup, t.sequenceIdentifier, t.sequenceIndex)],
)
