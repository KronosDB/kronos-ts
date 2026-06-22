import { qn, emptyMetadata } from "@kronos-ts/common"
import { createDeadLetter, type DeadLetter } from "@kronos-ts/messaging"

/**
 * Shared DDL for the Kronos dead-letter table. Mirrors the column set expected
 * by every persistent SequencedDeadLetterQueue backend (drizzle/kysely/knex/…).
 */
export const DEAD_LETTER_TABLE_DDL = `
  CREATE TABLE IF NOT EXISTS kronos_dead_letters (
    dead_letter_id VARCHAR(255) PRIMARY KEY,
    processing_group VARCHAR(255) NOT NULL,
    sequence_identifier VARCHAR(255) NOT NULL,
    sequence_index INTEGER NOT NULL,
    message TEXT NOT NULL,
    cause_type VARCHAR(255),
    cause_message TEXT,
    diagnostics TEXT NOT NULL,
    enqueued_at VARCHAR(32) NOT NULL,
    last_touched VARCHAR(32) NOT NULL,
    processing_started VARCHAR(32)
  )
`

export const DROP_DEAD_LETTER_TABLE = `DROP TABLE IF EXISTS kronos_dead_letters`

const EVENT_NAME = qn("dlq-it", "SomethingHappened")

/** Build a DeadLetter for sequence `seqId` carrying a payload `value`. */
export function makeDeadLetter(seqId: string, value: string, cause = new Error("boom")): DeadLetter {
  return createDeadLetter(
    {
      identifier: `evt-${seqId}-${value}`,
      name: EVENT_NAME,
      version: "1.0",
      payload: { value },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [{ key: "id", value: seqId }],
    },
    cause,
    seqId,
    { position: 0 },
  )
}

/** Extract the payload value from a parked letter. */
export function valueOf(letter: DeadLetter): string {
  return (letter.message.payload as { value: string }).value
}
