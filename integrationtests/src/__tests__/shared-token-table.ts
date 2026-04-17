/**
 * Shared DDL for the Kronos token entry table.
 * Used by all ORM integration tests to create the table in PostgreSQL.
 * Schema aligned with Kronos Framework's TokenEntry.
 */
export const TOKEN_TABLE_DDL = `
  CREATE TABLE IF NOT EXISTS kronos_token_entries (
    processor_name VARCHAR(255) NOT NULL,
    segment INTEGER NOT NULL,
    mask INTEGER NOT NULL DEFAULT 0,
    token_type VARCHAR(255),
    token VARCHAR(10000),
    timestamp VARCHAR(255),
    owner VARCHAR(255),
    PRIMARY KEY (processor_name, segment)
  )
`

export const DROP_TOKEN_TABLE = `DROP TABLE IF EXISTS kronos_token_entries`
