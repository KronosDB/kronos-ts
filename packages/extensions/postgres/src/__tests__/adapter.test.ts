import { describe, it, expect } from "bun:test"
import { IsolationLevel } from "../adapter.js"
import type {
  PostgresAdapter,
  PostgresAdapterTransaction,
  ListenSubscription,
  QueryRow,
} from "../adapter.js"

describe("IsolationLevel enum", () => {
  it("declares the three Postgres isolation levels the engine ever uses", () => {
    expect(IsolationLevel.READ_COMMITTED).toBe("READ COMMITTED")
    expect(IsolationLevel.REPEATABLE_READ).toBe("REPEATABLE READ")
    expect(IsolationLevel.SERIALIZABLE).toBe("SERIALIZABLE")
  })

  it("exposes only those three (no surprise additions that the SQL builder would not know how to emit)", () => {
    const values = Object.values(IsolationLevel)
    expect(values).toHaveLength(3)
    expect(values.sort()).toEqual(["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
  })
})

describe("PostgresAdapter interface (structural)", () => {
  it("compiles against a hand-rolled stub that implements every required method", () => {
    // If any method is missing from the interface, this stub will fail tsc.
    // The runtime assertion just confirms the stub is shaped right.
    const stub: PostgresAdapter = {
      async query<R>(_sql: string, _params?: unknown[]): Promise<R[]> {
        return []
      },
      async queryOne<R>(_sql: string, _params?: unknown[]): Promise<R | null> {
        return null
      },
      async transaction<T>(
        _isolationLevel: IsolationLevel,
        fn: (tx: PostgresAdapterTransaction) => Promise<T>,
      ): Promise<T> {
        return fn({
          async query<R>(_sql: string, _params?: unknown[]): Promise<R[]> {
            return []
          },
        })
      },
      async listen(
        _channel: string,
        _onNotification: (payload: string | undefined) => void,
      ): Promise<ListenSubscription> {
        return { async unlisten() {} }
      },
      async connect(): Promise<void> {},
      async disconnect(): Promise<void> {},
    }
    expect(typeof stub.query).toBe("function")
    expect(typeof stub.queryOne).toBe("function")
    expect(typeof stub.transaction).toBe("function")
    expect(typeof stub.listen).toBe("function")
    expect(typeof stub.connect).toBe("function")
    expect(typeof stub.disconnect).toBe("function")
  })

  it("PostgresAdapterTransaction exposes query but NOT transaction (no nested transactions)", () => {
    const tx: PostgresAdapterTransaction = {
      async query<R>(_sql: string, _params?: unknown[]): Promise<R[]> {
        return []
      },
    }
    expect("query" in tx).toBe(true)
    expect("transaction" in tx).toBe(false)
  })

  it("ListenSubscription exposes unlisten()", () => {
    const sub: ListenSubscription = { async unlisten() {} }
    expect(typeof sub.unlisten).toBe("function")
  })

  it("QueryRow is a structural alias for plain records (verified by structural assignment)", () => {
    const row: QueryRow = { id: 1, name: "x" }
    expect(row.id).toBe(1)
  })
})
