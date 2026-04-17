import { describe, expect, it } from "bun:test"
import {
  KRONOS_APPLICATION,
  KRONOS_COMMAND_GATEWAY,
  KRONOS_QUERY_GATEWAY,
  KRONOS_CONFIGURER,
  KronosModule,
} from "../kronos-module.js"

describe("KronosModule", () => {
  it("exports injection tokens", () => {
    expect(KRONOS_APPLICATION).toBeDefined()
    expect(KRONOS_COMMAND_GATEWAY).toBeDefined()
    expect(KRONOS_QUERY_GATEWAY).toBeDefined()
    expect(KRONOS_CONFIGURER).toBeDefined()
    expect(typeof KRONOS_APPLICATION).toBe("symbol")
  })

  it("forRoot returns a module definition with providers", () => {
    const module = KronosModule.forRoot({
      configure: () => {},
    })

    expect(module.global).toBe(true)
    expect(module.providers).toBeDefined()
    expect(module.exports).toContain(KRONOS_APPLICATION)
    expect(module.exports).toContain(KRONOS_COMMAND_GATEWAY)
    expect(module.exports).toContain(KRONOS_QUERY_GATEWAY)
  })

  it("forRootAsync returns a module definition with async providers", () => {
    const module = KronosModule.forRootAsync({
      inject: ["ConfigService"],
      useFactory: (config: any) => {
        const { EventSourcingConfigurer } = require("@kronos-ts/eventsourcing")
        return EventSourcingConfigurer.create()
      },
    })

    expect(module.global).toBe(true)
    expect(module.providers).toBeDefined()
    expect(module.exports).toContain(KRONOS_APPLICATION)
  })
})
