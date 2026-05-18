import { describe, expect, it } from "bun:test"
import {
  KRONOS_APPLICATION,
  KRONOS_COMMAND_GATEWAY,
  KRONOS_QUERY_GATEWAY,
  KRONOS_APP,
  KronosModule,
} from "../kronos-module.js"

describe("KronosModule", () => {
  it("exports injection tokens", () => {
    expect(KRONOS_APPLICATION).toBeDefined()
    expect(KRONOS_COMMAND_GATEWAY).toBeDefined()
    expect(KRONOS_QUERY_GATEWAY).toBeDefined()
    expect(KRONOS_APP).toBeDefined()
    expect(typeof KRONOS_APPLICATION).toBe("symbol")
    expect(typeof KRONOS_APP).toBe("symbol")
  })

  it("forRoot returns a module definition with providers", () => {
    const module = KronosModule.forRoot({
      configure: (_app) => { /* no-op */ },
    })

    expect(module.global).toBe(true)
    expect(module.providers).toBeDefined()
    expect(module.exports).toContain(KRONOS_APPLICATION)
    expect(module.exports).toContain(KRONOS_COMMAND_GATEWAY)
    expect(module.exports).toContain(KRONOS_QUERY_GATEWAY)
  })

  it("forRoot factory invokes configure with the unstarted App", () => {
    let receivedApp: any = undefined
    const module = KronosModule.forRoot({
      configure: (app) => { receivedApp = app },
    })

    // Find the KRONOS_APP provider and invoke its useFactory
    const providers = module.providers as Array<{ provide: any; useFactory: (...args: any[]) => any }>
    const appProvider = providers.find((p) => p.provide === KRONOS_APP)!
    expect(appProvider).toBeDefined()
    const app = appProvider.useFactory()
    expect(app).toBeDefined()
    expect(receivedApp).toBe(app) // same instance handed to configure
    // App must expose the fluent surface
    expect(typeof app.states).toBe("function")
    expect(typeof app.commands).toBe("function")
    expect(typeof app.start).toBe("function")
  })

  it("forRootAsync returns a module definition with async providers", () => {
    const module = KronosModule.forRootAsync({
      inject: ["ConfigService"],
      useFactory: async (_config: any) => {
        const { kronos } = await import("@kronos-ts/core")
        return kronos()
      },
    })

    expect(module.global).toBe(true)
    expect(module.providers).toBeDefined()
    expect(module.exports).toContain(KRONOS_APPLICATION)
  })
})
