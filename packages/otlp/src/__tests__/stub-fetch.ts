// Container-free test rig: the collector is a captured `globalThis.fetch`.
// Every test installs one, asserts on the JSON it received, and restores the
// original in `afterEach` — no OTel SDK, no exporter double, no network.

export interface OtlpPost {
  readonly url: string
  readonly body: any
}

export interface FetchStub {
  readonly posts: OtlpPost[]
  /** Bodies POSTed to `/v1/traces`. */
  traces(): any[]
  /** Bodies POSTed to `/v1/metrics`. */
  metrics(): any[]
  /** Every span across every trace POST, in order. */
  spans(): any[]
  /** Every metric across every metrics POST, in order. */
  allMetrics(): any[]
  restore(): void
}

export function stubFetch(options: { fail?: boolean } = {}): FetchStub {
  const posts: OtlpPost[] = []
  const original = globalThis.fetch

  globalThis.fetch = (async (input: any, init: any) => {
    posts.push({ url: String(input), body: JSON.parse(String(init?.body)) })
    if (options.fail) throw new Error("collector unreachable")
    return new Response("{}", { status: 200 })
  }) as typeof fetch

  const bodiesFor = (suffix: string) =>
    posts.filter((post) => post.url.endsWith(suffix)).map((post) => post.body)

  return {
    posts,
    traces: () => bodiesFor("/v1/traces"),
    metrics: () => bodiesFor("/v1/metrics"),
    spans: () =>
      bodiesFor("/v1/traces").flatMap((body: any) =>
        body.resourceSpans.flatMap((rs: any) => rs.scopeSpans.flatMap((ss: any) => ss.spans)),
      ),
    allMetrics: () =>
      bodiesFor("/v1/metrics").flatMap((body: any) =>
        body.resourceMetrics.flatMap((rm: any) => rm.scopeMetrics.flatMap((sm: any) => sm.metrics)),
      ),
    restore: () => {
      globalThis.fetch = original
    },
  }
}

/** Attribute lookup over an OTLP `[{key, value}]` list. */
export function attribute(attributes: any[], key: string): any {
  const found = attributes.find((entry) => entry.key === key)
  return found?.value
}

export const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
