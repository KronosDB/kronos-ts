/**
 * THE ANTI-LAUNDERING PROBE — every family's wrapper, and every order.
 *
 * A compile-time demand is only as good as the types that reach it. A wrapper
 * whose input and output are the same seam but which is typed
 * `(Base) => Base` LAUNDERS: the runtime object still delegates everything the
 * inner one had, but the signature threw the capability away, so a
 * genuinely-capable configuration gets REJECTED by a demand for a capability it
 * actually has. That is worse than having no demand at all, because it is
 * unfixable from the call site.
 *
 * The rule, and this file is what holds every wrapper to it:
 *
 *   - SAME-SEAM WRAPPERS ARE GENERIC IDENTITY — `<T extends Base>(next: T, …) => T`
 *   - CAPABILITY ADDERS ARE ADDITIVE — `<T extends Base>(next: T, …) => T & Capability`
 *
 * so arbitrarily stacked capabilities survive the pipe in BOTH directions.
 *
 * Nothing here runs. It is judged by `bunx tsc --noEmit` through the root
 * `tsconfig.json` `files` array; the connection handles are stood in for,
 * because a type has no use for a live socket.
 */
import type {
  CommandBus,
  EventStore,
  QueryBus,
  ScheduleCapableEventStore,
  SnapshotCapableEventStore,
} from "@kronos-ts/core"
import {
  correlating,
  inMemoryEventStore,
  inMemorySchedulingEventStore,
  inMemorySnapshottingEventStore,
  interceptingCommandBus,
  interceptingQueryBus,
  jsonSerializer,
  localCommandBus,
  localQueryBus,
  unitOfWork,
  upcastingEventStore,
  type CorrelatingUnitOfWork,
  type Intercept,
  type CommandMessage,
  type QueryMessage,
} from "@kronos-ts/core"
import {
  postgresEventStore,
  postgresSchedulingEventStore,
  postgresSnapshottingEventStore,
  postgresUnitOfWork,
  type PostgresResource,
} from "@kronos-ts/postgres"
import {
  kronosDbEventStore,
  kronosDbSchedulingEventStore,
  kronosDbSnapshottingEventStore,
} from "@kronos-ts/kronosdb"
import { axonServerEventStore, axonServerSnapshottingEventStore } from "@kronos-ts/axon-server"
import { recordingCommandBus, recordingEventStore, recordingQueryBus } from "@kronos-ts/test"
import { otlpCommandBus, otlpQueryBus, type OtlpExporter } from "@kronos-ts/otlp"

/** Stood in for: a type has no use for a live socket. */
declare const pg: PostgresResource
declare const kdb: Parameters<typeof kronosDbEventStore>[0]
declare const axon: Parameters<typeof axonServerEventStore>[0]
declare const tagResolver: Parameters<typeof postgresEventStore>[1]["tagResolver"]

const serializer = jsonSerializer()

// ---------------------------------------------------------------------------
// (a) ALL FOUR FAMILY WRAPPERS ADD THE CAPABILITY.
//
// One line of wiring each, one shape each, and the base store in every family
// has never heard of snapshots.
// ---------------------------------------------------------------------------

export const inMemoryCapable: SnapshotCapableEventStore = inMemorySnapshottingEventStore(
  inMemoryEventStore(),
)

export const postgresCapable: SnapshotCapableEventStore = postgresSnapshottingEventStore(
  postgresEventStore(pg, { tagResolver }),
  pg,
  { serializer },
)

export const kronosDbCapable: SnapshotCapableEventStore = kronosDbSnapshottingEventStore(
  kronosDbEventStore(kdb, "default"),
  kdb,
  "default",
)

export const axonServerCapable: SnapshotCapableEventStore = axonServerSnapshottingEventStore(
  axonServerEventStore(axon, "default"),
  axon,
  "default",
)

/** And the bases are NOT capable, so none of the above is vacuous. */
// @ts-expect-error — the base postgres store mentions snapshots nowhere
export const postgresBaseIsNotCapable: SnapshotCapableEventStore = postgresEventStore(pg, {
  tagResolver,
})
// @ts-expect-error — nor does the base kronosdb store
export const kronosDbBaseIsNotCapable: SnapshotCapableEventStore = kronosDbEventStore(kdb, "default")
// @ts-expect-error — nor the base axon-server store
export const axonBaseIsNotCapable: SnapshotCapableEventStore = axonServerEventStore(axon, "default")

// ---------------------------------------------------------------------------
// (b) STACKING SURVIVES BOTH ORDERS.
//
// `upcastingEventStore` is a same-seam wrapper, so it is a generic identity;
// the snapshotting wrappers are adders, so they are additive. Between them,
// every arrangement keeps every capability — which is exactly what makes the
// `ctx.load` demand trustworthy rather than a source of false rejections.
// ---------------------------------------------------------------------------

export const upcastOutsideSnapshots: SnapshotCapableEventStore = upcastingEventStore(
  postgresSnapshottingEventStore(postgresEventStore(pg, { tagResolver }), pg, { serializer }),
  (e) => e,
)

export const snapshotsOutsideUpcast: SnapshotCapableEventStore = postgresSnapshottingEventStore(
  upcastingEventStore(postgresEventStore(pg, { tagResolver }), (e) => e),
  pg,
  { serializer },
)

/** Three deep, in the order a real host writes it. */
export const threeDeep: SnapshotCapableEventStore = upcastingEventStore(
  upcastingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore()), (e) => e),
  (e) => e,
)

// ---------------------------------------------------------------------------
// (b2) TWO TIERS ACROSS TWO PACKAGES, EVERY ORDER.
//
// The store-tier category has TWO members now — snapshotting and scheduling —
// and that is the first time the anti-laundering rule has had to hold between
// wrappers that live in DIFFERENT files and were written months apart. With one
// capability a collapsing wrapper is invisible; with two, a wrapper that
// collapsed to its own capability would keep the one it adds and silently drop
// the other, and the report would read "this handler will not compile against a
// store that obviously works".
// ---------------------------------------------------------------------------

/** The postgres pair, scheduling outermost — the arrangement a host writes. */
const postgresBoth = postgresSchedulingEventStore(
  postgresSnapshottingEventStore(postgresEventStore(pg, { tagResolver }), pg, { serializer }),
  pg,
  { unitOfWork: postgresUnitOfWork(unitOfWork, pg), tagResolver },
)
export const postgresBothSchedules: ScheduleCapableEventStore = postgresBoth
export const postgresBothSnapshots: SnapshotCapableEventStore = postgresBoth

/** The same pair, the other way up. Order is a preference, never a constraint. */
const postgresBothReversed = postgresSnapshottingEventStore(
  postgresSchedulingEventStore(postgresEventStore(pg, { tagResolver }), pg, {
    unitOfWork: postgresUnitOfWork(unitOfWork, pg),
    tagResolver,
  }),
  pg,
  { serializer },
)
export const reversedSchedules: ScheduleCapableEventStore = postgresBothReversed
export const reversedSnapshots: SnapshotCapableEventStore = postgresBothReversed

/** The KronosDB pair — where the schedules ride the log server-side already. */
declare const kdbConnection: Parameters<typeof kronosDbSchedulingEventStore>[1]
const kronosDbBoth = kronosDbSchedulingEventStore(
  kronosDbSnapshottingEventStore(kronosDbEventStore(kdb, "default"), kdb, "default"),
  kdbConnection,
  { serializer },
)
export const kronosDbBothSchedules: ScheduleCapableEventStore = kronosDbBoth
export const kronosDbBothSnapshots: SnapshotCapableEventStore = kronosDbBoth

/**
 * THREE TIERS DEEP, mixing an adder, an adder and an identity — with the
 * identity in the MIDDLE, which is the arrangement that catches a same-seam
 * wrapper that is not generic.
 */
const everything = inMemorySchedulingEventStore(
  upcastingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore()), (e) => e),
)
export const everythingSchedules: ScheduleCapableEventStore = everything
export const everythingSnapshots: SnapshotCapableEventStore = everything

/** And neither tier grants the other, so none of the above is vacuous. */
// @ts-expect-error — the postgres snapshotting wrapper adds no scheduling
export const pgSnapshotIsNotSchedulable: ScheduleCapableEventStore =
  postgresSnapshottingEventStore(postgresEventStore(pg, { tagResolver }), pg, { serializer })

// @ts-expect-error — and the base postgres store is neither
export const pgBaseIsNotSchedulable: ScheduleCapableEventStore = postgresEventStore(pg, {
  tagResolver,
})

// ---------------------------------------------------------------------------
// (c) THE FIXTURE'S OWN COMPOSITION. Recording is an adder too, and the
// recorder sits OUTERMOST so `appended` is what left the fixture — which means
// the snapshotting capability has to survive a layer ABOVE the store that has
// it, or every scope loading a snapshotting state would fail to compile against
// a fixture that serves it perfectly.
// ---------------------------------------------------------------------------

export const recordedCapableStore: SnapshotCapableEventStore = recordingEventStore(
  inMemorySnapshottingEventStore(inMemoryEventStore()),
)

/** …and the recording members are still there, in the same type. */
export const recordedStillRecords: ReadonlyArray<unknown> = recordingEventStore(
  inMemorySnapshottingEventStore(inMemoryEventStore()),
).appended

// ---------------------------------------------------------------------------
// (d) THE BUS SIDE — the correlation demand must survive every wrap too.
//
// Correlation was the FIRST compile-time demand in this codebase, and it is the
// one a laundering bus wrapper breaks. A chain that mints correlating units of
// work still mints them at the far end, however many layers were added.
// ---------------------------------------------------------------------------

const correlatingUow = () => correlating(unitOfWork())
declare const intercept: Intercept<CommandMessage>
declare const interceptQuery: Intercept<QueryMessage>

export const interceptingPreservesCorrelation: CommandBus<CorrelatingUnitOfWork> =
  interceptingCommandBus(localCommandBus(correlatingUow), intercept)

export const interceptingQueryPreservesCorrelation: QueryBus<CorrelatingUnitOfWork> =
  interceptingQueryBus(localQueryBus(correlatingUow), interceptQuery)

/** And recording keeps BOTH the correlation demand and its own members. */
export const recordedCorrelatingBus: CommandBus<CorrelatingUnitOfWork> = recordingCommandBus(
  interceptingCommandBus(localCommandBus(correlatingUow), intercept),
)

export const recordedCorrelatingBusStillRecords: ReadonlyArray<unknown> = recordingCommandBus(
  interceptingCommandBus(localCommandBus(correlatingUow), intercept),
).dispatched

export const recordedCorrelatingQueryBus: QueryBus<CorrelatingUnitOfWork> = recordingQueryBus(
  interceptingQueryBus(localQueryBus(correlatingUow), interceptQuery),
)

/**
 * TRACING PRESERVES IT TOO — the case that was actually broken. `otlpCommandBus`
 * was typed `(CommandBus) => CommandBus`, which erased `U` outright and rebuilt
 * a two-member record besides, so tracing a correlating chain produced a bus no
 * correlating handler would fit behind. The runtime worked; the build did not.
 */
declare const exporter: OtlpExporter

export const tracedChainKeepsCorrelation: CommandBus<CorrelatingUnitOfWork> =
  interceptingCommandBus(otlpCommandBus(localCommandBus(correlatingUow), exporter), intercept)

export const tracedQueryChainKeepsCorrelation: QueryBus<CorrelatingUnitOfWork> =
  interceptingQueryBus(otlpQueryBus(localQueryBus(correlatingUow), exporter), interceptQuery)

/** …and the recorder's members survive tracing, in either order. */
export const tracedRecorderKeepsItsMembers: ReadonlyArray<unknown> = otlpCommandBus(
  recordingCommandBus(localCommandBus(correlatingUow)),
  exporter,
).dispatched

/** A BARE chain is still bare, so the probes above are not vacuous. */
// @ts-expect-error — this chain mints plain units of work
export const bareChainIsNotCorrelating: CommandBus<CorrelatingUnitOfWork> = interceptingCommandBus(
  localCommandBus(unitOfWork),
  intercept,
)

// ---------------------------------------------------------------------------
// (e) AND THE WHOLE POINT: a demand written against a DEEPLY WRAPPED store is
// satisfied. This is the configuration a laundering wrapper would have broken.
// ---------------------------------------------------------------------------

export const deeplyWrappedStillSatisfiesTheDemand = (
  store: SnapshotCapableEventStore,
): EventStore => store

export const proof = deeplyWrappedStillSatisfiesTheDemand(
  recordingEventStore(
    upcastingEventStore(
      postgresSnapshottingEventStore(postgresEventStore(pg, { tagResolver }), pg, { serializer }),
      (e) => e,
    ),
  ),
)

/** The same claim for the second tier, through the same four layers. */
export const deeplyWrappedStillSchedules = (store: ScheduleCapableEventStore): EventStore => store

export const scheduleProof = deeplyWrappedStillSchedules(
  recordingEventStore(
    upcastingEventStore(
      postgresSchedulingEventStore(
        postgresSnapshottingEventStore(postgresEventStore(pg, { tagResolver }), pg, { serializer }),
        pg,
        { unitOfWork: postgresUnitOfWork(unitOfWork, pg), tagResolver },
      ),
      (e) => e,
    ),
  ),
)
