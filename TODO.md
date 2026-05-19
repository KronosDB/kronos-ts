# Axon Framework TypeScript — Work Items

## Status

**376 tests passing.** Core framework feature-complete with AF5-aligned configuration API.

---

## Core Framework

### Snapshots
`SnapshotPolicy` interface with triggers: after N events, when sourcing time exceeds threshold, or custom conditions. `SnapshotStore` interface with in-memory and database-backed implementations. Integration with `EventSourcingConfigurer` to configure per-entity snapshot policies. Axon Server snapshot store integration via gRPC.

### Correlation Data Providers
Automatic metadata propagation across message chains. `CorrelationDataProvider` interface that extracts metadata from incoming messages and attaches it to outgoing messages. Built-in providers for correlation ID and trace ID propagation. Registered on the configurer, applied automatically by buses.

### Subscribing Event Processor
Push-based event processor that subscribes directly to the event bus (not the event store). Useful for in-memory event delivery without tracking tokens. Complements the existing `TrackingEventProcessor` and `StreamingEventProcessor`.

### Routing Strategies
`RoutingStrategy` interface for command routing key generation in distributed scenarios. Metadata-based routing, tag-based routing. Integration with the distributed command bus for consistent hashing.

### Handler Enhancer Definitions
`HandlerEnhancerDefinition` interface for wrapping all message handlers with cross-cutting concerns. Used for tracing, security, timeouts, caching. Registered on the configurer, applied to all handlers automatically. Different from interceptors — enhancers wrap the handler itself, interceptors wrap the dispatch.

### Entity Lifecycle Hooks
Lifecycle callbacks on entities: creation, deletion, state transitions. Integration with the repository and state manager. Useful for cleanup and initialization logic.

---

## Extensions

### @axonframework/extensions/opentelemetry
Span creation around command dispatch, query dispatch, and event processing. `SpanFactory` interface for pluggable tracing backends. OpenTelemetry integration as the default. Handler-level span attributes. Trace context propagation across message boundaries.

> **Web framework extensions (fastify / express / hono) — dropped by design.**
> Kronos composability is decoupled from the HTTP layer. The framework
> extensions saved ~1 line over plain wiring while adding a deferred-decorator
> footgun. Recommended pattern: start the app, then register routes against the
> `RunningApp` gateways, co-located with each domain slice. See
> `integrationtests/src/__tests__/e2e-axonserver-http.integration.test.ts`.

### @axonframework/extensions/nestjs
NestJS module with `AxonModule.forRoot()` / `forRootAsync()` registration. Injectable gateways via NestJS DI. Lifecycle integration via `OnModuleInit` / `OnModuleDestroy`. Decorator support for handler registration from NestJS services.

### @axonframework/extensions/nextjs
Singleton pattern via `globalThis` for surviving HMR and webpack chunking. Lazy initialization on first request. Gateway getters that await initialization. Works in Server Components and API Routes.

### @axonframework/extensions/drizzle
Drizzle ORM integration: transaction manager that wraps Drizzle transactions, SQL-based token store using Drizzle schema, projection helpers for common read model patterns.

### @axonframework/extensions/knex
Knex integration: transaction manager using Knex transactions, SQL-based token store with Knex migrations, query builder integration for projections.

### @axonframework/extensions/prisma
Prisma integration: transaction manager wrapping Prisma interactive transactions (`$transaction`), token store using Prisma models, projection helpers.

### @axonframework/extensions/typeorm
TypeORM integration: transaction manager using TypeORM's `EntityManager` transactions, token store as a TypeORM entity, repository-style projection support.

### @axonframework/extensions/kysely
Kysely integration: transaction manager wrapping Kysely transactions, type-safe SQL token store, query builder integration for projections.

### @axonframework/extensions/mikro-orm
MikroORM integration: transaction manager using MikroORM's unit of work, token store as a MikroORM entity, flush-based projection patterns.

---

## Integration Test Suite

### University Demo Integration Tests
Full end-to-end integration test suite using testcontainers (Axon Server + PostgreSQL). Serves as both comprehensive test coverage AND reference example application. Tests:
- Command → event → projection flow
- Business rule enforcement
- Concurrency conflict handling
- Event processor delivery and position tracking
- Subscription queries with live updates
- Replay/reset scenarios
- Dead letter queue behavior
- Process manager / automation patterns
- Given-When-Then fixture assertions
- Error paths and recovery
- Real database-backed projections via Drizzle/Knex
- Transaction atomicity testing (projection + token in same DB transaction)
- Token store persistence and resume-from-position testing
