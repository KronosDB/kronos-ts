---
"@kronos-ts/rabbitmq": patch
---

A command or query that times out says what is known about why: `Command X timed out after Nms. A queue is bound for the name, and RabbitMQ does not report whether a handler is consuming it`. It is still an `UnavailableError`, at the bus timeout.

A name whose handler has stopped keeps its queue on the broker, and RabbitMQ accepts a request for it like any other, so the caller cannot be told `no-handler` for it. The transports' code comments say so, and tests against a broker cover both cases: a name no queue is bound for fails at once with `NoHandlerError`, and a stopped handler's queue fails at the timeout with `UnavailableError`. The request expires in the queue, so a handler that starts later does not run it.
