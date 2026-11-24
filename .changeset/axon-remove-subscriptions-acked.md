---
"@kronos-ts/axon-server": minor
"@kronos-ts/kronosdb": patch
---

**Breaking:** `PlatformConnection.subscriptionsAcked()` is removed from `@kronos-ts/axon-server`, so `axon.platform.subscriptionsAcked()` no longer exists. Nothing in the package called it. It resolved with `true` as soon as the platform stream had sent its registration, and said nothing about handler subscriptions. `axon.start()` waits for Axon Server to acknowledge those, and `axon.platform.connected` reports whether the platform stream is open.

`@kronos-ts/kronosdb`: comment corrected, no code change. `PlatformConnection.subscriptionsAcked()` reports that KronosDB has accepted the client's registration on the platform stream. Its comment said it reported handler subscriptions and named a file that does not exist.
