---
"@kronos-ts/postgres": minor
---

A statement that fails because the server could not be reached or could not serve it now carries core's `unavailable` kind, so `isUnavailable` recognises it, on all three drivers. That covers refused or lost connections, SQLSTATE class 08, a server shutting down or starting (57P01–57P03), too many connections (53300), and the statement, lock and idle-in-transaction timeouts the pool arms (57014, 55P03, 25P03). The driver's error is marked in place, so `.code` is unchanged.
