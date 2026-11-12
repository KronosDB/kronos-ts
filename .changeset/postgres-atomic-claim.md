---
"@kronos-ts/postgres": patch
---

`postgresTokenStore`'s `claimToken` claims in one statement. Several owners claiming a free segment at once each got the claim, so two nodes could handle the same segment until one of them failed to write its token; now exactly one gets it and the others get `UnableToClaimTokenError`. A claim on a segment with no row yet no longer fails the others with a primary-key violation.
