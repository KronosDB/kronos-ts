---
"@kronos-ts/core": minor
---

**Breaking:** `CommandDescriptor.routingKey` is removed, with the `routingKey` field of `command({ … })` and `ns.command(name, { … })`. Nothing read it: no bus or transport took a routing key from the descriptor. A command's routing key is `metadata.processingInstructions.routingKey`, given where the command is sent, which the KronosDB and Axon Server command buses already send to the server. Delete `routingKey` from command declarations, and pass the key in the metadata of `send` or `ctx.send` for commands that need one.
