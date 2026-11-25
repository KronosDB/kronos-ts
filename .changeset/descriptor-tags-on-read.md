---
"@kronos-ts/core": patch
---

An event a log read back without tags gets the tags its descriptor gives it before a processor sequences it or hands it to a handler, and before a state's evolver folds it. A `Sequence`, handler or evolver that reads `event.tags` finds them on a log that does not return tags. An event read back with tags keeps them.
