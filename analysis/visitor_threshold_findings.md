# Note: is a per-IP activity threshold for "visitors" defensible?

Companion finding to `assess_visitor_threshold.py`. Question: should the visitor /
`number_of_requesters` count exclude low-activity IPs (drive-by probes, one-off
previews), with the exclusion threshold found as a "valley" like the 8-hour session
timeout? Short answer: **no request-count threshold is defensible, but a categorical
"streamed vs. never-streamed" split is — and it's large.**

## Method

`assess_visitor_threshold.py` aggregates four per-IP metrics over the extraction
cache — total requests, streaming requests, streaming sessions (8 h gap, the shipped
`number_of_views` definition), and distinct assets — and for each reports the
distribution, the CCDF (a straight log-log line ⇒ power law ⇒ no valley), a
min-density valley / knee finder, and a visitor-count-vs-threshold sweep.

## Findings (one run: 584,337 unique IPs)

- **No valley.** For `total_requests`, the fraction of total activity retained is
  **99.99 % at every threshold K from 2 to 100** — the signature of a pure heavy
  tail. Most IPs are trivial; almost all *activity* is in the tail; there is no
  non-arbitrary cutpoint. A request-count threshold would be a number pulled from a
  hat. (The valley finder degenerates and the knee pins near K = 1, as expected.)
- **A request-count cutoff would discard real access.** 68.8 % of IPs (402,065) made
  exactly one request, but a single request is very often *one full download* — a
  genuine one-file user, not a bot. `total_requests >= K` throws those out too.
- **The real structure is categorical, not a threshold.** Only **94,374 IPs
  (16.2 %) ever made a streaming request**; ~84 % only downloaded or probed. That is
  a clean behavioral partition, not a fuzzy tail cut.

## Recommendation

Do not curtail `number_of_requesters` with a request-count threshold. Instead add a
**complementary** metric reported alongside the (unchanged) total unique-requester
count:

  number_of_viewers = unique IPs with >= 1 streaming session

- ~94,374 vs. 584,337 — a meaningful "engaged audience" number.
- Needs **no arbitrary K**: the cutoff is the categorical stream-vs-not distinction.
- Reuses the shipped `number_of_views` session logic (a viewer is an IP with >= 1
  view), and is privacy-safe (large aggregate).

## De-botting within viewers

If further trimming of monitoring bots is wanted, a per-IP session-count threshold
is the wrong tool: `n_sessions >= 2` drops viewers 94k → 38k (−60 %) while keeping
**99.5 % of all sessions**, and a bot with one endless session is not caught by it
anyway. Asset-level bot exclusion (see `testing_blobs.txt`) is the right mechanism.
