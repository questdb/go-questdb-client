# QWP Client NACK Policy v2 — no drop, no lists, no dead senders

Status: implemented (client side).
Tandem: OSS core reserves `STATUS_NOT_WRITABLE = 0x0C` (`QwpStatusNotWritable`).

## Problem (v1)

Mid-stream server NACKs were classified client-side into two policies, both
wrong for store-and-forward:

| v1 policy | Categories | Consequence |
|---|---|---|
| `DROP_AND_CONTINUE` | `SCHEMA_MISMATCH`, `WRITE_ERROR` | **silent data loss** — batch discarded, watermark advanced |
| `HALT` | `PARSE_ERROR`, `INTERNAL_ERROR`, `SECURITY_ERROR`, terminal WS closes, `UNKNOWN` | **sender permanently dead** — next producer call errors |

Concrete failures:

- **Failover killed ingestion.** A read-only/demoting node rejecting writes
  surfaced as `SECURITY_ERROR` → HALT mid-stream, while the *same cluster
  state* at connect time (421 role reject) retried forever under Invariant B.
- **Fail-closed on the unknown.** `UNKNOWN` → HALT meant any new server status
  byte killed old clients (`STATUS_CANCELLED` 0x0A and `STATUS_LIMIT_EXCEEDED`
  0x0B already hit this).
- **Transport codes drove policy.** WS close codes 1002/1003/1007/1008/1009
  were interpreted via a client-side list (`qwpSfIsTerminalCloseCode`) — a
  transport detail leaking into an application-layer decision, blind to
  middlebox closes.

## Design principles (v2)

1. **No silent data loss.** There is no drop policy. Every rejected byte is
   either replayed or halts loudly with the bytes preserved in the SF log.
2. **User code never stops on anything transient.** Outages, failover,
   read-only windows, unknown statuses: the producer keeps writing into SF,
   the client keeps retrying. The only inherent bound is SF disk capacity
   (`sf_append_deadline_millis` backpressure).
3. **The client doesn't guess.** Policy tiers are minimal and deterministic;
   *empirical* poison detection replaces close-code lists.

## The three policies (`Policy`)

| Policy | Behavior | Categories |
|---|---|---|
| `PolicyRetriable` | Recycle the connection (same `qwpSfRetriableRejection` → `connectWithBackoff` machinery as a wire failure: capped backoff + jitter, endpoint rotation via the round walk, no wall-clock give-up), replay from `ackedFsn+1`. Dispatch to `SenderErrorHandler` is informational. | `WRITE_ERROR`, `INTERNAL_ERROR`, `UNKNOWN` (fail open), any WS close without a preceding terminal NACK |
| `PolicyRetriableOther` | Same replay semantics; the node cannot serve writes at all, so rotation matters more than backoff. | `NOT_WRITABLE` (reserved wire byte 0x0C — see below) |
| `PolicyTerminal` | `recordFatalServerError` latch: next producer call returns the typed `*SenderError`; drainer quarantines its slot. Reserved for rejections **deterministic under byte-identical replay**. Bytes stay on disk. | `SCHEMA_MISMATCH`, `PARSE_ERROR`, `SECURITY_ERROR` (ACL denial on a writable node), `PROTOCOL_VIOLATION` (poison escalation) |

## Poison-frame detector (replaces the WS close-code list)

WS close codes carry **zero policy semantics**. Every close is a transport
event → reconnect + replay. The guarded case — a frame that deterministically
kills the connection without a NACK (e.g. an intermediary frame-size limit) —
is caught *behaviorally*:

> A server-active rejection (retriable NACK, or non-orderly close after at
> least one send on the connection) at the same head-of-line FSN, with no ack
> progress in between, counts a strike. `max_frame_rejections` (default 4;
> connect-string key or `WithMaxFrameRejections(int)`) consecutive strikes
> escalate to a typed `PROTOCOL_VIOLATION` TERMINAL naming the FSN. Any ACK
> resets the counter. Orderly closes (`NORMAL_CLOSURE` role-change handoff,
> `GOING_AWAY` restart drain) never count strikes.

This catches everything `qwpSfIsTerminalCloseCode` caught, plus middlebox
closes the list missed, and never false-positives on outages (those fail at
connect, not deterministically on one FSN). `qwpSfIsTerminalCloseCode` is
retained for diagnostics only.

## The read-only / demotion case

The server already handles it at the right layer (Invariant B work): the
read-only gate and the commit-path authorization refusal both close with a
reconnect-eligible `NORMAL_CLOSURE` instead of NACKing `SECURITY_ERROR`. The
client reconnects, hits the 421 role reject on the now-replica, and retries
from SF until a primary is reachable. Consequently:

- `SECURITY_ERROR` mid-stream can only mean **ACL denial on a writable node**
  → TERMINAL is correct.
- `QwpStatusNotWritable` (0x0C) is **reserved**, not emitted: a future server
  may NACK it mid-stream once deployed client fleets classify it as
  retriable-with-rotation. Until then the graceful close covers the case with
  no version-gating problem.

## Behavior changes (release notes)

- **`SCHEMA_MISMATCH`: silent drop → loud TERMINAL.** One table's schema
  drift now halts a multi-table sender — the conscious no-silent-loss trade.
- **`WRITE_ERROR`: silent drop → retry forever** (poison detector bounds the
  deterministic case). A persistent write error blocks the stream until SF
  backpressure; loud stall beats silent loss.
- **`UNKNOWN`: fatal → retry.** Future server statuses degrade to retry.
- **Terminal WS close codes: fatal-on-first-sight → reconnect + poison
  detection.** The former silent-conn-strike heuristic (never-ACK'd drops) is
  subsumed by the detector.
- **Watermark purity:** `ackedFsn` now advances *only* on server OKs.
  `SenderProgressHandler` no longer needs the "settled ≠ durable because of
  drops" caveat, and the durable tracker no longer needs drop placeholders
  (`enqueueEmpty`) or rejection-density tracking (`durableRejectionSeqGap`).
- **Source-breaking:** `Policy{DropAndContinue, Halt}` →
  `Policy{Retriable, RetriableOther, Terminal}`; `CategoryNotWritable` added;
  connect-string `on_*_error` values are now
  `terminal` / `retriable` / `retriable_other` (`halt` / `drop` rejected with
  a migration hint).

## Invariants to hold the line on

- **Replay idempotency:** NACK ⇒ batch atomically not applied server-side.
  RETRIABLE replay depends on it; violating it trades loss for duplication.
- **NACK-before-close:** a server rejecting bytes must NACK before closing;
  a close without a terminal NACK is treated as transport.
- **No watermark advance on any rejection**, ever.

## Test coverage

- `TestQwpSfDefaultPolicyFor` / `TestErrorApiPerCategory(Strict)` — full
  category → policy matrix, incl. `NOT_WRITABLE` and fail-open `UNKNOWN`.
- `TestQwpSfSendLoopRetriableNackRecyclesAndReplays` /
  `TestErrorApiResilience_RetriableStreakThenTerminal` — e2e: RETRIABLE NACK
  → reconnect+replay per strike → genuine terminal or poison escalation;
  watermark untouched.
- `TestQwpSfMaxFrameRejectionsConfigHonoredEndToEnd` — pins
  `max_frame_rejections=2` end-to-end.
- `TestQwpSfCloseCodeRepeatPoisonEscalates` /
  `TestQwpSfPoisonTerminalMultiFrameFsnSpan` — close-code path through the
  detector, multi-frame FSN span on the terminal.
- `TestQwpDurableAckRetriableNackRecyclesAndReplays` /
  `TestQwpDurableAckRejectionNeverAdvancesWatermark` — durable mode: no
  placeholder, no trim, replay behind pending OKs.
