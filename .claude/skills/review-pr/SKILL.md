---
name: review-pr
description: Review a GitHub pull request against the go-questdb-client coding standards
argument-hint: [PR number or URL] [--level=0..3]
allowed-tools: Bash(gh *), Read, Grep, Glob, Agent
---

Review the pull request `$ARGUMENTS`.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. `go-questdb-client` is mission-critical software — bugs can cause data loss, silent data corruption, dropped rows, or crashes (a panic in a background goroutine takes down the *host* application, not just the client) in customer Go services across HTTP/TCP ILP and the QWP columnar protocol. There is zero tolerance for correctness issues, goroutine/connection leaks, data races, or wire-format errors. Be critical, thorough, and opinionated. Your job is to catch problems before they ship, not to be nice.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on a contract the diff quietly changed. Treat the diff as the entry point, not the scope.
- **Flag every issue you find**, no matter how small. Do not soften language or hedge. Say "this is wrong" not "this might be an issue".
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, work through:
  - Inputs: which values break this? Empty buffers, zero-length strings, boundary integers, max-length symbols, names containing the disallowed-character set, NaN/Inf floats, nil slices/maps, zero-value timestamps.
  - Encoding: how does the code behave with invalid UTF-8, embedded NUL bytes, oversized lengths, or a string that needs escaping in ILP vs QWP framing?
  - Concurrency: what happens under concurrent calls to the same sender, an auto-flush firing during a fluent call, the QWP send-loop goroutine racing the producer, a context cancelled mid-flush, `Close()` racing an in-flight flush?
  - Failure modes: connection dropping mid-flush, partial write, TLS handshake failure, auth rejection, server-side QWP rejection (`*SenderError`, retriable recycle+replay vs terminal latch), reconnect + replay from `engineAckedFsn()+1`, poison-frame escalation, disk-backed segment-file (`sf_dir`) I/O errors.
  - Callers: what happens when a caller ignores the returned `error`, reuses a sender after a latched terminal error instead of rebuilding it, type-asserts `LineSender` to `QwpSender` when the transport is HTTP, or shares one sender across goroutines without synchronization?
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing doc comments on exported API changes, a new ILP column type that didn't update all six concrete structs or the `export_test.go` switch helpers, a new config key not added to `conf_parse.go`.
- **Verify every claim.** If the PR title says "fix", verify the bug existed and the fix is correct. If it says "improve performance", look for a benchmark or reason about the algorithmic change — and check `BenchmarkQwpSenderSteadyState` still holds 0 allocs/op. If it says "simplify", verify the new code is actually simpler and drops no behavior. Treat the PR description as an unverified hypothesis.
- **Read the full context of changed files** when the diff alone is ambiguous. Use Read/Grep/Glob to inspect surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem requires physically impossible conditions (a buffer larger than `math.MaxInt`, a NUL injected through an API that already rejects it via name validation, a panic behind a validation guard that all callers pass through), it is not a real finding — drop it. Focus on bugs real workloads trigger, not theoretical edge cases.
- **Panics that guard library-internal invariants are valid.** A `panic` on a "this should never happen given our own invariants" condition is the preferred mechanism for library-internal bugs. Do NOT flag it as insufficient. Only flag a `panic` (or unchecked slice index, nil-map write, or `, ok`-less type assertion) if a caller honoring the documented contract — including the disallowed-character rules documented on each `LineSender` method — can plausibly trigger it. **The fluent-API error-latching convention is intentional, not a missing-return bug:** `Table` / `Symbol` / `*Column` deliberately keep returning the sender and surface the latched error on the next `At` / `AtNow` / `Flush`. Do not flag a method for "swallowing" an error if it latches per this convention; *do* flag it if it latches in a way that loses the error or fails to surface it on the next terminal call.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level token before feeding the remainder (PR number or URL) to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs: QWP wire format / cursor engine / send loop (`qwp_wire.go`, `qwp_encoder.go`, `qwp_sf_*.go`), ILP wire format (`buffer.go`, any V1/V2/V3 change), the `LineSender` interface or the six `{http,tcp}LineSender{,V2,V3}` structs or the `export_test.go` switch helpers, authentication/TLS, sender/buffer state-machine changes, the conf parser (`conf_parse.go`), or any change to goroutine lifecycle / channel protocols / mutex ordering.

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 4. Skip Step 2.5. Skip Step 3 — no agent spawn; review the diff inline in the main loop, using Read/Grep on demand to resolve ambiguities. Skip Step 3b — verify each finding inline as you write it. Single-pass review covering correctness, panic/crash surface, concurrency, tests, and coding standards on the diff itself. |
| **1** | Adds Step 2.5a (semantic delta only — skip 2.5b/2.5c/2.5d). In Step 3, launch only Agent 1 (correctness), Agent 2 (panic/crash surface), and Agent 7 (tests) in parallel. Skip all other agents. Skip Step 3b — verify findings inline as you draft the report. |
| **2** | Full Step 2.5, but in 2.5b restrict the callsite inventory to exported symbols plus everything re-exported through `export_test.go`. In Step 3, launch Agents 1-8. Skip Agent 9 (cross-context) and Agent 10 (adversarial fresh-context). Step 3b uses a single batched verification agent for all findings instead of one per finding. |
| **3** | Every step below as written, all 10 agents, per-finding verification. The full mission-critical pass. |

State the chosen level in one line at the start of the review so the user knows what they're getting (e.g., "Reviewing PR #141 at level 2"). If the level was defaulted, mention that level 3 exists for the full review.

## Step 1: Gather PR context

Capture the PR identifier in `$PR` (the part of `$ARGUMENTS` left after stripping the level token), then fetch metadata, diff, and review comments in a single bash call so `$PR` is in scope for all three `gh` invocations:

```bash
PR='<PR number or URL from $ARGUMENTS, with any --level=N / -lN / bare-digit level token removed>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr view "$PR" --comments
```

## Step 2: PR title and description

Check:
- Title is clear and describes the change
- Description speaks to end-user impact, not implementation internals
- If fixing an issue, `Fixes #NNN` or a link to the issue is present
- Tone is level-headed and analytical
- For public API changes (the `LineSender` / `QwpSender` interfaces, exported `With*` options, a new or renamed config key, a new ILP column type, an `*_integration_test.go` behavior change visible to users), the description calls out the API/behavior change explicitly

## Step 2.5: Map the change surface

Before launching review agents, produce a structured change surface map. This step is mandatory and must use Grep/Glob — do not reason about callsites from memory. The output of this step is required input for every agent in Step 3.

### 2.5a Semantic delta per changed symbol

For every modified or added function, method, interface method, struct field, or exported constant/var, write:

- **Symbol:** fully-qualified name (e.g., `(*qwpLineSender).Flush`, `httpLineSenderV2.column`, `LineSenderFromConf`)
- **Before:** signature, return type, error behavior (returned `error` vs latched `*SenderError` vs retriable recycle), panic behavior, receiver mutation (which fields mutated; pointer vs value receiver), ordering/idempotency/replay guarantees, allocation behavior (hot path vs setup path), goroutine/channel interaction, context handling, lock acquisition
- **After:** same fields
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "improved", "simplified" are not acceptable deltas. State the actual behavioral difference. If nothing semantically changed, write "no behavioral change" — but only after checking, not as a default.

### 2.5b Callsite inventory

For every changed symbol that is exported, re-exported via `export_test.go`, an interface method on `LineSender`/`QwpSender`, a config key, or part of the ILP/QWP wire encoders, run Grep across the entire repository to find every callsite, implementation, or reference outside the diff.

Produce a list grouped by file. The repository is a flat `package questdb` at the root (`*.go`), plus `examples/`, `bench/`, and `test/`. Search at minimum:

- **Production + test callers (root package):** `grep -rn 'SymbolName' *.go`
- **Interface implementations:** every changed `LineSender`/`QwpSender` method must be checked against *all* implementations — the six ILP structs `httpLineSender{,V2,V3}` (`http_sender.go`), `tcpLineSender{,V2,V3}` (`tcp_sender.go`), and the QWP `qwpLineSender` (`qwp_sender.go`)
- **The six-struct + switch-helper invariant:** for a new/changed ILP column type or buffer behavior, `grep -n` the `Messages` / `MsgCount` / `BufLen` / `ProtocolVersion` switches in `export_test.go` and confirm they stay exhaustive over all six structs
- **Config keys:** `grep -n 'keyname' conf_parse.go` — `conf_parse.go` is the single source of truth for supported keys
- **Black-box test surface:** `grep -rn 'SymbolName' export_test.go` (re-exports into `package questdb` for `questdb_test`)
- **Examples and benchmarks (questdb.io renders these):** `grep -rn 'SymbolName' examples/ bench/`
- **Interop conformance:** `grep -rn 'SymbolName' interop_test.go test/interop/`

A changed exported / interface / config-key symbol with zero recorded Grep calls in the trace is a skill violation. The model is not allowed to assert "this is only used here" without showing the search.

### 2.5c Implicit contract list

For each changed symbol, walk this checklist and write one line per item, stating before vs after:

- Panics on which inputs, and whether the panic site runs on a background goroutine (QWP send loop `qwpSfSendLoop`, background drainers, auto-flush) — a panic there crashes the host process with no caller `recover`
- Which `error` values / `*SenderError` categories are returned, and which call-chains propagate vs swallow them; whether the error latches per the fluent-API convention vs surfaces immediately
- Flush ordering, idempotency, replay safety — for QWP, whether cursor frames remain self-sufficient (full schema + full symbol dictionary from id 0 every flush) so reconnect/replay from `engineAckedFsn()+1` and orphan adoption stay safe
- Re-entrancy: calling `Flush`/`Close` from inside a `WithErrorHandler` callback; auto-flush firing mid-fluent-call
- Lock acquisition order and which mutexes are held on return; which channels are read/written and who owns closing them; goroutine spawn/join/leak on every path including error returns
- Context cancellation / deadline propagation (the `ctx` threaded through `NewLineSender`, `Flush`, `engineAppendBlocking`)
- Allocation on the hot path (`Table`→`Symbol`→`*Column`→`At` build path, flush, QWP encode) vs setup path (construction, conf parsing) — the hot path is pinned at 0 allocs/op
- Buffer state on error: does a failed call leave the buffer half-written? Does the sender require close+rebuild after a terminal latch (matches Java; the sender does not auto-resume)?
- Error-policy resolution precedence (highest first): `WithErrorPolicyResolver` → `WithErrorPolicy(category, …)` → connect-string `on_*_error` → `on_server_error` → spec defaults; `PROTOCOL_VIOLATION` forced TERMINAL and `UNKNOWN` forced RETRIABLE (fail open) — never user-configurable
- Wire format: any change to the ILP bytes produced (per protocol version V1/V2/V3) or the QWP frame structure/codec accepted by the server
- `LineSenderPool` is HTTP-only by design — does the change wrongly let a TCP/QWP config through, or break `errHttpOnlySender`?

### 2.5d Cross-context exposure list

End this step with an explicit list of "places this change is visible from but the diff does not touch". This is the highest-priority input for the bug-hunting agents in Step 3.

Group the callsites from 2.5b by execution context. Typical contexts in this codebase:

- **`LineSender` interface surface:** all six ILP structs + `qwpLineSender` — any interface-method change must be correct in all seven
- **`QwpSender` superset:** code that type-asserts `LineSender` to `QwpSender` for QWP-only column types
- **Buffer build hot path:** `Table`, `Symbol`, the `*Column` methods, `At`/`AtNow` and their callers (0-alloc pinned)
- **Flush path:** `Flush`, `FlushAndGetSequence`, `AwaitAckedFsn`
- **Auto-flush path:** the non-blocking `enqueueCursor` path and whatever triggers it
- **QWP cursor engine + send loop:** `qwpSfCursorEngine`, `engineAppendBlocking`, `qwpSfSendLoop`, reconnect/replay, ACK parsing, `engineAckedFsn`/`enginePublishedFsn` (`qwp_sf_*.go`)
- **Background drainer goroutines:** orphan-slot adoption (`qwp_sf_orphan.go`, `qwp_sf_drainer.go`, `qwp_sf_round_walk.go`), visible via `QwpSender.BackgroundDrainers()`
- **Disk-backed segments:** `sf_dir` set → `<sf_dir>/<sender_id>/*.sfa` (the per-sender directory is itself the slot), on-disk-compatible with the Java client's `MmapSegment.java`
- **Configuration parsing:** `LineSenderFromConf`, `conf_parse.go`
- **Authentication / TLS:** TLS config, basic/token auth on HTTP/TCP, QWP handshake
- **Error callback:** `WithErrorHandler` async path, plus producer-side `errors.As` after `Flush`/`FlushAndGetSequence`
- **Connection pool:** `sender_pool.go` (`LineSenderPool`), HTTP-only
- **Examples & benchmarks:** `examples/{from-conf,http,qwp,tcp}`, `bench/` — referenced by `examples.manifest.yaml`
- **Interop conformance:** `interop_test.go` + the `test/interop/questdb-client-test` submodule (ILP vectors shared across QuestDB clients)

Every entry on this list must be reviewed in Step 3.

## Step 3: Parallel review

Every agent receives:
1. The PR diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list)

### Anti-anchoring directive (applies to all agents)

- **Bugs at callsites outside the diff outrank bugs inside the diff.** A confirmed bug in a file the PR did not touch but that calls a changed symbol is a P0 finding.
- **"Looks correct in isolation" is not a valid conclusion.** Before clearing a changed symbol, the agent must walk the callsite inventory from 2.5b and explicitly state, per callsite, whether the new behavior is still correct there.
- **The diff is the entry point, not the scope.** If the change surface map shows the symbol is reachable from N other files, the review covers N+1 files.
- A single finding of the form "in `tcp_sender.go` the new behavior of `buffer.column` causes Y in `tcpLineSenderV3`" is worth more than five findings inside the diff.

### Agents

Launch the following agents in parallel.

**Agent 1 — Correctness & bugs:** nil handling at API boundaries, edge cases, logic errors, off-by-one, operator precedence, error paths, integer overflow/truncation (buffer length math, FSN/sequence arithmetic, varint/length-prefix encoding), wrong wire bytes. Verify ILP encoding per protocol version (V1 text-only, V2 binary float64 + n-dim float arrays, V3 decimals) and QWP frame/codec correctness. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite. When the diff touches the SF sender, the send loop / background or orphan drainers (`qwp_sf_send_loop.go`, `qwp_sf_drainer.go`, `qwp_sf_orphan.go`), primary reconnect/failover (`qwp_sf_round_walk.go`), or pool startup (`lazy_connect` / `initial_connect_retry` / `qwpSenderPool` / `qwpQueryPool`), also verify the "Store-and-forward & pool startup invariants" checklist — a running send loop or drainer that propagates a transport error to the producer, imposes a reconnect time budget, or hard-fails on a transient outage is a Critical (data-loss) finding.

**Agent 2 — Panic & crash surface:** A panic on a background goroutine aborts the host process with no recovery. Flag every reachable instance of:

- **Panic sources:** nil pointer / nil receiver dereference, slice/array index or slice expression out of bounds, write to a nil map, `, ok`-less type assertion (especially `LineSender`→`QwpSender`), integer divide-by-zero, `make` with a negative or untrusted-huge size, string→int conversions assumed infallible.
- **Channel misuse:** send on a closed channel, close of a closed/nil channel, close from the wrong side, double close — especially around `qwpSfSendLoop`, drainers, and shutdown/`Close()`.
- **Goroutine-crash propagation:** a panic in `qwpSfSendLoop`, a background drainer, an auto-flush goroutine, or any goroutine spawned by the client crashes the *whole application*. This is the Go analog of "a panic crossing the FFI boundary" — there is no caller-side `recover`. Verify such goroutines either cannot panic on contract-honoring input or have a deliberate top-level `recover` that converts the panic into a latched error / error-handler call.
- **Panic-in-`defer` during unwind:** a `panic` inside a deferred function while another panic is in flight is unrecoverable. Flag deferred functions that can panic (index, nil-map write, failed type assertion).
- **`unsafe` / unaligned access:** any use of `unsafe`, `reflect`, or pointer arithmetic — verify alignment, lifetime, and that no Go pointer escapes its backing array.
- **Resource-exhaustion crash:** an allocation, slice grow, or `make` sized by an untrusted length parameter (e.g., a server-supplied frame length) — validate the bound before allocating.
- **Unbounded recursion / stack overflow** on attacker- or server-controlled depth (decoders, nested arrays).

Every fallible operation must return `error`, not swallow it. Every client-spawned goroutine must have a defined crash story.

**Agent 3 — Public API & interface conformance:** Verify every changed `LineSender`/`QwpSender` method is implemented correctly and consistently across **all seven implementations** (`httpLineSender{,V2,V3}`, `tcpLineSender{,V2,V3}`, `qwpLineSender`). For a new/changed ILP column type or buffer behavior, verify all six concrete structs *and* the `Messages`/`MsgCount`/`BufLen`/`ProtocolVersion` switches in `export_test.go` were updated and remain exhaustive. For a new/changed config key, verify `conf_parse.go` (the single source of truth) accepts it for the right schemas (`http`,`https`,`tcp`,`tcps`,`ws`,`wss`) and that `NewLineSender`'s `With*` option path stays in sync. Verify HTTP auto-negotiates the protocol version while TCP still requires `WithProtocolVersion`/`protocol_version`. Verify exported identifiers carry doc comments and the QuestDB Apache-2.0 license banner heads any new file.

**Agent 4 — Concurrency & data races:** race conditions on `qwpLineSender` / sender fields, missing synchronization, the producer vs `qwpSfSendLoop` handoff, drainer goroutines vs engine state, `engineAppendBlocking` deadline/backpressure correctness, `sync.Mutex`/`RWMutex` ordering and double-unlock, channel direction/ownership/close discipline, context cancellation racing in-flight flush, `Close()` racing a concurrent `Flush`. Confirm whether `go test -race` would cover the changed paths. For every callsite from 2.5b, check whether the symbol is now reachable from a goroutine/context where the previous synchronization assumptions don't hold.

**Agent 5 — Resource management & leaks:** goroutine leaks on every path (including early `error` returns and terminal latches) — every spawned goroutine must have a join/cancel/exit story; connection/socket cleanup on error and reconnect; `Close()` idempotency and that it drains/stops drainers and the send loop; channel close discipline (no leaked blocked senders/receivers); disk-backed segment-file (`*.sfa`) creation/cleanup/locking under `sf_dir` on error paths; context-cancellation propagation freeing resources; buffer/scratch lifecycle. Walk every callsite from 2.5b that constructs or owns a changed type and verify cleanup on all paths (success, `error` early return, panic-unwind, `Close`).

**Agent 6 — Performance & allocations:** unnecessary allocations on the hot path (`Table`/`Symbol`/`*Column`/`At*` build, flush, QWP encode), excessive copying, inefficient serialization, redundant syscalls, buffer growth strategy. **The `Table`→`Symbol`→`Column`→`At` pipeline is pinned at 0 allocs/op by `BenchmarkQwpSenderSteadyState` / `TestQwpSenderSteadyStateZeroAllocs`** — any new hot-path allocation must move to a reusable scratch buffer on `qwpLineSender` (see the `encodeInfoBuf` pattern). For each new loop on the data path, analyze scaling at realistic volume (millions of rows per flush, hundreds of columns, thousands of symbols); flag any O(n²). Setup-path allocations (construction, conf parsing) are acceptable; data-path allocations are not.

**Agent 7 — Test review & coverage:** coverage gaps, error-path tests, nil/edge-case tests, boundary conditions, regression tests, test quality. Check:
- Unit tests (`*_test.go`, pure ILP tests; QWP unit tests use the `httptest.Server` stand-in `newQwpTestServer` in `qwp_sender_test.go`)
- Integration tests (`*_integration_test.go`) — these need Docker via testcontainers-go; note the live-server vs testcontainer distinction (QWP integration suites can hit a live `localhost:9000`; `TestIntegrationSuite` and the HTTP/TCP suites spin up a real container)
- testify suites dispatch via the top-level `Test*Suite` entry point plus the method name
- Interop conformance: `interop_test.go` + the `test/interop/questdb-client-test` submodule
- `export_test.go` extended (not production code made public) when tests need new internals
- `BenchmarkQwpSenderSteadyState` still asserts 0 allocs/op if the hot path changed
- `examples/` + `bench/` still build and stay consistent with `examples.manifest.yaml`

Cross-reference 2.5d: every cross-context exposure should have a test exercising the changed symbol from that context. Missing tests for cross-context callsites are high-priority findings.

**Agent 8 — Code quality & API design:** exported API ergonomics and consistency, backward compatibility of the `LineSender`/`QwpSender` interfaces and config keys (breaking changes must be intentional and called out in the PR body), naming consistent with the codebase, dead code, unused imports, doc comments on every exported identifier, the Apache-2.0 license banner on new files, the fluent-API error-latching convention preserved on any new method, `go vet ./...` and `staticcheck ./...` clean, `examples.manifest.yaml` paths/filenames stable.

**Agent 9 — Cross-context caller impact:** Walk the callsite inventory from 2.5b. For every callsite, fetch the surrounding code (the calling function plus its callers up two levels) and answer:

- Does this caller pass inputs the new behavior handles incorrectly?
- Does this caller depend on a contract from the implicit contract list (2.5c) the change broke?
- Is this caller in a context (the send-loop or drainer goroutine, auto-flush, holding a mutex, an `error`/terminal path, a hot loop, a `WithErrorHandler` callback, TLS handshake, `Close()`, panic-unwind, the conf parser) where the new behavior misbehaves even with valid inputs?
- For changed interface methods: do all seven `LineSender` implementations still satisfy the new contract? Does the `export_test.go` switch stay exhaustive?
- For changed config keys: does `conf_parse.go` stay the single source of truth, and does the `With*` option path agree?
- For changed buffer/sender/cursor state machines: do all callers respect the new state transitions (buffer cleared after error before reuse; sender rebuilt after a terminal latch; cursor frame still self-sufficient for replay)?

This agent's output is structured per callsite, not per failure mode. Each callsite gets a verdict: SAFE / BROKEN / NEEDS VERIFICATION. Every BROKEN entry is a P0 finding regardless of whether the file is in the diff. Not optional even when the diff is small — small diffs to widely-used symbols (`buffer.column*`, `Flush`, interface methods, the cursor engine) have the largest blast radius.

**Agent 10 — Fresh-context adversarial:** Dispatched separately from agents 1-9 to escape checklist anchoring. Different rules:

- It receives ONLY the PR diff and the names of the changed files. It does NOT receive the change surface map, the implicit contract list, the cross-context exposure list, or any checklist below.
- Its sole instruction: "find ways this code is wrong". No category list, no failure-mode taxonomy, no QuestDB-Go style guide.
- It is free to use Read, Grep, and Glob to explore the repository however it wants.
- Findings are not pre-classified. Each states: what's wrong, why it's wrong, and the code path that demonstrates it.

A finding here that none of agents 1-9 produced is high signal. A finding that overlaps is corroboration. Run in parallel with agents 1-9. Mandatory regardless of diff size.

Combine all agent findings into a single deduplicated **draft** report. Do NOT present this draft to the user yet — it goes straight into verification.

## Step 3b: Verify every finding against source code

The parallel agents work from the diff plus the change surface map and frequently produce false positives — especially around the error-latching convention, goroutine lifecycle, channel ownership, and Go control-flow guarantees. Every finding MUST be verified before it is reported.

For each finding in the draft report:

1. **Read the actual source code** at the exact lines cited. Do not rely on the agent's description alone.
2. **Trace the full code path:** follow callers and interface dispatch. A method called on a `LineSender` value may dispatch to any of the seven implementations — check the one(s) actually reachable.
3. **Check the right implementation(s):** if a finding involves an interface method, confirm it against every implementation the callsite can dispatch to, not just one.
4. **For leak claims:** trace every goroutine to its exit, every connection/file to its close, every channel to its close, on ALL paths (success, `error` early return, terminal latch, panic-unwind, `Close()`). Before claiming a leak between acquisition and cleanup, verify the intervening code can actually fail.
5. **For panic claims:** verify the panic site is actually reachable. Trace control flow backwards — a preceding validation guard (including name-validation rejecting the disallowed-character set), match arm, or early return may make it unreachable.
6. **For goroutine-crash claims:** confirm the panic is reachable on a *client-spawned* goroutine with no top-level `recover`, from contract-honoring input. If a documented validation guard upstream rejects the triggering input, drop it; if the goroutine is the validation boundary, it IS reachable — flag it.
7. **For numeric overflow claims:** check reachability at realistic scale — buffers up to a few hundred MB, millions of rows per flush, columns in the tens to low hundreds, symbol cardinality in the thousands, FSNs growing monotonically over a long-lived sender. If overflow needs values beyond that scale, drop it.
8. **For `unsafe` / race claims:** verify the invariant is actually violated. For races, confirm the two access paths can run concurrently (different goroutines, no intervening happens-before) and whether `go test -race` exercises it.
9. **For error-latching claims:** confirm whether the method follows the intentional fluent-API latching convention (latch, surface on next `At`/`AtNow`/`Flush`). If it does and the error is not lost, it is a FALSE POSITIVE. Only confirm if the error is dropped or never surfaces.
10. **For performance claims:** check whether the cost is measurable on a realistic workload. Downgrade to a nit if negligible relative to surrounding I/O. Exception: any allocation on the pinned 0-alloc hot path is always worth flagging, even a single one — verify against `BenchmarkQwpSenderSteadyState`.
11. **For cross-context findings (Agent 9):** re-read the callsite in full including callers up two levels, and confirm the broken behavior is reachable from production or user-exercised test paths. High-value but easy to overstate — verify carefully.

**Classify each finding** as:
- **CONFIRMED in-diff** — the bug is real and inside the diff
- **CONFIRMED at out-of-diff callsite** — the bug is in an unchanged file because the changed symbol is used there in a now-broken way (cite the file and the 2.5c contract violated)
- **FALSE POSITIVE** — the code is actually correct (explain why)
- **CONFIRMED with nuance** — the issue exists but is less severe than stated (explain)

**Move false positives to a separate "Downgraded" section** at the end. For each, give a one-line explanation of why it was dismissed. This lets the PR author verify the reasoning and catch verification mistakes.

Launch verification agents in parallel where findings are independent. Each should read surrounding source files, not just the diff.

## Review checklists

Review the diff for:

### Correctness & bugs
- nil handling at API boundaries (nil receiver, nil slice/map, nil context, nil channel)
- Edge cases and error paths
- Logic errors, off-by-one, incorrect bounds, wrong operator precedence
- Integer overflow/truncation (buffer size math, length prefixes/varints, FSN/sequence arithmetic)
- Correct ILP wire format per protocol version (V1 text-only, V2 binary float64 + n-dim arrays, V3 decimals) and correct QWP frame/codec bytes
- **Reachability expansion:** for each changed symbol, list the goroutines, error/terminal paths, mutex-held states, and transports it can now appear in but didn't before. Verify it works in each.

### Panic & crash surface
A panic on a client-spawned goroutine aborts the host process. Check for:
- nil deref, out-of-bounds slice/index, nil-map write, `, ok`-less type assertion, divide-by-zero, `make`/slice-grow sized by an untrusted length
- Channel misuse: send-on-closed, double-close, close-from-wrong-side (send loop, drainers, `Close()`)
- Panics in `qwpSfSendLoop` / drainers / auto-flush / any client goroutine with no top-level `recover` — the Go analog of a panic crossing FFI
- Panic-in-`defer` during unwind
- `unsafe`/`reflect`/pointer-arithmetic soundness and alignment
- Unbounded recursion on server/attacker-controlled depth

### Concurrency
- Data races on sender/engine state (would `go test -race` catch it?)
- Producer vs send-loop handoff; drainer vs engine; `engineAppendBlocking` backpressure/deadline correctness
- Mutex ordering, double-unlock, lock held across a blocking channel op or I/O
- Channel direction/ownership/close discipline; no leaked blocked goroutines
- Context cancellation/deadline racing in-flight flush; `Close()` racing `Flush`
- For every changed symbol, whether it is now reachable from a goroutine/context where prior synchronization assumptions don't hold

### Public API & interface conformance
- Every changed `LineSender`/`QwpSender` method correct across all seven implementations
- New/changed ILP column type updates all six structs **and** the exhaustive `export_test.go` switches
- New/changed config key added to `conf_parse.go` (single source of truth) for the right schemas, with `With*` option parity
- HTTP still auto-negotiates protocol version; TCP still requires explicit selection
- Backward compatibility of interfaces/config keys; breaking changes intentional and called out
- Exported identifiers documented; Apache-2.0 banner on new files; fluent-API error-latching preserved on new methods
- `LineSenderPool` stays HTTP-only (`errHttpOnlySender` intact)

### QWP protocol & error semantics
- Cursor frames remain self-sufficient (full schema + symbol dictionary from id 0 every flush) so reconnect/replay from `engineAckedFsn()+1` and orphan adoption stay safe
- `Flush` contract preserved: publishes into the cursor engine and returns **without waiting for the server ACK** (backpressure via `engineAppendBlocking` may block, bounded by `sf_append_deadline_millis`); `FlushAndGetSequence` returns the published FSN upper bound; `AwaitAckedFsn` is the only ACK barrier
- Error-policy precedence intact: `WithErrorPolicyResolver` → `WithErrorPolicy` → `on_*_error` → `on_server_error` → defaults; `PROTOCOL_VIOLATION` forced TERMINAL, `UNKNOWN` forced RETRIABLE (fail open)
- **Mid-stream server NACKs (no drop policy).** The NACK policy must mirror
  the connect-time tiering. A rejection category that a transient cluster
  state can produce (`WRITE_ERROR`, `INTERNAL_ERROR`, `UNKNOWN` — and any
  future status byte) is RETRIABLE: recycle the wire and replay from
  `ackedFsn+1`. It must NEVER drop the batch and NEVER latch a terminal /
  quarantine a slot on first sight. Only rejections deterministic under
  byte-identical replay (`SCHEMA_MISMATCH`, `PARSE_ERROR`, `SECURITY_ERROR`
  on a writable node) may go TERMINAL. A client that advances the ack
  watermark past a NACKed frame is silently losing data — Critical. A frame
  repeatedly rejected with no ack progress must escalate through the
  poison-frame detector (`max_frame_rejections` consecutive strikes at the
  same head FSN), not through a WS close-code list — close codes carry no
  policy semantics. `UNKNOWN` must fail OPEN (retry), never closed
  (terminal): a status byte from a newer server must degrade to retry, not
  to a dead sender.
- TERMINAL latches on the I/O loop and surfaces on the next producer call; no auto-resume (close+rebuild is the only recovery)
- Disk-backed segment files under `sf_dir` stay on-disk-compatible with the Java `MmapSegment.java` layout

### Store-and-forward & pool startup invariants (QWP)
Apply this whenever the diff touches the SF sender, the cursor engine / send
loop (`qwp_sf_send_loop.go`, `qwp_sf_engine.go`), the background or orphan
drainers (`qwp_sf_drainer.go`, `qwp_sf_orphan.go`), primary reconnect/failover
(`qwp_sf_round_walk.go`), `qwpSenderPool` / `qwpQueryPool` startup,
`lazy_connect`, or `initial_connect_retry`. A violation here is a **Critical**
finding: the whole point of store-and-forward is that a running producer never
loses data and never hard-fails on a transient outage.

**Send loop / drainer (steady state — once the sender or pool is running).**
- Once running, the send loop and drainer goroutines ship buffered SF data to
  the server. They MUST NOT propagate server / transport errors back to the
  producer (`At`/`AtNow`/`Flush`, the pooled lease). The ONLY error a running
  drain path may surface to the producer is **SF out of space** (the on-disk /
  RAM backing buffer is full and can accept no more rows —
  `ErrBackpressureTimeout` after `sf_append_deadline_millis`). Flag any other
  failure class (connect-refused, DNS, unreachable/black-hole, TLS/cert, auth,
  role-reject, upgrade/protocol timeout, reset) that can escape onto a producer
  or borrow call.
- Primary reconnect MUST be fully contained inside the send-loop/drainer
  goroutine and MUST have **no time limit** — no
  `reconnect_max_duration_millis`-style budget, no deadline, no "give up and
  latch terminal after N ms". A budget that latches the sender terminal (or
  reintroduces a budget-exhausted terminal event kind) on a long outage is a
  Critical violation: it drops a producer that store-and-forward promised to
  keep alive.
  Flag any bounded reconnect loop, `time.Now().After(deadline)` /
  `deadline`-gated retry, or terminal `*SenderError` reachable from the running
  reconnect path.
- The reconnect loop must retry with **exponential backoff** and handle every
  connect failure class gracefully, without a hard fail — it keeps buffering
  and keeps retrying until the wire is back. The per-attempt backoff may be
  capped (`reconnect_max_backoff_millis`), but the RETRY LOOP ITSELF must be
  unbounded. Flag a capped total retry duration or an attempt-count cap on the
  steady-state path.
- **Sanctioned terminals (orphan-slot drainer only).** The orphan drainer MAY
  quarantine its slot (`.failed` sentinel, human-in-the-loop) on conditions
  that are terminal by design: auth failure, a non-421 upgrade reject, and a
  genuine cluster-wide durable-ack capability gap that exhausted its documented
  settle budget (a cap counted ONLY in consecutive capability-gap sweeps, with
  any wall-clock component anchored at the FIRST capability-gap error of the
  episode). These are NOT violations of the no-budget rule above. Transient
  classes (role reject, transport error) must never increment that budget or
  burn its wall clock — a transient state consuming the terminal budget
  (shared attempt counter, entry-anchored deadline) IS a Critical violation of
  this checklist.

**Pool startup — two modes; the mode decides who sees connectivity errors.**
- `lazy_connect=true`: `NewQuestDB` / `Connect` MUST succeed with **no server
  present**. The producing sender must work immediately (writes buffer via
  SF / the cursor engine), and once the server comes up the read side must
  also connect and read (reads are deferred, not disabled). Verify the build
  does not fail-fast, the sender does not error on the first write while the
  server is down, and a later `BorrowQuery` succeeds once the server is up.
- `lazy_connect=false` (default): the build / initial connect MUST expose
  connectivity problems to the caller — DNS errors, connect-refused /
  unreachable, TLS/cert, authentication/authorization, and connect/upgrade
  timeouts must all surface as a returned error at startup, not be swallowed.
  Verify each of those failure classes reaches the user during initialization.
- **In BOTH modes the boundary is the same:** connectivity errors are only
  ever the caller's problem DURING initialization. Once the client has
  connected and is past initialization, the running send loop/drainer reverts
  to the steady-state contract above — it must NEVER expose transport problems,
  NEVER impose a reconnect time budget, and NEVER hard-fail on a transient
  outage. Anything that undermines the store-and-forward guarantee past init
  is Critical.

### Performance
- No new allocations on the pinned 0-alloc hot path (`Table`/`Symbol`/`*Column`/`At*`) — verify against `BenchmarkQwpSenderSteadyState`; new hot-path scratch must reuse a buffer on `qwpLineSender`
- No regressions on flush/encode paths; minimal copying; sane buffer growth; batched syscalls
- No O(n²) on any data path at realistic scale (millions of rows, hundreds of columns)
- Setup-path allocations (construction, conf parsing) acceptable; data-path allocations not

### Resource management
- Every client-spawned goroutine has a join/cancel/exit story on all paths
- Connections/sockets/TLS sessions and `*.sfa` segment files cleaned up on error and reconnect
- `Close()` idempotent; stops the send loop and drainers; drains or fails cleanly
- No leaked channels or blocked goroutines; context cancellation frees resources

### Test review
- **Coverage gaps:** every new/changed code path has a test; flag "missing test for X" explicitly
- **Cross-context coverage:** every 2.5d entry exercised by a test from that context (missing = high priority)
- **Error-path coverage:** connection drop, partial write, TLS/auth failure, server `*SenderError`, reconnect/replay, HALT, context cancellation — not just the happy path
- **Edge-case tests:** nil inputs, empty buffers, zero-length strings, max-length/disallowed-character names, NaN/Inf, boundary integers
- **Integration tests:** protocol-level changes covered (Docker/testcontainers; mind live-server vs container); interop vectors in `interop_test.go` + submodule still pass
- **Test quality:** assertions check the right thing; no trivially-passing tests; `export_test.go` extended rather than production code made public; benchmark 0-alloc assertion preserved
- **Regression tests:** a bug fix ships a test that fails without the fix

### Unresolved TODOs and FIXMEs
- Scan the diff for `TODO`, `FIXME`, `HACK`, `XXX`, `WORKAROUND`. For each:
  - Pre-existing (moved/reformatted) or newly introduced in this PR?
  - If new: unfinished work that should block merge, or an acceptable known limitation? Flag deferred bugs / incomplete implementations.
  - If it references a ticket/issue, verify the reference exists.

### Commit messages
- Plain English titles, under 50 chars
- Active voice, naming the acting subject

## Step 4: Output

Present ONLY verified findings (false positives are excluded from Critical/Moderate/Minor). Structure as:

### Critical
Issues that must be fixed before merge. Each must include:
- Exact file path and line numbers (including out-of-diff files)
- Whether the finding is **in-diff** or **out-of-diff**
- Code path trace showing why the bug is real
- For out-of-diff findings: the contract from 2.5c that was violated and the callsite that triggers it
- Suggested fix

### Moderate
Issues worth addressing but not blocking.

### Minor
Style nits and suggestions.

### Downgraded (false positives)
Findings from the initial review dismissed after source verification. For each:
- The original claim (one line)
- Why it was dismissed (one line, citing the specific code that disproves it)

### Summary
- One-line verdict: approve, request changes, or needs discussion
- Highlight any regressions or tradeoffs
- State how many draft findings were verified vs dropped as false positives (e.g., "8 findings verified, 4 false positives removed")
- State the in-diff vs out-of-diff split (e.g., "5 findings in-diff, 3 findings out-of-diff"). If the diff is non-trivial and out-of-diff is zero, the cross-context pass likely underran — re-invoke Agent 9 with a wider grep before finalizing.
