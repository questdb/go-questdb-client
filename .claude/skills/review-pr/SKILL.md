---
name: review-pr
description: Review a GitHub pull request against the go-questdb-client coding standards
argument-hint: [PR number or URL] [--level=0..3]
allowed-tools: Bash(gh *), Read, Grep, Glob, Agent
---

Review the pull request `$ARGUMENTS`.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. `go-questdb-client` is mission-critical software: bugs can cause data loss, silent corruption, dropped rows, or host-process crashes. There is zero tolerance for correctness issues, lost resource ownership, unsafe release, data races, or wire-format errors. Evaluate retained resources against the public contract. Be critical, thorough, and direct.

- **Read the contract at every review level:** [README: QWP shutdown and ownership](../../../README.md#qwp-shutdown-and-ownership), relevant public Go docs, and the PR's declared behavior changes. The README and Go docs define the shutdown contract. Verify that documentation, implementation, tests, and PR claims agree; report discrepancies.
- **The diff is the entry point, not the scope.** Inspect surrounding code and unchanged callers whose assumptions the change affects. Do not clear a change solely because it looks correct in isolation.
- **Verify every claim.** For a fix, establish the original bug and corrected behavior. For performance, inspect measurements and scaling. For simplification, compare affected actors, entry routes, ownership transfers, and authoritative state before/after. Intentional behavior removal must be explicit and reflected in code, tests, callers, and docs.
- **Think adversarially:** empty and boundary inputs; invalid encodings and oversized declared lengths; partial publication/write; transport, authentication, server, and local-storage failures; cancellation, callbacks, concurrent access, and cleanup. Derive supported caller behavior and expected outcomes from the applicable contracts.
- **Check what's missing:** error handling, tests, public docs, affected implementations/wrappers, configuration validation, and test adapters. Discover dependencies rather than assuming a fixed inventory.
- **Establish reachability.** Trace actual callers, validation, configuration, dispatch, and resource limits. Drop claims proved unreachable; do not dismiss unusual inputs merely because they exceed a typical workload.
- **Distinguish panic reachability from containment.** Internal invariant assertions are not defects merely because they panic. Determine whether supported inputs can violate the invariant, and separately verify containment, ownership, and reporting obligations after an injected panic.
- **Respect documented error reporting.** Fluent methods may intentionally latch errors and surface them at a later operation. Verify the documented reporting point and that the error is not lost; do not mistake latching for swallowing an error.
- **Report proven issues plainly; do not praise the code.** Separate verified defects from unresolved questions and validation gaps. Severity depends on impact and likelihood, not location in the diff or number of reviewers agreeing.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`–`3`. **Default to 0.** Strip the level token before passing the PR number or URL to `gh`. If a lone digit could be either the PR number or the level, ask rather than guessing.

The level controls breadth and independent review effort, not the evidence needed for a finding. Reserve level 3 for high-stakes changes to wire/file formats, public interfaces, authentication, configuration semantics, state machines, resource ownership, or concurrency.

**This table is authoritative.** Later step and role instructions apply only when selected. Reviewers receive the map portions produced at that level and perform targeted searches as needed; they must not assume omitted inventory work was done.

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 4. Review inline without delegation or a formal change map. Cover correctness, panic/crash surface, concurrency, tests, and project conventions. Follow callers as needed and verify each finding before reporting. |
| **1** | Adds Step 2.5a only. In Step 3, select Agents 1 (correctness), 2 (panic/crash), and 7 (tests). Verify their findings inline; skip separate Step 3b reviewers. |
| **2** | Full Step 2.5, with the required inventory in 2.5b limited to public and test-exposed symbols; follow internal dependencies as needed to verify their behavior. Select Agents 1–8. Step 3b uses one batched verification reviewer. |
| **3** | Full change map, all ten roles, and per-finding verification in Step 3b. |

State the chosen level at the start. If defaulted, mention that level 3 provides the full review.

## Step 1: Gather PR context and authoritative sources

Capture the PR identifier in `$PR`, then fetch metadata, diff, and review discussion:

```bash
PR='<PR number or URL after removing the level token>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr view "$PR" --comments
```

Ensure the source being inspected corresponds to the PR revision; identify any local differences that affect verification.

Read project guidance, starting with `CLAUDE.md`, for architecture pointers, conventions, and validation entry points. Inspect the public API docs, protocol references, configuration definitions, fixtures, manifests, and CI configuration relevant to the diff. Follow references to their maintained sources and record which sources establish the guarantees under review.

Paths and symbols in this skill are **starting points, not exhaustive inventories**. Discover implementations, wrappers, adapters, test helpers, and build-tagged code from the repository. Resolve commands, pinned tool versions, fixture prerequisites, and supported platforms from project and CI configuration. Treat implementation and tests as evidence of behavior, not permission to contradict a public contract.

## Step 2: PR title and description

Before assessing shutdown claims, read README's shutdown section and relevant method/error/callback Go docs. Distinguish intentional contract changes from accidental regressions. Verify behavior and validation claims against the contract, source, and test evidence.

Check:
- Title clearly describes the change.
- Description explains end-user impact and compatibility consequences, not just implementation mechanics.
- Bug fixes identify the issue or reproducible failure.
- Tone is analytical and claims are supported.
- Public API, configuration, protocol-support, and other user-visible behavior changes are explicit.

## Step 2.5: Map the change surface

Produce the portions required by the selected level using repository search and source inspection, not memory. Record search scope and evidence. Selected Agents 1–9 receive those portions; Agent 10 receives only its neutral source inputs.

### 2.5a Semantic delta per changed symbol

For each modified or added function, method, interface member, field, or exported constant/variable, record:

- **Symbol:** fully-qualified name and source location.
- **Before / after:** signature, error and panic behavior, receiver mutation, ordering/idempotency/replay guarantees, allocations, goroutine/channel interaction, context handling, and lock acquisition, as applicable.
- **Delta:** the actual semantic difference. “Refactored” or “simplified” is not a behavioral description. Say “no behavioral change” only after checking.
- **Contract change, if any:** old guarantee → new documented guarantee → required code/test/caller/doc changes → evidence. For shutdown, distinguish caller responsibilities, panic reachability, wait deadlines, resource lifetime, retry/failure policy, and each API's return/completion meaning. Derive repeated-Close semantics from each method's docs.

### 2.5b Callsite inventory

Within the selected level's scope, search for callers, implementations, and references to changed symbols. At level 3, include internal symbols as well as public and test-exposed ones. Group results by file and execution context. Include relevant:

- Callers across packages and build tags.
- Implementations of the **specific changed interface**, including embedding, pooled leases, wrappers, adapters, and conformance assertions. Do not conflate a base interface with its transport-specific superset.
- Test-only exposure and dispatch helpers; check coverage of the discovered receiver types.
- Configuration parsing, defaults, option setters, validation, and documentation.
- Examples, benchmarks, generated-documentation inputs, and interoperability fixtures.
- Hidden project guidance and build/CI configuration.

Record search scope and results before asserting that a symbol has no other callers. Use available search tools or shell equivalents: evidence matters, not a particular tool name. Follow indirection and interface dispatch where textual symbol search alone would miss an exposure.

### 2.5c Implicit contracts

For each affected symbol, identify the applicable guarantees and whether they change:

- Supported inputs, validation boundaries, panic reachability and containment.
- Immediate versus deferred/latched errors, propagation, policy overrides, and recovery/reuse preconditions.
- Publication ordering, ACK/durability semantics, replay boundaries, and schema/dictionary recovery.
- Callback operations, notification delivery, ownership, and required synchronization.
- Lock ordering, channel ownership, goroutine lifetime, and resource cleanup on every exit.
- Cancellation/deadline effects on waiting, publication/drain, and physical release.
- Allocation and scaling expectations for the particular path; distinguish zero-allocation row building from flush/encoding and setup.
- Buffer state after rejection, partial publication, cancellation, or local failure.
- Wire/file compatibility, transport support, configuration behavior, and pooling restrictions.

Cite maintained contracts rather than copying a policy/category or protocol-version catalog into the review procedure.

### 2.5d Cross-context exposures

List places where the change is visible but the diff does not touch. Derive execution and ownership contexts from the code:

- Which receivers, wrappers, leases, and interface assertions reach it?
- Does it execute during row building, explicit flush, auto-flush, ACK waiting, construction, or shutdown? Which operations can block, and what bounds them?
- Which goroutines perform publication, I/O, reconnect/replay, recovery, and cleanup?
- Can it run under a lock, inside a callback, during cancellation or panic unwind, or while a pool entry is being created, returned, or retired?
- Which resources and persistent state can outlive the caller?
- Which configurations, transports, platforms, and shipped callers expose it?

Review every relevant discovered exposure. Record unverified paths as coverage gaps, not safe paths.

## Step 3: Independent review

Use only the roles selected by the level table, through the environment's supported delegation workflow and resource limits. These are responsibilities, not assumed executable agent names. Keep reviewers read-only and independent; run selected roles in parallel where supported. Report unavailable roles or incomplete coverage rather than silently claiming they ran.

Selected Agents 1–9 receive the diff, available change-map portions and their scope limits, relevant contract sources, and declared behavior changes. Each follows callers as needed to substantiate findings even when no inventory was required at that level.

### Anti-anchoring rules

- Search beyond the diff and inspect caller assumptions and actual dispatch paths.
- Check relevant exposures before clearing a changed symbol; state verification gaps.
- Assess severity from reachability, data/resource consequences, scope, and recoverability. An out-of-diff defect is not automatically P0.
- Coverage is evidence, not a finding quota. Zero findings can be valid; agreement between reviewers is not proof.

### Roles

**Agent 1 — Correctness & bugs:** API boundaries, edge cases, partial failures, logic, bounds, integer arithmetic, and serialization. Verify callers against applicable API/protocol contracts. For persistence, reconnect, or pool changes, apply the specialized checks below. Trace claimed data loss or terminal failure to an observable consequence.

**Agent 2 — Panic & crash surface:** derive containment, recovery, ownership, and reporting obligations from the public contract. Check:
- nil dereferences, out-of-bounds indexing/slicing, nil-map writes, unchecked assertions, division by zero, and allocation sizes derived from untrusted values.
- Channel close/send ownership, double close, and blocked goroutines.
- Panic propagation: `recover` applies within the panicking goroutine; an uncontained panic can terminate the host process. Trace reachable panics to their actual recovery boundary or process termination.
- Deferred cleanup: **a panic raised during unwind can be recovered in Go**. Trace defer ordering, recovery boundaries, retained resources, and preservation of the failure cause instead of assuming a second panic is unrecoverable.
- `unsafe`, reflection, pointer arithmetic, alignment, and backing-storage lifetime.
- Resource exhaustion and unbounded recursion on server/attacker-controlled sizes or depth.

Assess original-panic reachability separately from injected-panic tests. Verify ownership and truthful reporting after containment; recovery alone is not proof of safe cleanup.

**Agent 3 — Public API & interface conformance:** discover implementations, wrappers, leases, adapters, and test dispatch helpers for each changed interface. Check forwarding, receiver-specific semantics, and compatibility. Resolve transport/type support, negotiation, configuration defaults/validation/precedence, and option behavior from maintained definitions and public docs. Verify documentation and project conventions.

**Agent 4 — Concurrency & data races:** derive execution contexts and ownership handoffs from code. Check shared state, happens-before relationships, lock ordering, channel ownership, backpressure/deadlines, cancellation, pool lifecycle, and stop/dial/publication fencing. Use API docs to identify supported application concurrency; verify internal synchronization regardless of caller restrictions. Determine whether race-detector tests exercise the relevant interleavings.

**Agent 5 — Resource management & leaks:** trace acquisition, ownership transfer, and release, including failed construction. Derive cleanup, retry, retention, callback-lifetime, and repeated-Close obligations from public docs. A cancelled wait or finished attempt is not proof of release. Verify strong references and a responsible owner for retained resources after worker exit or loss of the application handle. Check quiescence before unmap/reuse, safe late completion, lock/capacity accounting, and truthful results.

**Agent 6 — Performance & allocations:** preserve the QWP steady-state row-building guarantee of **0 allocs/op**. Locate its tests/benchmarks from project guidance and verify the affected workload. Measure flush, encoding, and setup separately against their established expectations. Analyze copying, serialization, syscalls, buffer growth, and scaling under supported workloads and configured limits. Substantiate regressions; evaluate remedies against measured costs and ownership constraints rather than prescribing a particular scratch field or storage strategy.

**Agent 7 — Tests & coverage:** discover relevant unit, integration, interoperability, platform, and performance tests. Inspect fixture resolution and CI to determine prerequisites and whether missing prerequisites skip or fail. Resolve actual entry points/selection commands and confirm the intended tests executed. Check supported cross-context behavior, partial failures, boundaries, cancellation, recovery, and shutdown—not just happy paths. A bug fix needs a test that fails without it. Assertions must establish the promised outcome, not merely completion of an attempt. For behavior changes or deleted tests/helpers, verify consistent caller/doc migration and surviving safety coverage. Keep test access within project conventions and build affected examples/benchmarks, including their external manifest references.

**Agent 8 — Code quality & API design:** for claimed simplification or retired behavior, compare affected actors, entry routes, handoffs, and authoritative state before/after. Verify obsolete machinery is removed, not hidden behind flags, and remaining synchronization protects identifiable hazards. Check compatibility, naming, dead code, documentation, licensing, and project conventions. Resolve applicable static-analysis checks and pinned tooling from project/CI configuration; verify available results without inventing execution evidence.

**Agent 9 — Cross-context caller impact:** walk the callsite inventory. Trace callers, dispatch, and ownership until reachability and behavior are established, not to a fixed depth. For each relevant callsite:
- Do its inputs or assumptions conflict with the new behavior?
- If a public guarantee changes intentionally, is the caller migrated and compatibility impact documented?
- Does its execution context, lock state, lifecycle stage, or error path invalidate the callee's assumptions?
- Do implementations, wrappers, configuration paths, and state-machine preconditions agree?

Report SAFE / BROKEN / NEEDS VERIFICATION per callsite with evidence. Classify confirmed defects by impact and likelihood, regardless of whether the callsite is in the diff.

**Agent 10 — Fresh-context adversarial:** when selected, run independently of Agents 1–9 to avoid checklist anchoring.
- Provide only the diff, changed-file names, and neutral source inputs: README's shutdown section and relevant public method/error/callback docs, or instructions to read them.
- Do not provide the change map, findings, implicit-contract inventory, category lists, or checklists.
- Instruction: “Find ways this code is wrong against the public contract; flag source/documentation disagreements.” Allow independent read-only repository exploration.
- Each finding states what is wrong, why, and the demonstrating code path. Verify both novel and corroborated findings before accepting them.

Combine results into a deduplicated draft. Verify inline at level 1 or through Step 3b at levels 2–3 before presenting findings.

## Step 3b: Verify every finding against source

Every finding requires verification, including at levels that perform it inline. At level 2 use one batched reviewer; at level 3 use independent per-finding reviewers, parallel where supported. Record unavailable verification or inconclusive evidence explicitly.

1. **Read cited source**, not just the diff or reviewer's description.
2. **Trace actual dispatch and callers**, including wrappers and every concrete receiver reachable at the claimed failing callsite.
3. **Cleanup/leaks:** cite the public guarantee, supported trigger, owner, observable result, and evidence. Trace acquisition through release or documented retention. Distinguish cancelled waits and finished attempts from actual release. Check lost ownership, unsafe reuse, abandoned obligations, stuck control locks/channels, and false success.
4. **Panics:** trace validation and supported callers to establish original-panic reachability. Separately evaluate injected outcomes against containment/recovery obligations, including ownership and reporting. Inspect the actual defer/recovery path rather than assuming the presence or absence of one recovery call settles the result.
5. **Overflow/resource exhaustion:** establish reachability from validated limits, wire-controlled values, configuration, architecture width, and feasible resource consumption or lifetime. Small inputs can declare huge sizes; inspect validation before arithmetic or allocation. Drop paths proved unreachable, not those exceeding assumed typical sizes.
6. **Unsafe/races:** demonstrate the violated invariant. For races, identify concurrent access paths and missing happens-before relationships; determine what race tests actually exercise.
7. **Error reporting:** compare immediate/deferred/latching behavior with the documented reporting point. Confirm loss, suppression, false success, or wrong policy rather than flagging intentional deferral.
8. **Performance:** use measurements or demonstrated scaling under supported workloads. Verify zero-allocation row building separately from other costs and preserve established regression thresholds.
9. **Cross-context impact:** follow callers and ownership far enough to establish the failure. Apply the same evidence and severity standard inside and outside the diff.

Classify draft findings as CONFIRMED, CONFIRMED WITH NUANCE, FALSE POSITIVE, or UNRESOLVED. Cite the violated contract and callsite for out-of-diff regressions. Explain downgraded/dismissed claims with source evidence. Unresolved suspicions belong in validation gaps, not verified-defect sections.

## Specialized safety checks

Apply relevant checks at every level; the level controls reviewer breadth, not which contracts matter.

### Protocol and error semantics

Use project guidance to locate maintained protocol, configuration, error-policy, and compatibility sources (starting points include `sender_error.go` and `conf_parse.go`). Verify:
- Encoding/decoding and persistent formats remain compatible with the supported client/server versions and cross-language fixtures.
- Frames, schema, symbol dictionaries, and recovery state provide what replay on a fresh connection requires.
- Publication, transmission, ACK, durable ACK, and physical cleanup remain distinct; each API's return value/wait matches its documented guarantee.
- Rejections do not silently drop data or advance acknowledged/durable watermarks past undelivered work.
- Policy resolution matches documented defaults, supported overrides, precedence, forced policies, and handling of unknown statuses. Do not assume a default policy is immutable or copy a category catalog here.
- Repeated-failure escalation satisfies all documented evidence and timing gates. Transport close information is not substituted for the specified rejection/escalation policy.
- Errors reach the required synchronous/asynchronous reporting paths, and retry/rebuild requirements remain truthful.

### Store-and-forward, recovery, and pool startup

Apply whenever persistence, reconnect/failover, drainers, startup configuration, or pool lifecycle changes. Read the running-sender, local-error, quarantine, and startup contracts in README/public docs and the relevant error definitions. Derive implementation paths from those sources.

- **Preserve Invariant B:** running senders, asynchronous initial connection, and background/orphan drain paths keep retrying transport outages and all-replica role-reject windows indefinitely with capped exponential backoff. Check attempt/deadline gates for what they count; a transient outage must not become terminal or quarantine retained rows by spending an unrelated episode budget.
- Distinguish bounded synchronous initial connection, sanctioned terminal episodes, local-storage failures, and explicit shutdown from transport-outage retry. Resolve exact terminal conditions and policy overrides from maintained contracts; verify each gate's evidence, reset conditions, and reporting.
- **Preserve local-storage safety:** environmental I/O faults must not be treated as proof of slot corruption. Check recovery validation, committed boundaries, durable ordering, evidence preservation, and quarantine/deletion authority. A local fault must not authorize data loss or premature ownership release.
- Verify backpressure and local-durability errors against their non-terminal recovery contract. Retained data, error persistence/clearing, and operator recovery must remain consistent.
- Derive startup behavior from the full effective configuration: pool minimums/capacity, lazy connection, initial-connect mode, transport, persistent-slot recovery, defaults/injections, and validation. Do not infer prewarming or connectivity-error timing from a single flag.
- Trace when each connection is actually attempted and who observes failure: construction, first borrow/use, or a background owner. Test supported combinations with unavailable and later-recovered servers; deferred connectivity must not disable functionality.
- After initialization, verify running retry semantics independently of startup behavior. Pool shutdown must account for all owned work, including construction, leases, returns, and deferred cleanup.

### Resource ownership and simplification

- Derive completion, retry, retention, callback lifetime, and repeated-call obligations from README and the relevant public docs.
- Every resource remains strongly owned through release or documented retention; timeout, removal from a live list, or worker exit alone proves neither release nor completion.
- Verify quiescence before unmap/reuse, safe late completion, accurate capacity/lock accounting, and truthful unfinished/failed results.
- When simplification or behavior retirement is claimed, compare before/after actors, entries, handoffs, and authoritative state. Verify obsolete machinery is removed and surviving tests cover supported behavior.
- Tie synchronization and bookkeeping to identifiable hazards; flag conflicting authorities, unused state, and complexity without a demonstrated requirement.

### Unfinished work and repository conventions

- Scan changed TODO/FIXME/HACK/XXX/WORKAROUND markers. Distinguish moved text from newly deferred defects or incomplete implementation; verify referenced issues.
- Check commit messages, licensing, naming, public documentation, module/toolchain constraints, and example paths against maintained project conventions rather than duplicating those rules here.

## Step 4: Output

Present verified findings only in severity sections. Use impact, likelihood, affected scope, and recoverability to justify severity—not in-diff/out-of-diff location.

### Critical

Issues that must be fixed before merge. Each includes:
- Exact file path and line numbers, and whether in-diff or at an unchanged callsite.
- Applicable contract and supported trigger.
- Code-path evidence and observable consequence.
- Suggested fix.

### Moderate

Verified issues worth addressing but not blocking, with source evidence and suggested fixes.

### Minor

Verified minor issues and clearly labeled style suggestions.

### Downgraded

Draft findings dismissed or reduced in severity after verification. Give the original claim and a brief source-backed explanation.

### Summary

- Verdict: approve, request changes, or needs discussion.
- Highlight regressions, intentional contract changes, compatibility impact, and documented limitations.
- State verification/coverage gaps, unresolved questions, and tests actually run, skipped, or unavailable. Do not equate a skipped fixture or unexecuted platform job with a pass.
- Summarize confirmed versus dismissed/unresolved findings and in-diff versus out-of-diff impact where useful. No minimum finding count or out-of-diff quota: justify coverage through examined paths and remaining limitations.
