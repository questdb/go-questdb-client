---
name: review-pr
description: Review a GitHub pull request against the go-questdb-client coding standards
argument-hint: [PR number or URL] [--level=0..3]
allowed-tools: Bash(gh *), Bash(git fetch *), Bash(git merge-base *), Bash(git show *), Bash(git diff *), Bash(git log *), Read, Grep, Glob, Agent
---

Review the pull request `$ARGUMENTS`.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. `go-questdb-client` is mission-critical software: bugs can cause data loss, silent corruption, dropped rows, or host-process crashes. Be critical, thorough, and direct. Your job is to catch problems that would hurt an application or operator before they ship, not to demonstrate thoroughness by volume.

**A review that blocks on everything blocks on nothing.** Every finding costs the author a fix-and-review round, and an inflated one costs the whole report its credibility. Reserve Critical for defects with a real consequence for an application or operator, report everything else at the severity its evidence earns, and approve when the gates pass. Approval is a normal outcome of reviewing competent work, and a review with zero findings is a successful one.

- **Read the contracts the change touches.** At every level, read [docs/qwp-limits-and-invariants.md](../../../docs/qwp-limits-and-invariants.md): it lists maintainer invariants and limits the project has deliberately accepted. A behavior listed there is not a finding unless the PR widens it or the conditions that bound it no longer hold. When a finding is accepted rather than fixed, the record belongs in that file, not the README. When the change touches shutdown, Close, pools, slot release, cleanup, or retained resources, also read [README: QWP shutdown and ownership](../../../README.md#qwp-shutdown-and-ownership) and the relevant method, error, and callback Go docs; together they define the shutdown contract. Always read the Go docs of changed exported symbols and the PR's declared behavior changes.
- **Keep the PR's blast radius small.** The PR must fix what it set out to fix, plus anything it demonstrably breaks. A problem whose behavior on the merge base, for the same trigger, is the same or worse is pre-existing: it is never a finding against this PR and never affects the verdict. The one exception is a pre-existing bug the PR moves onto a newly reachable path. A pre-existing bug proved to the full evidence bar leaves the review as an Adjacent issue draft (Step 4) rather than being discarded.
- **The diff is the entry point, not the scope.** Inspect surrounding code and unchanged callers whose assumptions the change affects. Out-of-diff code is in scope when this PR changed a contract it relies on. Do not clear a change solely because it looks correct in isolation.
- **Discovery is not a finding.** Every concern, including one raised by several reviewers, is a hypothesis until it passes the Step 3b admission gate. Try to disprove it before building a narrative: look for the guard, validation, retry, caller restriction, accepted limit, or identical base behavior that defeats it. Failing to disprove a claim is not evidence for it, and uncertainty never raises severity.
- **The claim carries the burden of reachability.** A finding names the supported trigger that produces every condition it needs: a sequence of public API calls, a configuration, a server response the protocol permits or a buggy or hostile server can send, or a local-storage state that a supported crash or fault leaves behind. Wire-controlled lengths and counts are reachable by construction; inspect validation before arithmetic or allocation. Unusual but supported inputs count. A condition that no API, configuration, server, or supported fault can produce is not a finding.
- **Verify every claim.** For a fix, establish the original bug and corrected behavior. For performance, inspect measurements and scaling. For simplification, compare affected actors, entry routes, ownership transfers, and authoritative state before/after. Intentional behavior removal must be explicit and reflected in code, tests, callers, and docs. Treat the PR description as a hypothesis, not a statement of fact.
- **Documentation disagreements.** A disagreement between documentation, implementation, tests, and PR claims is a finding when this PR introduces it, or when the PR changes behavior a document describes and leaves that document stale. A pre-existing disagreement in text and behavior the PR does not touch is an Adjacent item at most.
- **Distinguish coding conventions from runtime guarantees.** Do not require exhaustive mechanical enforcement of a convention. Evaluate tests against promised observable behavior, not whether every conceivable source mutation is detected.
- **Think adversarially:** empty and boundary inputs; invalid encodings and oversized declared lengths; partial publication/write; transport, authentication, server, and local-storage failures; cancellation, callbacks, concurrent access, and cleanup. Derive supported caller behavior and expected outcomes from the applicable contracts.
- **Check what the change requires:** error handling, tests, public docs, affected implementations/wrappers, configuration validation, and test adapters that this change needs and does not supply. Discover dependencies rather than assuming a fixed inventory. Missing tests alone never make a finding Critical.
- **Distinguish panic reachability from containment.** Internal invariant assertions are not defects merely because they panic. Determine whether supported inputs can violate the invariant, and separately verify containment, ownership, and reporting obligations after an injected panic.
- **Respect documented error reporting.** Fluent methods may intentionally latch errors and surface them at a later operation. Verify the documented reporting point and that the error is not lost; do not mistake latching for swallowing an error.
- **Do not praise the code.** Severity depends on impact and likelihood, not on the checklist a finding came from, its location in the diff, or the number of reviewers that raised it.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`–`3`. **Default to 0.** Strip the level token before passing the PR number or URL to `gh`. If a lone digit could be either the PR number or the level, ask rather than guessing.

The level controls breadth and independent review effort, not the evidence needed for a finding. Reserve level 3 for high-stakes changes to wire/file formats, public interfaces, authentication, configuration semantics, state machines, resource ownership, or concurrency.

**This table is authoritative.** Later step and role instructions apply only when selected. Reviewers receive the map portions produced at that level and perform targeted searches as needed; they must not assume omitted inventory work was done. Select only roles whose area the change materially touches, up to the level's cap. Depth comes from reachability evidence and verification, not from the number of reviewers.

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 4. Review inline without delegation or a formal change map. Cover correctness, panic/crash surface, concurrency, tests, and project conventions. Every candidate passes the Step 3b admission gate inline before any report prose is written. |
| **1** | Adds Step 2.5a. In Step 3, run Agent 1 plus at most **two** of Agents 2, 4, 5, and 7. Apply the Step 3b admission gate inline. |
| **2** | Full Step 2.5, with the required inventory in 2.5b limited to public and test-exposed symbols; follow internal dependencies as needed to verify their behavior. Run Agent 1 plus at most **four** of Agents 2–9. Step 3b uses one batched verification reviewer. |
| **3** | Full change map. Run at most **six** roles: Agent 1 always; Agent 9 when changed symbols have out-of-diff callers; Agent 10 when an independent adversarial pass is warranted; the others only when their area is touched. Step 3b uses an independent reviewer per candidate. |

State the chosen level at the start. If defaulted, mention that level 3 provides the full review.

## Step 1: Gather PR context and authoritative sources

Capture the PR identifier in `$PR`, then fetch metadata, diff, and review discussion:

```bash
PR='<PR number or URL after removing the level token>'
gh pr view "$PR" --json number,title,body,labels,state,baseRefName,headRefOid
gh pr diff "$PR"
gh pr view "$PR" --comments
```

Ensure the source being inspected corresponds to the PR head (`headRefOid`); identify any local differences that affect verification. Fetch the base branch and the PR head, and record their merge base as `$BASE`:

```bash
git fetch origin "<baseRefName>" "pull/<number>/head"
BASE=$(git merge-base "<headRefOid>" "origin/<baseRefName>")
```

Step 3b compares behavioral candidates against `$BASE`. Read base source with `git show "$BASE:<path>"` rather than changing the working tree.

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

A description problem is Moderate when it misstates or omits user-visible behavior, compatibility impact, or a contract change; otherwise it is Minor.

## Step 2.5: Map the change surface

Produce the portions required by the selected level using repository search and source inspection, not memory. Record search scope and evidence. Selected Agents 1–9 receive those portions; Agent 10 receives only its neutral source inputs.

### 2.5a Semantic delta per changed symbol

For each modified or added function, method, interface member, field, or exported constant/variable, record:

- **Symbol:** fully-qualified name and source location.
- **Before / after:** signature, error and panic behavior, receiver mutation, ordering/idempotency/replay guarantees, allocations, goroutine/channel interaction, context handling, and lock acquisition, as applicable. "Before" is the behavior at `$BASE`.
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

Review every relevant discovered exposure. An exposure you could not verify is a limit of the review, not a safe path and not a finding; keep it in the private ledger.

## Step 3: Candidate discovery

Use only the roles selected by the level table, through the environment's supported delegation workflow and resource limits. These are responsibilities, not assumed executable agent names. Keep reviewers read-only and independent; run selected roles in parallel where supported. Report unavailable roles or incomplete coverage rather than silently claiming they ran.

All delegated reviewers read applicable repository guidance, starting with `CLAUDE.md`, and follow its references to maintained contracts. Supply those sources or instructions to read them; do not assume the parent's reading is available in a fresh context.

Selected Agents 1–9 also receive the diff, `$BASE` and the PR head revision, available change-map portions and their scope limits, relevant contract sources, and declared behavior changes. Each follows callers as needed to substantiate candidates even when no inventory was required at that level.

### Candidate-discovery directive (all roles)

- You generate hypotheses; you do not publish findings. Return atomic candidates. Do not assign severity, propose fixes, or describe a candidate as verified or confirmed. Role text below that mentions what to check describes where to look, not what you may conclude.
- For each candidate give: the changed hunk, or the unchanged callsite plus the contract this PR changed; the supported trigger, or `trigger: unknown`; the reachability chain; behavior at `$BASE` for the same trigger (worse, same, better, new surface, or unknown); the consequence for an application or operator; and the strongest counterevidence you found.
- A proposition with no consequence of its own is evidence for its parent candidate, not a separate candidate. If the parent falls, it falls too.
- A claim containing **never**, **only**, **always**, **no retry**, or another universal requires an exhaustive search of callers and event sources, not one traced path.
- Mark a problem whose base behavior is the same or worse as pre-existing. It is an Adjacent candidate, not a candidate against this PR.
- Two reviewers repeating the same reasoning are one candidate, not corroboration.
- Returning no candidates is valid and preferred to returning a speculative one.

### Anti-anchoring rules

- Search beyond the diff and inspect caller assumptions and actual dispatch paths.
- Check relevant exposures before clearing a changed symbol; state verification gaps.
- Out-of-diff code yields a candidate against this PR only when the PR changed a contract it relies on or made a pre-existing bug newly reachable.
- Coverage is evidence, not a finding quota. Zero candidates can be valid; agreement between reviewers is not proof.

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

**Agent 3 — Public API & interface conformance:** discover implementations, wrappers, leases, adapters, and test dispatch helpers for each changed interface. Check forwarding, receiver-specific semantics, and compatibility. Resolve transport/type support, negotiation, configuration defaults/validation/precedence, and option behavior from maintained definitions and public docs. Verify that documentation the change affects stays accurate.

**Agent 4 — Concurrency & data races:** derive execution contexts and ownership handoffs from code. Check shared state, happens-before relationships, lock ordering, channel ownership, backpressure/deadlines, cancellation, pool lifecycle, and stop/dial/publication fencing. Use API docs to identify supported application concurrency; verify internal synchronization regardless of caller restrictions. Determine whether race-detector tests exercise the relevant interleavings.

**Agent 5 — Resource management & leaks:** trace acquisition, ownership transfer, and release, including failed construction. Derive cleanup, retry, retention, callback-lifetime, and repeated-Close obligations from public docs. A cancelled wait or finished attempt is not proof of release. Verify strong references and a responsible owner for retained resources after worker exit or loss of the application handle. Check quiescence before unmap/reuse, safe late completion, lock/capacity accounting, and truthful results.

**Agent 6 — Performance & allocations:** preserve the QWP steady-state row-building guarantee of **0 allocs/op**. Locate its tests/benchmarks from project guidance and verify the affected workload. Measure flush, encoding, and setup separately against their established expectations. Analyze copying, serialization, syscalls, buffer growth, and scaling under supported workloads and configured limits. Substantiate regressions with a multiplier (per row, per frame, per flush) or a fixed bound; evaluate remedies against measured costs and ownership constraints rather than prescribing a particular scratch field or storage strategy.

**Agent 7 — Tests & coverage:** discover relevant unit, integration, interoperability, platform, and performance tests. Inspect fixture resolution and CI to determine prerequisites and whether missing prerequisites skip or fail. Resolve actual entry points/selection commands and confirm the intended tests executed. Check that behavior the change adds or alters is exercised across supported contexts, partial failures, boundaries, cancellation, recovery, and shutdown—not just happy paths. A coverage candidate names the changed behavior, the credible regression mode, and its consequence; absence of a test alone is not a candidate. Regression tests should demonstrate the original supported failure and the corrected observable outcome. Removing redundant protection without changing promised behavior is not, by itself, evidence of a test defect. This does not excuse vacuous assertions or missing coverage of actual failure behavior. Assertions must establish the promised outcome, not merely completion of an attempt. For behavior changes or deleted tests/helpers, verify consistent caller/doc migration and surviving safety coverage. Keep test access within project conventions and build affected examples/benchmarks, including their external manifest references.

**Agent 8 — Code quality & API design:** for claimed simplification or retired behavior, compare affected actors, entry routes, handoffs, and authoritative state before/after. Verify obsolete machinery is removed, not hidden behind flags, and remaining synchronization protects identifiable hazards. On changed lines, check compatibility, naming, dead code, documentation, licensing, and project conventions. Resolve applicable static-analysis checks and pinned tooling from project/CI configuration; verify available results without inventing execution evidence.

**Agent 9 — Cross-context caller impact:** walk the callsite inventory. Trace callers, dispatch, and ownership until reachability and behavior are established, not to a fixed depth. For each relevant callsite:
- Do its inputs or assumptions conflict with the new behavior?
- If a public guarantee changes intentionally, is the caller migrated and compatibility impact documented?
- Does its execution context, lock state, lifecycle stage, or error path invalidate the callee's assumptions?
- Do implementations, wrappers, configuration paths, and state-machine preconditions agree?

Return a SAFE / BROKEN / NEEDS VERIFICATION verdict per callsite with evidence, as working material for the parent; the per-callsite table does not appear in the report. A BROKEN callsite becomes a candidate against this PR only when its breakage follows from this PR's change; otherwise it is a pre-existing candidate.

**Agent 10 — Fresh-context adversarial:** when selected, run independently of Agents 1–9 to avoid checklist anchoring.
- Provide only the diff, changed-file names, `$BASE` and the PR head revision, and neutral source inputs: applicable repository guidance including `CLAUDE.md`, README's shutdown section, `docs/qwp-limits-and-invariants.md`, and relevant public method/error/callback docs, or instructions to read them.
- Do not provide the change map, candidates, implicit-contract inventory, category lists, or reviewer-generated checklists. Maintained repository guidance is a neutral source, not a prior review finding.
- Instruction: “Find ways this change is wrong against the public contract, including documentation it makes inaccurate.” Allow independent read-only repository exploration. The candidate-discovery directive applies.

Combine results into a deduplicated candidate list. Every candidate passes Step 3b before it can appear in the report.

## Step 3b: Admit or omit each candidate

Every candidate passes this gate: inline at levels 0–1, through one batched verification reviewer at level 2, and through an independent reviewer per candidate at level 3, parallel where supported. A verification reviewer receives the neutral proposition (the claim stated without the discoverer's narrative, proposed severity, or fix), `$BASE` and the PR head revision, the relevant file names, and instructions to read `CLAUDE.md` and the relevant maintained contracts. Its first task is to construct the strongest disproof; only a candidate that survives proceeds to proof.

A behavioral candidate is admitted only when each field is backed by cited evidence:

- **Attribution:** the changed hunk, or the unchanged callsite plus the contract this PR changed.
- **Trigger:** the supported operation, configuration, server behavior, or fault that creates every required condition.
- **Reachability:** the complete path from trigger to symptom through actual callers, dispatch, guards, retries, and locks.
- **Base comparison:** the same trigger at `$BASE`, from source or execution; or `new surface`, with the reason base cannot express the trigger.
- **Consequence:** what an application or operator observes. A statement that only supports another candidate is not a consequence.
- **Counterevidence:** the strongest disproof attempted and why it fails.

A static finding fully proved by source—a compile error, a convention violation on changed lines, documentation the PR made wrong—needs only attribution and the source proof.

When a candidate concerns ordering, races, retries, recovery, or filesystem state and a targeted test or reproduction is cheap, run it at the PR head and, where the trigger exists there, at `$BASE`; record when execution was not possible. A race candidate needs both concurrent access paths and the missing happens-before edge; timing prose alone does not admit it.

After the admission fields, apply the domain checks that fit:

1. **Read cited source**, not just the diff or reviewer's description.
2. **Trace actual dispatch and callers**, including wrappers and every concrete receiver reachable at the claimed failing callsite.
3. **Cleanup/leaks:** cite the public guarantee, supported trigger, owner, observable result, and evidence. Trace acquisition through release or documented retention. Distinguish cancelled waits and finished attempts from actual release. Check lost ownership, unsafe reuse, abandoned obligations, stuck control locks/channels, and false success.
4. **Panics:** trace validation and supported callers to establish original-panic reachability. Separately evaluate injected outcomes against containment/recovery obligations, including ownership and reporting. Inspect the actual defer/recovery path rather than assuming the presence or absence of one recovery call settles the result.
5. **Overflow/resource exhaustion:** establish reachability from validated limits, wire-controlled values, configuration, architecture width, and feasible resource consumption or lifetime. Small inputs can declare huge sizes; inspect validation before arithmetic or allocation. A path a supported trigger reaches is admissible even when it exceeds a typical workload; a path none reaches is omitted.
6. **Unsafe/races:** demonstrate the violated invariant. For races, identify concurrent access paths and missing happens-before relationships; determine what race tests actually exercise.
7. **Error reporting:** compare immediate/deferred/latching behavior with the documented reporting point. Confirm loss, suppression, false success, or wrong policy rather than flagging intentional deferral.
8. **Performance:** use measurements or demonstrated scaling under supported workloads, and state the multiplier or fixed bound. Verify zero-allocation row building separately from other costs and preserve established regression thresholds.
9. **Cross-context impact:** follow callers and ownership far enough to establish the failure. An out-of-diff candidate is admitted only as out-of-diff breakage: the unchanged callsite fails because of a contract this PR changed, or a pre-existing bug becomes newly reachable through this PR.
10. **Verify the conjunction, not just the links.** A multi-step candidate is only as true as its weakest step. Identify the step most likely to be false—usually “this state can occur”—and test it first.

Classify each candidate:

- **ADMITTED in-diff** — every applicable field is proved and the defect is in the diff.
- **ADMITTED out-of-diff breakage** — every applicable field is proved, and an unchanged callsite fails because of this PR.
- **OMITTED pre-existing** — base shows the same or worse behavior for the same trigger, and the PR adds no new path to it. When its trigger, reachability, and consequence are proved, it leaves the ledger as an Adjacent issue draft.
- **OMITTED false** — counterevidence disproves it.
- **OMITTED unverified** — a required field is missing or inconclusive.

When only part of a candidate holds, admit the narrower proved claim and omit the rest. There is no partially confirmed state. Keep omitted candidates and their disproofs in a private ledger; they do not appear in the report. Derive a suggested fix only after admission, and check that it closes every admitted path without breaking ownership or ordering.

## Specialized safety checks

Apply a section when the change touches its area; the level controls reviewer breadth, not which contracts matter. These sections define what counts as a defect in their area. They are not an audit every PR must pass, and a check that finds nothing produces no report text.

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

Apply whenever persistence, reconnect/failover, drainers, startup configuration, or pool lifecycle changes. Read the running-sender, local-error, quarantine, and startup contracts in README/public docs, the accepted limits in `docs/qwp-limits-and-invariants.md`, and the relevant error definitions. Derive implementation paths from those sources.

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

Present only admitted findings, plus Adjacent issue drafts. Omitted candidates, disproofs, dismissed or downgraded claims, the private ledger, and candidate or agent counts do not appear. Do not publish a claim and retract it later; finish admission first. A report with no findings is valid.

**Proportionality.** Keep the report actionable in one sitting. If a normal-sized PR yields more than about seven findings, re-run admission on every item and remove duplicates, dependent propositions, pre-existing items (move them to Adjacent), and low-value prose. Review depth is shown by evidence, not report length.

**Every finding, at every severity, opens with three one-line summaries:**

- **Problem:** what is wrong, in at most 12 words. No mechanism or fix.
- **Impact:** who is affected and how much, in at most 12 words.
- **Evidence:** the decisive source location or test output, with the reviewed revision.

Write these lines last, from the completed admission fields. Then give only the minimal trigger → path → symptom trace, the base comparison, and the suggested fix.

### Severity classification

Severity follows what an application or operator loses on a reachable path, not the checklist a finding came from. Do not classify up “to be safe”: an inflated Critical costs the author exactly what a real one costs.

“The user” is an application developer using this client or an operator running a service that embeds it. A consequence only for maintainers, CI, or the release process is never Critical.

**The Critical test.** A finding is Critical only when “Because of this, the user sees ___” completes with one of:

- lost, duplicated, or corrupted rows; wrong query results; store-and-forward slot or dictionary state that cannot be replayed correctly;
- a host-process crash, deadlock, or hang; a sender that stalls without erroring; goroutine, file-descriptor, connection, memory, or disk growth without bound under a repeatable operation;
- false success or a missing/wrong error: an operation reports success for unpublished or unacknowledged data, a terminal failure goes unreported, or a transport outage becomes terminal;
- a security failure: TLS verification or authentication bypass, credential exposure;
- a compatibility break: wire format, persisted segment/dictionary format, public API, or configuration semantics changed so existing applications or retained data break;
- a performance regression the user can feel: allocations in steady-state QWP row building, or added per-row or per-frame cost on the publication path, stated with its multiplier.

Each completion names its trigger: “application does X → sees Y”. A behavioral candidate without a supported trigger and one of these consequences is not Critical, and one without any concrete consequence is omitted.

**Moderate.** Admitted defects with bounded impact: a convention violation on changed lines, a proved weak or vacuous test, missing coverage for changed behavior with a credible regression mode, documentation the PR made wrong or left stale, a bounded cost off the publication path (construction, setup, Close).

**Minor.** Cosmetics on changed lines: naming, comment wording, formatting, ordering; description issues from Step 2 that do not misstate user-visible behavior.

**Not findings:**

- **Tautologies.** If the finding would appear on every PR of this shape, it describes the workflow, not this change.
- **Accepted limits and project decisions.** A behavior recorded in `docs/qwp-limits-and-invariants.md`, or permitted by a documented convention or CI configuration, is a decision. It becomes a finding only when the PR widens it or the conditions that bound it no longer hold. Disagreement with the decision itself belongs under “needs discussion”, not under a severity.
- **Pre-existing problems.** These are Adjacent issue drafts at most.

### Critical

Blocking issues introduced or exposed by this PR, worst first. Each includes:
- The three summary lines.
- Exact file path and line numbers, and whether it is in-diff or out-of-diff breakage. For out-of-diff breakage, the contract this PR changed and the callsite that fails.
- The symptom sentence with its supported trigger.
- The reachability trace and base behavior for the same trigger.
- For performance: the multiplier and what it multiplies.
- A suggested fix, applicable in this PR.

### Moderate

Non-blocking admitted issues worth fixing, each with the three summary lines, its evidence, and a suggested fix.

### Minor

One line each.

### Adjacent findings (not blocking; issue drafts)

Bugs that exist at `$BASE`, found in code this review visited, that the PR does not introduce, break, or make newly reachable. They are not findings against this PR, never influence the verdict, and are never proposed as changes to it. They meet the same evidence bar: only an OMITTED pre-existing candidate with its trigger, reachability, and consequence proved appears here.

Report each as a ready-to-file issue draft:

- **Problem:** at most 12 words; doubles as the issue title.
- **Impact:** at most 12 words, or “None — <reason>”.
- **Location:** file path and line numbers.
- **Symptom:** what a user would observe, or “latent: no user-visible impact today”, naming the guard that prevents it.
- **Reachability:** the path that reaches it, or why nothing does yet.
- **Suggested fix:** one or two lines.
- **Severity if filed standalone:** Critical, Moderate, or Minor.

List the most severe first. Beyond about five drafts, give the rest as one-line titles with locations. Offer to file them; do not file anything unless asked. If the PR moves code onto a path where the bug now fires, it is out-of-diff breakage under Critical instead, with that argument stated.

### Summary

- **Verdict**, exactly one of:
  - **approve** — no admitted Critical finding remains open. Moderate and Minor items may remain; list them and approve anyway.
  - **approve with comments** — no admitted Critical finding; name the Moderate items you want addressed without blocking on them.
  - **request changes** — at least one admitted Critical finding is open.
  - **needs discussion** — the change requires a product, architecture, or compatibility decision a reviewer cannot make alone.
- Withholding approval when no admitted Critical finding remains is a review failure.
- State contract changes and compatibility impact the PR introduces, intentional or not.
- State tests actually run, skipped, or unavailable. Do not equate a skipped fixture or unexecuted platform job with a pass.
- State the review's limits in one or two lines: what the selected level did not cover (for example, limited callsite analysis at levels 0–1) and any check that could not be completed. Do not list individual unverified hypotheses.
- State the severity distribution and the in-diff / out-of-diff split of admitted findings. If the report is long or Critical-heavy, re-run admission rather than keeping weak items at a lower severity.
