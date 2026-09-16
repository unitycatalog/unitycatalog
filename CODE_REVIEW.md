# Code Review Guidelines (Unity Catalog)

Code review guidance for **Unity Catalog**. AI-assisted review is **human-gated** (see `AI_POLICY.md`); for build, test, and architecture see `AGENTS.md`.

Use it as a checklist when you review a pull request, or as a rubric an AI assistant can apply to self-review a PR before you open it.

## When to comment (the signal threshold)

Be high-signal and selective, not a line-by-line nitpicker. Do not flag every theme on every diff. Comment when one of these is true; otherwise stay silent:

- It changes the design (a boundary, an abstraction, where responsibility lives, the public surface).
- It affects whether a test actually proves anything.
- A name is wrong, misleading, or inconsistent.
- Something will drift or break later (a stale comment, a duplicated source of truth, a back-compat hazard).
- An error is swallowed, mis-coded, or leaks internals.
- The PR scope is muddied (a refactor mixed with logic, or unrelated churn).

If a comment is purely cosmetic and the code is fine, either label it `nit:` or skip it. Default to fewer, higher-value comments.

## Tone and approach

- Prefer a question that leads the author to the conclusion over a flat directive: "why is this a var?", "who depends on this?", "what is this actually testing?".
- Use soft directives: "can we consolidate these?", "please add class docs explaining the purpose".
- Share the reasoning, not just the verdict: name the future scenario you are worried about.
- Concede gracefully when the author gives a good reason, and invite pushback on your own comments.
- Call out genuine simplifications and good catches, not only problems.
- Reserve emphasis for foundational issues; do not treat every comment as urgent.
- Tag the right owner when something is outside your knowledge, and defer rather than block when you lack context.
- Ask for a plain-English explanation when a flow is hard to follow.

## Severity (label selectively)

Label a comment only when it is a nit or a hard blocker; leave normal feedback unmarked. Tagging every comment is noise.

- **blocker** — a foundational problem that must be fixed before merge. Rare.
- **normal** — the default, unlabeled feedback (most comments).
- **followup** — not a blocker; defer and ask for a cross-linked tracking issue so it is not forgotten.
- **nit** — cosmetic and optional; use it freely to downgrade. Conversely, "this is not a nit" elevates something small that matters.

## Quick check

A fast pre-push scan; each item is expanded in the themes below.

```
□ api/all.yaml updated and `build/sbt generate` run; no hand-edited generated code?
□ [Component] PR title prefix present? (DCO sign-off is expected but overridable, not a blocker.)
□ Authz: narrowest privilege; create-checks-parent / mutate-checks-self; fails closed?
□ Schema change additive-only (hbm2ddl.auto=update) and portable across H2/MySQL/PostgreSQL?
□ Connector changes hold across the Spark/Delta matrix; all distribution artifacts version-matched?
□ Managed AND external table paths covered; optional deps (e.g. delta-spark) not assumed present?
□ API errors matched by structured code, rendered in the right dialect (UC REST vs Delta/Iceberg)?
```

## What to look for

Each theme lists the concrete checks and the **principle** behind it (so you can apply it to cases the examples don't cover).

### Test coverage and test *quality* (the top concern)
*Principle: tests are part of the design. A test that can't fail is worse than no test; it gives false confidence.*
- Missing tests are near-blocking: "no tests?", "no testsuite for this?"
- Probe what the test really verifies: **"what is this testsuite really testing? its not reading the data back."**,
  "you could remove the entire grouping code path and this test would still pass."
- Demand **negative tests that auto-break** when behavior changes: "verify it fails as expected with the correct error
  message. dont disable.", "so that when the guarded feature is added, this test automatically fails and forces the PR to update it."
- Right pyramid level: fine-grained permutations in fast unit tests; coarse behavior in integration tests.
  Integration tests alone are **not enough**: "we need unit tests."
- Tests live **in the module of the code they test** (server tests in the server module, connector tests in the connector module).
- **Black-box against prod constants**: define expected strings in the test so a prod string change breaks it.
- **Boundary/off-by-one**: "test 1.7.1 and 1.7.2; often there's an off-by-1."
- **CI cost is finite**: consolidate tests; don't re-test behavior owned by an upstream library rather than your own code.
- **Clean up test state rigorously**: a failed assertion that leaves an entity behind cascades into later failures.

### Documentation and comments (two-sided rule)
*Principle: code is self-explanatory; comments capture only the non-obvious "why", and must never be allowed to drift.*
- Demand docs for the **why**: class docs (purpose), method docs (intent + **what the method does NOT do**), param
  docs **regardless of triviality**. Docs belong in the **same PR**, not as an afterthought.
- Demand **removal of stale-prone comments**: NO version numbers, NO time/date references, NO comments restating the
  code, NO cross-references to class names in other files/repos. "comments going stale confuse both humans and LLMs."
- A comment that **can't go stale** is fine; one that **will drift** is not.
- **Document non-obvious API and security behaviors** and their implications (for example, a wildcard JWT issuer lets an unauthenticated caller pick which host the server fetches JWKS from).
- **Examples must be complete and runnable end-to-end**: don't omit a setup step the reader can't guess ("the page never shows how to create the events source table, and the obvious guess fails").

### Error handling and failure behavior
*Principle: fail fast and loud; never hide a failure; don't leak internals.*
- **Never swallow errors silently**: log before swallowing: "we should never eat up an error without logging."
- Prefer **allow-lists over deny-lists**; make illegal states unrepresentable (a sealed type or enum, so a value is one of the known cases by construction).
- **No implementation details in error messages**; watch security (don't leak existence of resources the caller can't see): "is it secure to respond with `existingExternalLocation` in the error message? what if the caller does not have the permissions to list external locations?"
- Preserve the **root cause** (pass the original exception as the cause).
- Correct **response codes** (no 500 for client errors; no `table_does_not_exist` unless certain).
- **Match API errors on the structured error code, not a substring of the message**, and render each in the right dialect (UC REST vs Delta/Iceberg). Prefer **centralized error codes** for stability "independent of code structure."
- **Keep log levels honest** so real failures stay findable: an expected-absent case (a catalog/schema/table missing in a test) belongs at DEBUG, not "a large number of stack traces ... which makes it noisy."

### Naming (near-non-negotiable)
*Principle: names exist forever and are read far more than written; pick the one that minimizes future confusion.*
- Names must match what code **does/returns**: a method that returns a DAO shouldn't be named as if it returns the entity.
- No **implementation details** in names ("'Stopwatch' is an impl detail").
- Encode the **owning concept**, consistent project-wide ("UC in the name", "Uc → UC everywhere", "CLI → Cli").
- Disambiguate ("tableRootLocationUri: delta root? iceberg root? UC storage root?").

### Stale / version drift, and single source of truth
*Principle: anything a human must remember to keep in sync will drift; demand a forcing function or a structure that can't go stale.*
- Hardcoded values in N places → centralize ("define one variable, not the version string in 5 places";
  "source of truth should be the crossbuild file"). A version hardcoded across many files goes stale on the next bump: single source plus a release checklist.
- Demand a **forcing function** (a test/build check that fails) when two things can diverge silently.
- Centralize policy in one class; don't scatter conf-key checks.
- **The OpenAPI spec (`api/all.yaml`) is the source of truth**: regenerate with `build/sbt generate`, never hand-edit generated code, and change the generator templates rather than the output. Verify non-generated clients against the spec, since generated clients follow automatically.
- **One source of truth for config**: don't override values in one file with values from another (`server.properties` vs `hibernate.properties`).

### Abstraction, coupling, and responsibility placement
*Principle: boundary isolation is foundational; bad boundaries today are the legacy mess of tomorrow.*
- Question every **new class/trait/abstraction**: "do you expect multiple implementations? if not, the trait is
  unnecessary bloat, add it later", "what's the justification for a new class? why not the existing one?"
- **Separation of concerns / mechanism vs policy**: "decouple adding the framework from changing behavior."
- Don't let **core depend on non-essential** code.
- **Minimize public/API surface**: "make these package private with a class doc explaining why."
- Code lives where it's **used and synced**, not where it's "logically defined."
- **Keep legacy and new API paths from tangling** in one class: use a small separate interface rather than mixing old and new calls in the same class.
- **Centralize authorization filtering in the decorator layer, not per service** (a shared response filter, not a check copied into each handler).
- **Use the typed build API**: in sbt build code, use `Keys.*` rather than constructing a `SettingKey` by string.

### Flags and config
*Principle: use the type that prevents mistakes; roll risk out gradually.*
- "only 2 values? make it a boolean." Consistent naming (`enableX`). Flags "are not to be casually modified" in code.
- Gate a risky new feature behind a config option, default off until it is proven.
- Don't add a flag that multiplies combinatorial complexity.

### PR scope and diff hygiene
*Principle: a reviewable PR does one thing; mixing hides the real change and raises risk.*
- Split **refactors/renames into separate PRs** ("can this be pulled out in a PR of its own before this PR?").
- Strip **unrelated/cosmetic churn** ("these indent changes clutter the PR and hide the real change").
- Call out a PR mislabeled as a refactor that contains logic changes. Minimize diff/risk near releases.
- **`[Component]` PR title prefix** (`[Server]`, `[Spark]`, `[Python]`, `[Docs]`, ...) naming the primary area. **DCO sign-off** (`Signed-off-by:`, real name) is expected but not a hard blocker: don't block a PR only for a missing sign-off, since a maintainer can add or override it.
- **Coordinate the UC and Delta PRs** and land them in the right order when a change spans both projects.

### Simplicity: no dead code, no bloat
*Principle: YAGNI; add it when needed, not before.*
- "its not good to add dead code until its needed. whats the rush?" Inline single-use helpers; collapse one-liners.
  "Let's simplify." is a complete review comment.

### Readability and nesting
- Object to deep nesting and returns-inside-returns: "break this up and reduce nesting.", "why so nested."

### Back-compat and runtime robustness (high stakes)
*Principle: minimize blast radius; a released surface is forever.*
- **Never rename/change a released or public property**: internal refactor only.
- Migration/interop ("what happens with UC 0.4 + Delta-Spark 4.0 which has no code to throw this error?").
- Classpath robustness ("we'll accidentally load aws classes in an azure env").
- Use `Option` over sentinel/magic values ("how do you distinguish default 0 from not-measured?").
- Watch perf/complexity regressions and concurrency (synchronized/volatile; static state reset between tests).
- **Release what you acquire**: `stop()` must release everything `init()` or the constructor created (don't leave a session factory unclosed).
- **Persistence schema changes are additive and restart-safe** on an existing database (Hibernate runs `hbm2ddl.auto=update`): add columns or indexes, never drop or retype, and keep it portable across H2, MySQL, and PostgreSQL.
- **Avoid N+1 DB lookups in list operations** (batch or precompute rather than a lookup per result), and don't buffer a whole HTTP response for a check that can run synchronously.
- **Hold across the Spark/Delta version matrix**: fail fast on incompatible versions rather than failing silently; connector code in the shared tree must compile across the whole cross-build (verify each version and state what you validated); version-match every distribution channel; and keep shims to the differences only, not byte-identical copies.

### Domain-specific guards (Delta, Iceberg, Unity Catalog)
- **Authorization is the highest-stakes guard**: a new or changed endpoint carries the right `@AuthorizeExpression` (narrowest privilege, create-checks-parent / mutate-checks-self), a disabled or default path fails closed and never widens access, and auth-sensitive changes (token endpoints, auth exclusions) go to whoever owns server auth. Document what each privilege and enum value means, not just its name.
- **Lock down CI publish workflows**: `workflow_dispatch` plus repo-scoped secrets can allow arbitrary code execution with publishing credentials, so restrict triggers and secret scope.
- **Protocol/spec is critical-path**: minimize regression risk, cross-check the RFC, annotate required fields,
  check API verbs (POST vs GET) and singular/plural; a distributed-protocol spec must state precedence and actor roles (which party drives each step).
- **Cover both managed and external table paths**, and don't assume optional dependencies are present (UC-Spark need not always have Delta on the classpath; test the absent-dependency path).
- Don't trust convenient verification shortcuts ("describe table can hide issues by filling details from the delta log").

---

## Worked examples

**A test that asserts nothing real.** A new test creates a table and checks that the create call returned OK, but never reads the table back to verify the persisted fields. Ask: "what is this test actually verifying? it never reads the table back." The test would pass even if create were broken.

**A new class whose existence is not justified.** A PR adds a new class that wraps an existing model. Ask: "why do you need this class?". Make the author justify the abstraction; if there is a good reason, accept it, and if not, the class disappears.

**A big PR mixing refactor and feature.** A large feature PR also contains a big unrelated refactor. Say: "can the pure refactoring move into a separate PR? this one is large." It names the cost, is not a blocker, and makes the diff reviewable.

**Conceding when answered.** When the author explains why something is needed, accept it and move the conversation forward rather than defending the original comment.

## How to apply this rubric

1. Filter first. Decide what is worth a comment, and skip cosmetic noise. Aim for few, high-value comments.
2. Walk the diff in priority order: tests, then docs, error handling, naming, single-source-of-truth, abstraction and coupling, flags, scope, simplicity and readability, and finally back-compat and domain guards. In this repository, authorization deserves attention early despite its position in the list.
3. For each finding: phrase it as a concise question or soft directive; label it only if it is a nit or a blocker; explain the "why" through a future-maintainer or future-version scenario when you push on structure; and offer to defer with a tracked follow-up when it is not blocking. Praise genuine simplifications, concede when the author has a good reason, and tag owners for things outside your competence.

## Anti-patterns (do NOT do these)

- **Don't comment on everything.** Be selective and high-signal; a firehose of low-value comments buries the ones that matter.
- **Don't write long, formal, multi-paragraph essays.** Keep comments short and specific.
- **Don't label every comment.** Only nits and blockers get a label; the middle stays unmarked.
- **Don't give a bare directive with no "why"** when pushing on structure.
- **Don't demand perfection on internal/throwaway infra code**: weigh cost/benefit ("not too worried about making
  this bulletproof, it's internal infra").
- **Don't block where you lack context**: defer, ask, or tag the owner.
