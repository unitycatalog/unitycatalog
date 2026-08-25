# Unity Catalog - AI Assistant Context

Operating manual for AI coding assistants working in the Unity Catalog repository, and a dense onboarding reference for humans. Most agent tools load this file automatically at the start of a session. Read it before making changes. Keep edits factual and concrete; this is a working manual, not a design document.

Unity Catalog is an open, multimodal catalog for data and AI. It is built on an OpenAPI specification with an open-source server implementation under the Apache 2.0 license, and it is interoperable with the Apache Hive metastore API and the Apache Iceberg REST catalog API. It governs access to catalogs, schemas, tables, volumes, functions, and models, so authorization and correctness mistakes have direct consequences for the data a deployment protects. Treat every change here as security- and data-sensitive by default.

## Operating principles

These are the non-negotiables. The rest of this file makes them concrete.

1. **Explore before you change.** Read the surrounding code, the module, and the existing tests first. Mirror an existing pattern instead of inventing a new one. When an analogous feature already exists (another service, repository, or connector path), follow it.
2. **Understand every line you submit.** Do not open a pull request with generated or suggested code you cannot explain and defend in review. The most common review question in this project is "why?"; if you cannot answer it for a line, do not include the line.
3. **Tests ship with the change.** New behavior and bug fixes include tests in the same pull request, in the correct module, verifying both success and failure. A logic change without a test will be sent back.
4. **Never hand-edit generated code.** API models and client code are generated from the OpenAPI spec. Edit the spec, regenerate, and commit both together. See "Spec-first: generated code."
5. **Keep pull requests small and single-purpose.** Do not mix a refactor with a feature, or bundle unrelated fixes. A refactor PR contains zero behavior changes. Split them.
6. **Preserve the public contract.** The REST API, the OpenAPI spec, released property and config names, and public client interfaces are compatibility surfaces. Do not break them casually. See "Compatibility and stability."
7. **Format, test, and self-review before requesting review.** Run the formatter and the relevant tests locally, then re-read your own diff against the conventions below and the "Common reasons a pull request gets blocked" checklist. Ask a human only after that.

## Build, test, format, generate

Unity Catalog is a JVM project, and mostly Java: the server, the Java client, the Hadoop connector, and the CLI are Java, while Scala 2.13 is used only for the sbt build and part of the Spark connector. (The Python client and the `ai/` library are Python; the UI is TypeScript.) A single JDK 17 toolchain compiles and runs all the JVM code, so install JDK 17 and point `JAVA_HOME` at it. Always use the bundled launcher `build/sbt`, not a system `sbt`. Scope a task to one module with `module/task` (for example `server/test`).

```bash
build/sbt clean compile                 # compile everything, no tests
build/sbt clean package publishLocal    # build and publish artifacts locally
build/sbt -J-Xmx2G clean test           # full test suite (give it heap)
build/sbt "server/test"                 # one module's tests
build/sbt "server/testOnly io.unitycatalog.server.service.PermissionServiceTest"  # one class (fast loop)
build/sbt -J-Xmx2G jacoco               # coverage report
build/sbt createTarball                 # distributable tarball
build/sbt javafmtAll                    # auto-fix Java formatting (run before every push)
build/sbt generate                      # regenerate OpenAPI models + client SDK (after spec edits)
```

Run the server, CLI, and UI locally:

```bash
build/sbt package
bin/start-uc-server            # starts the server on localhost
bin/uc table list           # exercise the CLI against it
cd ui && yarn install && yarn start
```

Java follows the Google Java Format (a format check runs in CI; run `build/sbt javafmtAll` before pushing). Scala follows the Apache Spark Scala Style Guide. The UI has its own format check (`yarn test:format` in `ui/`).

**CI checks a pull request must satisfy:** unit tests across an engine/format matrix (currently Spark 4.0/4.1/4.2 with the pinned Delta release, plus informational non-blocking runs against a Delta development snapshot; the primary combination also runs license and connector tests), the Java and UI format checks, the cross-Spark build, and the Python client generation and tests. The development-snapshot runs do not block, but a failure there signals an upcoming incompatibility worth understanding.

CI does not always start automatically. A pull request from a fork (the usual path for contributors who are not project members with push access) needs a maintainer or reviewer to approve and trigger the workflow runs, especially for a contributor's first PR; do not be surprised if checks stay unstarted until someone kicks them off during review. All required checks must still pass before a PR can be merged.

## Architecture

### Request flow

The server is an [Armeria](https://armeria.dev/) HTTP application. `UnityCatalogServer` (in `server/src/main/java/io/unitycatalog/server/`) is the bootstrap entry point; it builds the server, registers services, and starts a Vert.x URL-transcoder verticle in front. `ArmeriaServerBuilder` registers each service and selects a protocol-specific request/response converter, so the same server speaks several API dialects:

- `UC`: the native Unity Catalog REST surface (catalogs, schemas, tables, and so on).
- `AUTH` and `SCIM`: control-plane auth and identity (SCIM2 user management), served under the control path.
- `ICEBERG`: the Iceberg REST catalog API.
- `DELTA`: the UC Delta API (Unity Catalog's Delta-dialect REST surface, including Delta commits).

A request maps to an annotated handler method on a `*Service` class in `server/src/main/java/io/unitycatalog/server/service/`. The handler authorizes, calls a `*Repository`, and returns `HttpResponse.ofJson(...)`. Representative handler (`CatalogService`):

```java
@Post("")
@AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER, CREATE_CATALOG) && ...")
@AuthorizeResourceKey(METASTORE)
public HttpResponse createCatalog(CreateCatalog createCatalog) {
  CatalogInfo catalogInfo = catalogRepository.addCatalog(createCatalog);
  initializeBasicAuthorization(catalogInfo.getId());
  return HttpResponse.ofJson(catalogInfo);
}
```

Key points: HTTP verb annotations (`@Post`, `@Get`, `@Patch`, `@Delete`) with `@Param` for query/path params; `@AuthorizeExpression` runs before the body executes; request/response bodies are generated `*Info` / `Create*` / `Update*` model types; success returns `HttpResponse.ofJson(...)`.

### Module and package map

| Path | What it is |
| --- | --- |
| `api/` | The OpenAPI spec and source of truth for the API: `all.yaml` (main), `control.yaml`, `delta.yaml`. `Apis/`, `Models/` are generated docs. |
| `spec/protocols/` | Protocol specs, e.g. `ManagedTablesSpec.md`. |
| `server/` | The catalog server (Java, base package `io.unitycatalog.server`). The core of the project. |
| `server-shaded/` | The server and client combined into a single shaded (fat) JAR, with some dependencies relocated; runnable, entry point `UnityCatalogServer`. |
| `clients/java/`, `clients/python/` | Generated client SDKs (Java and Python). |
| `connectors/spark/` | The Apache Spark connector (Scala). |
| `connectors/hadoop/` | The Hadoop connector. |
| `examples/cli/` | The `uc` command-line client. |
| `integration-tests/` | Cross-module and end-to-end tests. |
| `ui/` | The web UI (React, built with Yarn / Node). |
| `docs/` | User documentation, published with MkDocs (`mkdocs.yml`). |
| `ai/` | The Unity Catalog AI library: function-calling integrations for various frameworks. A separate Python subproject with its own `uv`-based toolchain. It is not the catalog server; do not confuse `ai/` with server code. |
| `bin/` | `bin/start-uc-server`, `bin/uc`. |

Server internal packages (`server/src/main/java/io/unitycatalog/server/`): `service/` (request handlers), `persist/` (repositories, DAOs, transactions), `auth/` (authorization), `security/` (authentication: JWT and security context), `delta/` (Delta integration), `exception/` (error types and handlers), `utils/` (config and shared helpers). Tests mirror this under `server/src/test/...`, with `base/` holding the shared test base classes (`BaseServerTest`, `BaseCRUDTest`) and per-domain extensions.

sbt projects: `server`, `serverShaded`, `client`, `pythonClient`, `controlApi`, generated model projects (`serverModels`, `controlModels`), `apiDocs`, `cli`, `spark`, `hadoop`, `integrationTests`, and the aggregate `root`.

## Domain and authorization model

### Securable hierarchy

The object hierarchy is: **metastore** (the singleton root) contains **catalogs**; each catalog contains **schemas**; each schema contains **tables**, **volumes**, **functions**, and **registered models** (with **model versions**). **External locations** and **credentials** are metastore-level securables. `SecurableType` enumerates them: `METASTORE, CATALOG, SCHEMA, TABLE, VOLUME, FUNCTION, REGISTERED_MODEL, EXTERNAL_LOCATION, CREDENTIAL`.

### Privileges

Access is granted through the `Privileges` enum (`server/src/main/java/io/unitycatalog/server/persist/model/Privileges.java`): `OWNER`, `USE_CATALOG`, `USE_SCHEMA`, `CREATE_CATALOG`, `CREATE_SCHEMA`, `CREATE_TABLE`, `CREATE_FUNCTION`, `CREATE_VOLUME`, `CREATE_MODEL`, `SELECT`, `MODIFY`, `EXECUTE`, `READ_VOLUME`, `WRITE_FILES`, `READ_FILES`, `CREATE_EXTERNAL_LOCATION`, `CREATE_MANAGED_STORAGE`, `CREATE_STORAGE_CREDENTIAL`, and more. Prefer the narrowest privilege that satisfies the operation.

### Authorization idiom

Authorization is declared on the handler with a SpEL expression, not written imperatively:

```java
@AuthorizeExpression("""
    #authorize(#principal, #metastore, OWNER) ||
    #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
    """)
@AuthorizeResourceKey(METASTORE)
public HttpResponse getCatalog(@Param("name") @AuthorizeResourceKey(CATALOG) String name) { ... }
```

- `@AuthorizeExpression` holds a SpEL string evaluated before the handler runs. Context variables: `#principal`, `#metastore`, `#catalog`, `#schema`, `#table`, and so on. Functions: `#authorize(...)` (requires the listed privilege) and `#authorizeAny(...)` (any of them), plus related helpers.
- `@AuthorizeResourceKey` binds a parameter (or the method) to a `SecurableType` so the evaluator can resolve which resource is being checked. `@AuthorizeKey` extracts a raw scalar value (for example a `storage_root` path).
- `@ResponseAuthorizeFilter` marks list/get responses that must be filtered to the entries the principal may see; the handler calls `applyResponseFilter(...)`.
- The authorizer is behind the `UnityCatalogAuthorizer` interface: `JCasbinAuthorizer` in normal operation, `AllowingAuthorizer` (permissive) in tests and simple setups.

**The create-checks-parent, mutate-checks-self rule.** Creating a child checks a privilege on the parent (create a catalog needs `OWNER`/`CREATE_CATALOG` on the metastore). Updating or deleting an object checks a privilege on the object itself (or its parent for delete). Follow this pattern for any new endpoint, and make sure a disabled or default authorization path fails closed (never widen access).

## Persistence and transactions

The `persist/` layer splits into repositories (`*Repository`, the business/transaction layer) and DAOs (`*InfoDAO`, Hibernate entities). A repository method runs inside `TransactionManager.executeWithTransaction(...)`, does its work against the Hibernate `Session`, and converts DAOs to and from the generated `*Info` API models:

```java
public CatalogInfo addCatalog(CreateCatalog createCatalog) {
  return TransactionManager.executeWithTransaction(
      sessionFactory,
      session -> {
        // duplicate check, build the DAO, persist, return the *Info
        ...
      },
      "Failed to add catalog",
      /* readOnly = */ false);
}
```

Conventions:

- **Read paths pass `readOnly = true`; write paths pass `false`.** A path that needs a specific isolation level passes it explicitly; `TransactionManager` saves and restores the connection's isolation so it does not pollute the pooled connection.
- **DAOs extend `IdentifiableDAO`** (UUID id plus name), are annotated `@Entity @Table(name = "uc_...")`, use Lombok (`@Getter @Setter @SuperBuilder @NoArgsConstructor @AllArgsConstructor`), and expose static `from(XInfo)` and instance `toXInfo()` converters. Keep business logic out of DAOs.
- **Backends:** H2 by default (embedded, file-based). MySQL and PostgreSQL are supported for a persistent deployment, configured in `etc/conf/hibernate.properties` (point at your database and add the JDBC driver); see `docs/server/deployment.md`. Hibernate is configured in `persist/utils/HibernateConfigurator`, and tests exercise MySQL and PostgreSQL via Testcontainers. When adding schema, keep any index or column change safe across a server restart on an existing database, and portable across all three backends.
- **Enforce invariants at the database level** (unique constraints) rather than only in code, where duplicate creation or races are possible.

## Error handling

Throw a `BaseException` with an `ErrorCode`; never surface a raw exception or an internal class name to the caller:

```java
throw new BaseException(ErrorCode.CATALOG_ALREADY_EXISTS, "Catalog already exists: " + name);
```

`ErrorCode` (`server/src/main/java/io/unitycatalog/server/exception/ErrorCode.java`) is dual-dialect: each entry maps to a gRPC-style code, a Unity Catalog REST HTTP status, and a Delta error type (and optionally a distinct Delta HTTP status). The registered exception handler renders the error in the dialect of the service that was called (`BaseExceptionHandler`, `IcebergRestExceptionHandler`, `DeltaApiExceptionHandler`). This dual mapping is where compatibility warts live and must be preserved: for example the legacy `*_ALREADY_EXISTS` codes return HTTP 400 on the native REST surface for backward compatibility, while the newer Delta surface returns 409. Do not "fix" one side without understanding the other.

Error-message rules (these come up repeatedly in review): make the message actionable and name the offending value; mask secrets (`***`); do not leak internal type names (say "Cannot determine authentication configuration from options", not the name of the internal provider class); and make the message correct for the specific caller rather than generic.

## Spec-first: generated code

The API is spec-first. `api/all.yaml` (plus `delta.yaml`, `control.yaml`) is the single source of truth. From it the build generates two kinds of output:

- **Generated code**, which is a build artifact and is NOT checked in. It is emitted under `target/` directories (ignored by `.gitignore`) and rebuilt on every compile: the server data models (`io.unitycatalog.server.model.*` `*Info`/`Create*`/`Update*` types under `server/target/models/`), the Java client (`io.unitycatalog.client.*`, including `ApiClient` and the `*Api` classes under `clients/java/target/`), and the Python client.
- **Generated reference docs**, which ARE checked in: the markdown under `api/Apis/`, `api/Models/`, and `api/delta-docs/`. These are produced from the spec, not written by hand.

Rules:

- To change the API, edit the YAML, then run `build/sbt generate` (compiling the affected modules also triggers it).
- **Never hand-edit generated output.** The generated code is overwritten on the next build (and lives under `target/`, so it should never appear in a diff); the checked-in `api/` docs are overwritten by generation too. Change the spec, not the output.
- **Keep the spec and its checked-in docs in sync in the same PR.** A spec change must include the regenerated `api/Apis`, `api/Models`, and `api/delta-docs` markdown alongside the `all.yaml`/`delta.yaml`/`control.yaml` edit, and the description should call out the API change. Do not commit the generated code (anything under `target/`).

## Configuration and properties

Server configuration is read through `ServerProperties` (`server/src/main/java/io/unitycatalog/server/utils/ServerProperties.java`) from `server.properties` (see `etc/conf/`) and the environment, with typed validators (enum, boolean, URL, storage path). Released property and config names are a compatibility surface: do not rename them (see "Compatibility and stability").

## Clients and connectors

- **Java client** (`clients/java`): `io.unitycatalog.client.ApiClient` (an HTTP client wrapper) plus generated `*Api` classes. Its public surface is generated from the spec; treat it as an API.
- **Python client** (`clients/python`): generated from the same OpenAPI spec.
- **Spark connector** (`connectors/spark`, Scala): entry point `io.unitycatalog.spark.UCSingleCatalog` (extends Spark's `StagingTableCatalog`), which talks to the server through the Java client. Connector changes must hold up across the supported Spark and Delta versions (see the CI matrix); do not rely on a single-version assumption.
- **CLI** (`examples/cli`): the `uc` command, built on the Java client.

## Coding conventions

Concrete review expectations for this repository:

- **Write lean code that reads like the surrounding code, not like a generator.** Match the density, structure, and naming of the module you are editing, and make the smallest change that does the job well. Do not pad a change with speculative abstractions, wrapper layers, or defensive checks that nothing needs. AI-assisted changes tend to over-produce, and reviewers here call that out directly, so trim before you push.
- **Justify the change.** The PR description must explain why the change is needed and what breaks without it. Unmotivated changes get held.
- **Reuse before you reinvent.** Search for existing logic that already does this. Do not duplicate a check, a data-type match, or a conversion that already has a home. Duplicated logic across two paths is a frequent review block.
- **Keep code shallow and named.** Reduce nesting (aim for fewer than three levels). Extract a big nested block, a `flatMap` body, or a giant `return` expression into a named private method. Avoid needless verbosity (for example, drive many similar assertions from a list of input/expected pairs rather than repeating a call).
- **Name things for what they are.** A method's name matches what it returns (a method returning a DAO is not named as if it returns the entity). Boolean names start with `is`/`has`/`should`. Do not add a redundant scope prefix (a class inside the client module is `TokenProvider`, not a `UC`-prefixed one). Match the casing and terminology already used (do not introduce a second spelling of an existing concept).
- **Delete what is not used.** Do not add public methods, parameters, config keys, or fields "for later." Add them when a caller needs them.
- **Prefer construction that makes illegal states unrepresentable.** A small sealed hierarchy or enum beats a set of booleans that can combine into invalid combinations.
- **Idioms in use:** `Optional<T>` for absence (with `.orElse`, `.isEmpty`, `.ifPresent`); Lombok for boilerplate (`@Getter`/`@Setter`/builders, `@SneakyThrows` on constructors that declare checked exceptions); SLF4J per-class loggers (not `System.out` for observability); immutable/builder-style construction of model objects.
- **Document the non-obvious, for a human, sparingly.** Public types and methods get doc comments describing intent, parameters, returns, and errors, and capture domain knowledge that cannot be inferred from the code (for example why a storage or identity flow works the way it does). But do not narrate trivial or self-evident lines, and do not write long prose-paragraph comments: prefer a tight sentence, or a short structured list a reader can scan, over a wall of text. Annotate non-obvious literal arguments inline (`/* force= */ true`). Do not delete existing docs while editing.
- **Copyright headers use the current year.**

## Testing conventions

- **Tests live in the module they test.** Server code is tested by server-module tests; a connector test belongs in the connector module. Put a new test in the package that mirrors the code under test, and reuse the shared base test classes (`BaseServerTest`, `BaseCRUDTest`) under `server/src/test/java/io/unitycatalog/server/base/` rather than duplicating setup.
- **Test success and failure.** Verify the failure path throws with the correct error code and message. Do not disable a failing test to make CI green; verify it fails as expected, then fix the cause.
- **Assertions must check the real thing.** Confirm the actual persisted state or the actual converted type, not a proxy that could hide the bug (for example, do not trust a describe that may backfill values from another source).
- **Cover the branches.** Every new branch, error path, and boundary (empty, zero, negative, invalid, concurrent) needs a case. For combinations of boolean conditions, cover the combinations, not just one. Test both the enabled and disabled paths of any flag or capability that guards behavior.
- **Do not generate test bloat; reuse setup.** Do not add heavy tests for trivial data-holder classes with no logic. Do not repeat identical setup in every method: hoist shared setup into `@BeforeEach` and extend the shared base test classes (`BaseServerTest`, `BaseCRUDTest`) rather than re-creating their scaffolding, and reserve `@BeforeAll` for a genuinely immutable, expensive fixture. Collapse several near-identical cases that do not mutate state into one parameterized test, or one method that walks a list of input/expected pairs, instead of copy-pasting a method per input. Object, client, and server setup is expensive, and slow or bloated suites get flagged in review.
- **Stack:** JUnit 5 (`@BeforeEach`/`@AfterEach`/`@TempDir`), servers bound to a random port per test, schema reset between tests. Integration tests spanning server plus a client or connector go in `integration-tests/`.

## Contribution workflow

1. **Discuss first for anything non-trivial.** A "major feature" is any change over 100 lines altered (excluding tests) or any change to user-facing behavior. For those, open a GitHub issue and reach agreement before writing code; larger designs need a design document hosted in or linked from the issue. Small patches and bug fixes need no prior discussion. To claim an open issue, comment `take` on it.
2. **Branch and implement** following the principles and conventions above.
3. **Sign off every commit.** A Developer Certificate of Origin sign-off with your real name is required (no pseudonyms). Use `git commit -s`:
   ```
   Signed-off-by: Jane Smith <jane.smith@email.com>
   ```
4. **Title with a component prefix.** The project consistently uses `[Component]` prefixes that signal scope: `[Server]`, `[Spark]`, `[Hadoop]`, `[Python]`, `[UI]`, `[CLI]`, `[API]`, `[Docs]`, `[Build]`, `[Docker]`. Use the prefix for the primary area you touched.
5. **Fill in the pull request template.** Checklist: a description of the changes is present; a related issue is linked; tests are added for a bug fix or new code; documentation under `docs/` is updated for a feature change.
6. **Write a description that matches the diff.** State what changed and how it affects users. For anything beyond a trivial fix, include the motivation, a short list of the technical changes, the test commands you ran, and a note on backward compatibility or API changes. Reviewers check that the title and description match the actual change; a refactor PR that also changes behavior must say so.
7. **Keep scope tight; run checks locally.** Do not fold in unrelated or cosmetic changes; pull a refactor into its own PR. Run format, tests, and generation locally before pushing (CI may not start until a maintainer approves the run, and must pass before merge; see the CI note under "Build, test, format"), and address any failures before expecting review.
8. **Stacked PRs:** if you split dependent work into a stack, keep the stack listing in each PR description current, and say which PR depends on which.

Code ownership is in `.github/CODEOWNERS`; the owners of the areas you touch are your reviewers, and a code-owner approval is required to merge. Some domains want a domain expert's explicit sign-off even after a high-level approval: authorization and security, protocol and spec changes, the Spark/Delta connector path, and the UC Delta API (including Delta commit handling).

## Compatibility and stability

- **The OpenAPI spec is the contract.** Change behavior by changing the spec first, then regenerating. Prefer additive, backward-compatible changes; the cost of this API style is that once something is added it is hard to remove, so add deliberately.
- **Released names are sacred.** Do not change a released property name, config key, API field, endpoint, or CLI behavior. If a break is truly necessary, call it out explicitly and provide a migration path.
- **Interoperability.** The project stays compatible with the Apache Hive metastore API and the Apache Iceberg REST catalog API. Changes near those surfaces must keep the interop intact.
- **Managed-table semantics** are described in `spec/protocols/ManagedTablesSpec.md`. When changing table behavior, reconcile the code and the spec deliberately. (Schema/migration safety across a restart and across backends is covered under "Persistence and transactions.")

## Common reasons a pull request gets blocked

A fast pre-push scan; each item is detailed in the sections above.

- Missing or mis-placed tests, or assertions that do not validate behavior.
- Unformatted code (`build/sbt javafmtAll` not run).
- Hand-edited generated code, or the spec and its checked-in docs out of sync.
- Missing DCO sign-off on a commit.
- Scope creep, or a "refactor" that also changes behavior.
- A large or behavior-changing change opened without a prior issue.
- Breaking a released API, wire contract, property/config name, or CLI behavior without calling it out.
- Deeply nested or duplicated code, or new public surface nothing uses yet.
- Error messages that leak internal class names or are wrong for the caller.
- Documentation not updated for a user-facing change.

## Pointers

- `CONTRIBUTING.md` for the authoritative contribution rules and governance.
- `AI_POLICY.md` for the policy on AI-assisted contribution and review.
- `README.md` for the quickstart, build, and deployment details.
- `api/README.md` and `api/all.yaml` for the API specification.
- `spec/protocols/` for protocol specifications.
- `docs/` (served via `mkdocs.yml`) for user documentation.
- `.github/CODEOWNERS` for who owns and reviews each area.
