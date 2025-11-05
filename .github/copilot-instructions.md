# unitycatalog Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-11-04

## Active Technologies

- Java 17 (server/core), Java 11 (Spark connector), Scala 2.13 (Spark connector) (001-s3-compatibility)

## Project Structure

```text
src/
tests/
```

## Commands

**Server Operations:**
- Start UC Server: `bin/start-uc-server &`
- Stop UC Server: `pkill -f UnityCatalogServer`
- Check if server is running: `pgrep -f UnityCatalogServer`
- Server logs: Check terminal output or `logs/` directory

**CLI Operations:**
- Run CLI: `bin/uc <command> <subcommand> [options]`
- Example: `bin/uc table list --catalog unity --schema default`
- Example: `bin/uc table read --full_name unity.default.table_name --max_results 10`

**Development Workflow:**
- When debugging/testing: Stop server with `pkill -f UnityCatalogServer`, then restart with `bin/start-uc-server &`
- Always ensure server is stopped before starting a new instance to avoid port conflicts
- **CRITICAL**: After recompiling code (`build/sbt compile`), MUST restart the server for changes to take effect
  - Stop: `pkill -f UnityCatalogServer`
  - Start: `bin/start-uc-server &`
- CLI changes are picked up immediately (no server restart needed), but server changes require restart

## Code Style

**Java (Primary Language):**
- Follow Google Java Style Guide
- Enforce with Checkstyle: `dev/checkstyle-config.xml`
- Format code: `build/sbt javafmtAll`
- Verify formatting: `build/sbt javafmtCheckAll`
- IDE setup: Install google-java-format plugin for IntelliJ/Eclipse

**Build & Quality Gates:**
- Compile: `build/sbt compile` (MUST pass before commit)
- Test: `build/sbt test` (MUST pass before merge)
- Package: `build/sbt package`
- Generate API models: `build/sbt generate` (after API spec changes)

Java 17 (server/core), Java 11 (Spark connector), Scala 2.13 (Spark connector): Follow standard conventions

## Recent Changes

- 001-s3-compatibility: Added Java 17 (server/core), Java 11 (Spark connector), Scala 2.13 (Spark connector)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
