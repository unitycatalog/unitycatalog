# [PROJECT NAME] Development Guidelines

Auto-generated from all feature plans. Last updated: [DATE]

## Active Technologies

[EXTRACTED FROM ALL PLAN.MD FILES]

## Project Structure

```text
[ACTUAL STRUCTURE FROM PLANS]
```

## Commands

[ONLY COMMANDS FOR ACTIVE TECHNOLOGIES]

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

[LANGUAGE-SPECIFIC, ONLY FOR LANGUAGES IN USE]

## Recent Changes

[LAST 3 FEATURES AND WHAT THEY ADDED]

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
