## Summary

Adds a GitHub Actions workflow (`.github/workflows/publish-snapshot.yml`) that automatically publishes `SNAPSHOT` artifacts from the `main` branch to the [Sonatype Central snapshots repository](https://central.sonatype.com/repository/maven-snapshots/io/unitycatalog/).

## Motivation

Currently there is no automated way for downstream users and integrators to consume the latest unreleased changes in `main`. Periodic snapshot publishing fills that gap without requiring a manual release cut.

## What's in this PR

- New workflow file: `.github/workflows/publish-snapshot.yml`
- Triggers:
  - **Daily** at 02:00 UTC (cron schedule)
  - **On every push** to `main`
  - **Manually** via `workflow_dispatch` (Actions UI or `gh workflow run`)
- Branch guard: job is skipped unless running on `main`, preventing accidental publishes from forks or feature branches
- Version guard: aborts if the current version is not a `*-SNAPSHOT`, protecting against accidentally publishing a release artifact to the snapshots repo
- Uses `+publish` (cross-Scala) with credentials injected via GitHub secrets — nothing is echoed in logs

## Required setup before merging

Two secrets must be added to the repository (**Settings → Secrets and variables → Actions**):

| Secret name | Description |
|---|---|
| `SONATYPE_USER` | Sonatype Central username |
| `SONATYPE_PASSWORD` | Sonatype Central password or user token |

## Testing

- [ ] Run `actionlint` on the workflow file locally — no errors
- [ ] Verify version guard locally: `./build/sbt -mem 4096 "show version"` returns `*-SNAPSHOT`
- [ ] Trigger `workflow_dispatch` manually on this branch and confirm the job is **skipped** (branch guard working correctly)
- [ ] Run a dry-run publish locally with real credentials and confirm artifacts appear at `https://central.sonatype.com/repository/maven-snapshots/io/unitycatalog/`
