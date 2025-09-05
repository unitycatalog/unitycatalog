# Unity Catalog OSS — API Evolution Policy

> A short, copy‑paste policy for updating `api/all.yaml` (data-plane) and `api/control.yaml` (auth/permissions) **without breaking existing clients**, while keeping close to Databricks’ commercial API style.

---

## 1) Versioning & Scope

- **Path versioning:** Keep existing endpoints under `/api/2.1/...`.
- **Breaking change rule:** If you *must* break the contract (remove/rename fields, change types/semantics), add a **new path version** (e.g., `/api/2.2/...`) and run both versions during a deprecation window.
- **Spec release tag:** Use `info.version` for the spec’s release identifier (e.g., `2.1.0-uc.12`). Do **not** change the path version for additive work.

**File boundaries**
- `all.yaml`: catalog/table/schema/lineage/data-plane resources & actions.
- `control.yaml`: identity/authz/policy endpoints (e.g., permissions, masking policies, role bindings).

---

## 2) Compatibility Rules (What’s Safe vs Breaking)

**Safe (backward-compatible):**
- Add a **new endpoint**.
- Add **new optional** request properties.
- Add **new optional** response properties.
- Add **custom method actions** on resources (e.g., `POST /…/tables/{id}:compact`).

**Breaking (requires new path version):**
- Remove/rename fields or endpoints.
- Change field types, required/validation, or meaning of existing methods.
- Tighten constraints that cause formerly valid requests to fail.

---

## 3) Naming, Shape & Alignment

- **Mirror Databricks naming** where an equivalent exists (nouns, field names, query params).
- **Uniform prefix:** `/api/<major.minor>/unity-catalog/...`
- **Pagination & errors:** Reuse the existing UC patterns for list pagination and error envelopes.

---

## 4) Stability Labels & Vendor Extensions

Expose stability and provenance in-spec:

```yaml
# Add these vendor extensions on new/changed operations or schemas
x-uc-stage: private_preview | public_preview | ga
x-uc-since: "2.1.0-uc.12"        # first spec release that includes this
# Optional:
x-uc-deprecated: true
x-uc-replaced-by: "/api/2.2/unity-catalog/..."
```

Use the `preview` tag for discoverability:

```yaml
tags:
  - name: preview
    description: "Endpoints or fields in preview; subject to change."
```

---

## 5) Discoverability: Capabilities Endpoint (Additive)

Expose feature flags so clients can adapt at runtime.

```yaml
# all.yaml (excerpt)
openapi: 3.0.3
info:
  title: Unity Catalog Open API
  version: 2.1.0-uc.12
paths:
  /api/2.1/unity-catalog/capabilities:
    get:
      tags: [capabilities, preview]
      summary: List server capabilities and feature flags.
      description: Returns booleans/metadata indicating support for optional features.
      x-uc-stage: public_preview
      x-uc-since: "2.1.0-uc.12"
      responses:
        '200':
          description: OK
          content:
            application/json:
              schema:
                type: object
                properties:
                  iceberg_rest: { type: boolean }
                  credential_vending: { type: boolean }
                  table_compaction: { type: boolean }
```

---

## 6) Examples of Safe Additions

**A) Optional field on an existing schema**

```yaml
components:
  schemas:
    TableInfo:
      type: object
      required: [name, schema, catalog]
      properties:
        name: { type: string }
        schema: { type: string }
        catalog: { type: string }
        retention_days:
          type: integer
          description: "Optional retention; omitted = server default."
          x-uc-stage: public_preview
          x-uc-since: "2.1.0-uc.12"
```

**B) Custom action method (server-side operation)**

```yaml
paths:
  /api/2.1/unity-catalog/tables/{full_name}:compact:
    post:
      tags: [tables, maintenance, preview]
      summary: Enqueue a compaction job for a table.
      x-uc-stage: private_preview
      x-uc-since: "2.1.0-uc.12"
      parameters:
        - in: path
          name: full_name
          required: true
          schema: { type: string }
      requestBody:
        required: false
        content:
          application/json:
            schema:
              type: object
              properties:
                target_file_size_mb: { type: integer, minimum: 8 }
      responses:
        '202': { description: "Accepted; long-running operation enqueued." }
```

---

## 7) Deprecation & Migration

- Mark old operations with `x-uc-deprecated: true` and a **deprecation note** in `description`.
- Provide `x-uc-replaced-by` (if applicable).
- Keep the older path version live until the announced **EOL date**; publish a migration guide.

---

## 8) Documentation & Changelog

- **CHANGELOG.md**: one entry per spec release, with sections per file.
- **Upgrade notes**: call out any new preview → GA transitions and any new path version.

**Changelog stub**

```markdown
## [2.1.0-uc.12] - 2025-08-28
### Added
- GET /api/2.1/unity-catalog/capabilities (public_preview).
- TableInfo.retention_days (optional, preview).
- POST /api/2.1/unity-catalog/tables/{full_name}:compact (private_preview).

### Deprecated
- (none)

### Notes
- No breaking changes; path version remains /api/2.1.
```

---

## 9) PR Checklist (Copy/Paste)

- [ ] Additive only under `/api/2.1` (no behavior changes to existing methods/fields).
- [ ] Names & shapes align with commercial Databricks API when an equivalent exists.
- [ ] `x-uc-stage` and `x-uc-since` present on new/changed surfaces; tagged `preview` if not GA.
- [ ] Pagination & error envelopes reuse existing patterns.
- [ ] `CHANGELOG.md` updated; migration notes present if introducing a new path version.
- [ ] Server/SDK regen passes; OpenAPI lints clean.

---

## Appendix A — Claude Sonnet Context Prompt

Use these messages as a starting point when asking Claude Sonnet to draft or review OpenAPI changes.

### System message (identity & guardrails)

```
You are an API design assistant for the Unity Catalog OSS project. Your job is to propose backward-compatible OpenAPI v3 additions to `api/all.yaml` (data plane) and `api/control.yaml` (auth/permissions) and to flag any proposal that would be a breaking change.

Principles:
- Keep existing endpoints under `/api/2.1/...`.
- Additive-only under the same path version: new endpoints, new optional request fields, new optional response fields, and custom methods (`:action`) for non-CRUD operations.
- Never remove or rename existing fields, change types, or alter the behavior of an existing method. If a breaking change is required, propose a new path version (e.g., `/api/2.2/...`) plus a clear migration note.
- Mirror Databricks' naming and structure when a commercial equivalent exists (nouns, field names, query parameters).
- Reuse pagination and error envelope patterns already present in the OSS spec.
- Annotate stability with vendor extensions (`x-uc-stage`, `x-uc-since`) and the `preview` tag where appropriate.

Output:
- Be precise and conservative about compatibility.
- Prefer small, composable, additive patches.
```

### Developer message (task template)

```
Goal: [Describe the feature or capability to add in one sentence.]

If a related commercial (Databricks) endpoint exists: match path, nouns, and field names. If not, propose a path under `/api/2.1/unity-catalog/...` that aligns with existing naming.

Deliverables:
1) Compatibility check — list which aspects are additive vs. potentially breaking (must be empty for `/api/2.1`).
2) Proposed endpoints — for each, give `METHOD path`, brief description, and why it’s additive.
3) OpenAPI patch — YAML snippets (paths/components) using tags and vendor extensions:
   - `x-uc-stage: private_preview | public_preview | ga`
   - `x-uc-since: "<spec release tag>"`
   - Optional: `x-uc-deprecated`, `x-uc-replaced-by`
4) Errors & pagination — reuse existing patterns; include example 400/403/404/409 responses.
5) Examples — one request and response per endpoint.
6) Changelog entry — one-line addition for `CHANGELOG.md`.
7) (If breaking is truly required) — propose `/api/2.2/...` variant and a short migration guide.
```

### User message (template to fill when invoking Claude)

```
Feature: "[Your feature here]".
Commercial equivalent exists? [yes/no + link if yes]
Constraints: [list any field names/contracts that must remain]
Output format: Markdown with sections per Deliverables list.
```
