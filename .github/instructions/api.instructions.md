---
applyTo: "api/**/*.yaml"
---
# API Spec Editing Rules (OpenAPI)

- **Additive only under `/api/2.1/...`**. New endpoints or optional fields are OK; do **not** remove/rename/change types or semantics.
- Label new/changed surfaces with `x-uc-stage` (preview/ga) and `x-uc-since` (spec release tag). Reuse pagination & error envelopes.
- Prefer `:action` custom methods for non‑CRUD operations (e.g., `.../tables/{id}:compact`) rather than overloading existing endpoints.
- For **PAT** and **admin bootstrap**:
  - Tokens under `/api/2.1/unity-catalog/tokens` (create/list/revoke, write‑once `token_value`).
  - `POST /api/2.1/unity-catalog/admins/bootstrap-owner` to bind OWNER for an Azure principal; idempotent; clear 403/409 semantics.
- Always provide **examples** and a **CHANGELOG** entry; keep names aligned with the commercial API when applicable.
