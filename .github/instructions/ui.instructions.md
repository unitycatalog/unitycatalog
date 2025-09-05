---
applyTo: "ui/**,web/**,frontend/**"
---
# UI Rules (Admin Bootstrap, PAT UX)

- Add a feature‑flagged **Admin Bootstrap** page: authenticate via Azure, search principal, select scope, call backend to grant OWNER.
- On success, optionally create a PAT for the new admin; **display once** with copy‑to‑clipboard and irreversible dismiss.
- Clear error states for 401/403/409; no secret logging; instrument with audit events.
- Tests: e2e for grant/re‑grant (idempotent), forbidden, and PAT creation flow.
