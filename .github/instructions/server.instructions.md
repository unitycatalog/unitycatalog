---
applyTo: "server/**,backend/**,src/server/**"
---
# Server Rules (AuthN/Z, OWNER, PATs)

- **JCasbin enforcement**: enumerate enforcement points; gate privileged routes with explicit actions; test OWNER vs lesser roles.
- **Local admin deprecation**: remove reliance on `etc/conf/token.txt` where possible; replace with Azure bootstrap.
- **OWNER binding**: implement explicit role bindings (global or scoped). Avoid subject‑based shortcuts.
- **PATs**: store only hashed token IDs + metadata; return token value once; support scopes, TTL, revoke; audit create/last‑used.
- **Logs**: never log secrets or PAT values; include correlation IDs; structured errors (403/409/422).

Deliverables should include tight unit/integration tests and migration notes.
