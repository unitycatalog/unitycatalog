---
applyTo: "helm/**,charts/**"
---
# Helm/Ops Rules (Bootstrap & Secrets)

- Introduce values for `bootstrap.initialOwner.upn`, `bootstrap.scope`, and optional `bootstrap.windowMinutes`.
- Use a **first‑run Job** (or init container) to call the admin bootstrap endpoint. Use **Kubernetes Secrets** for credentials.
- Do **not** mount static tokens (e.g., `etc/conf/token.txt`) into app containers.
- Provide a **rollback flag** to re‑enable legacy local admin temporarily; document the migration path.
