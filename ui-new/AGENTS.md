# ui-new — AI assistant context

Operating notes for the `ui-new` subproject: a rebuild of the Unity Catalog web
UI as a same-origin SPA (`web/`) backed by a small Rust Connect bridge
(`server/`). See `README.md` for the dev/build workflow. This is separate from
the legacy `ui/` (CRA + Ant Design) at the repo root; do not conflate the two.

## Layout

- `web/` — Bun + Vite + React 19 SPA. shadcn/ui on Tailwind v4, TanStack Router
  (file-based) + TanStack Query, connect-web + connect-query.
- `server/` — Rust axum bridge. Typed domain Connect services translate to UC
  REST; `UnityProxyService/Call` remains for control-plane endpoints. It also
  serves `/config`, `/healthz`, and the built SPA.
- `proto/` — typed catalog-domain RPCs plus the generic control-plane proxy.
  `buf.gen.yaml` generates TS clients into `web/src/gen` (`bun run generate`
  from `web/`).

## Principles

1. Domain reads and mutations use the generated Connect services in
   `server/src/services/`; keep their translation to UC REST explicit and
   narrow. The generic proxy is reserved for auth, SCIM, and permissions.
   Both paths forward `Cookie` and `Authorization`; the generic auth path also
   copies UC's `Set-Cookie` onto the same-origin response.
2. Auth mirrors the legacy `ui/`: cookie-based token-exchange (`/auth/tokens`,
   `ext=cookie`) + SCIM `/scim2/Me`, with an auth-disabled mode. Provider
   enablement is runtime via `/config`, not build-time env.
3. Domain data access uses generated method descriptors with
   `useQuery`/`useMutation` under `web/src/hooks`. `useUcQuery` / `ucJson` are
   only for control-plane surfaces without typed contracts. Do not scatter raw
   clients through components.
4. UI is shadcn components (`web/src/components/ui/*`, imported via `@/lib/utils`
   `cn`). Reuse the shared building blocks (`EntityHeader`, `CatalogCrumbs`,
   `MetaGrid`, `PropertiesCard`, `DescriptionCard`, `PermissionsPanel`,
   `QueryState`) instead of re-implementing page chrome.
5. Never hand-edit generated code: `web/src/gen/**` (buf) and
   `web/src/routeTree.gen.ts` (TanStack router). Change the proto or the routes,
   then regenerate.

## Common tasks

- Change the bridge contract: edit `proto/uc/v1/proxy.proto`, run
  `bun run generate` in `web/`, and update the Rust `CallRequest`/`CallResponse`
  in `server/src/proxy.rs` to match (fields use proto3 JSON camelCase).
- Change a typed domain contract: edit the matching file under
  `proto/uc/v1/`, keep its `buf.validate` rules aligned with the UC API, then
  run `bun run proto:check` and `bun run generate` from `web/`. Update the translation in
  `server/src/services/` or `server/src/mapping.rs`; `server/build.rs` generates
  Connect and `protovalidate-buffa` code at compile time.
- Add a page: add a typed hook in `web/src/hooks`, a page in `web/src/pages`, and
  a file route in `web/src/routes/_authed/**` that reads params and renders it.

## Testing

The SPA uses Vitest + React Testing Library (config in `web/vitest.config.ts`,
env `happy-dom`). Shared harness lives in `web/src/test/`:
- `providers.tsx` — `renderWithProviders` / `renderHookWithProviders` wrap a
  QueryClient and in-memory implementations of the generic proxy and typed
  domain services, so hooks/pages run their real query paths against canned UC
  replies (pass a `UcHandler` keyed by method + path).
- `tanstack-router-mock.tsx` — a Link/useParams/useNavigate/Navigate stand-in;
  opt in per file with `vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"))`.
- `localstorage-polyfill.ts` — the test DOM has no Web Storage; the polyfill is
  imported first in `setup.ts`.

Tests co-locate as `*.test.ts(x)` next to the code. Coverage is gated at 80%
(generated code, vendored `components/ui`, routes, and the entry are excluded).
When testing components that read UC data, prefer a `UcHandler` over mocking the
hooks, so the hook + query-key wiring is exercised too.

## Checks before pushing

```bash
cd ui-new/web && bun run typecheck && bun run test:coverage && bun run build
cd ui-new/server && cargo test && cargo fmt --check
```
