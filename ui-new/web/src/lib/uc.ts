import { proxyClient } from "@/lib/transport";
import { getToken } from "@/lib/session";

// Native Unity Catalog REST surface prefixes. Mirrors the current ui's constants.
export const UC_API_PREFIX = "/api/2.1/unity-catalog";
export const UC_AUTH_API_PREFIX = "/api/1.0/unity-control";

/** Skips per-table column/property hydration on GET /tables list. */
export const LIST_TABLES_QUERY = {
  omit_columns: "true",
  omit_properties: "true",
} as const;

/** Merge {@link LIST_TABLES_QUERY} into a GET /tables list query. */
export function withListTablesQuery(
  query: Record<string, string | undefined>,
): Record<string, string | undefined> {
  return { ...LIST_TABLES_QUERY, ...query };
}

export type RawResult = {
  httpStatus: number;
  ok: boolean;
  body: string;
};

export type CallOpts = {
  query?: Record<string, string | undefined>;
  body?: unknown;
  // Content-Type for the body. Defaults to application/json server-side. The
  // OAuth token endpoint uses application/x-www-form-urlencoded.
  contentType?: string;
};

function toKV(query?: Record<string, string | undefined>) {
  const out: { key: string; value: string }[] = [];
  if (!query) return out;
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== "") out.push({ key, value });
  }
  return out;
}

// ucCall returns the raw proxied response (status + body string). Auth is
// cookie-based and handled by the bridge, so serverUrl/token are left empty and
// resolved server-side (the bridge falls back to its configured UC_SERVER).
export async function ucCall(method: string, path: string, opts: CallOpts = {}): Promise<RawResult> {
  const res = await proxyClient.call({
    serverUrl: "",
    // When set, the bridge forwards this as `Authorization: Bearer`, bypassing
    // cookie login. Empty means rely on the forwarded session cookie.
    token: getToken(),
    method,
    path,
    query: toKV(opts.query),
    jsonBody:
      opts.body === undefined || opts.body === null || opts.body === ""
        ? ""
        : typeof opts.body === "string"
          ? opts.body
          : JSON.stringify(opts.body),
    contentType: opts.contentType ?? "",
  });
  return { httpStatus: res.httpStatus, ok: res.ok, body: res.body };
}

// UCError carries the UC HTTP status + raw body so callers can branch on
// expected statuses (e.g. 401 -> unauthenticated) instead of guessing.
export class UCError extends Error {
  status: number;
  body: string;
  constructor(status: number, body: string) {
    super(`UC request failed (HTTP ${status}): ${body}`);
    this.status = status;
    this.body = body;
    this.name = "UCError";
  }
}

// ucJson parses the response as JSON and throws UCError on non-2xx.
export async function ucJson<T = unknown>(method: string, path: string, opts: CallOpts = {}): Promise<T> {
  const res = await ucCall(method, path, opts);
  if (!res.ok) throw new UCError(res.httpStatus, res.body);
  if (!res.body) return undefined as T;
  try {
    return JSON.parse(res.body) as T;
  } catch {
    return res.body as unknown as T;
  }
}

export function prettyJson(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

// formatEpoch renders a UC millisecond epoch (number or numeric string) as a
// locale date/time, or an em dash when absent/invalid.
export function formatEpoch(ms?: number | string | null): string {
  if (ms === undefined || ms === null || ms === "") return "—";
  const n = typeof ms === "string" ? Number(ms) : ms;
  if (!Number.isFinite(n) || n <= 0) return "—";
  return new Date(n).toLocaleString();
}
