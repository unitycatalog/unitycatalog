#!/usr/bin/env bun
/**
 * Strips registry proxy URLs out of bun.lock.
 *
 * Local installs may route through a private npm proxy/registry, which bakes a
 * host-specific URL into the resolution (second) field of each package entry.
 * That URL must never land in a PR, so this resets it to "" — the value Bun
 * uses for the default registry.
 *
 * Host-agnostic: matches any http(s) registry tarball URL (".../-/<name>-<version>.tgz")
 * sitting in the resolution slot. Git, GitHub, file, and already-empty
 * resolutions are left untouched. Names, versions, dependency maps, and
 * integrity hashes are never modified.
 *
 * Usage:
 *   bun run scripts/strip-bun-lock-proxy.ts          # rewrite bun.lock in place
 *   bun run scripts/strip-bun-lock-proxy.ts --check   # exit 1 if proxy URLs exist (no write)
 */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const LOCK_PATH = resolve(import.meta.dirname, "..", "bun.lock");

// Group 1 is the entry head: ["<name>@<version>", . Group 2 is the proxied
// resolution URL, which we drop (replace the whole match with group 1 + "").
const PROXY_RESOLUTION =
  /(\["[^"]+@[^"]+", )"https?:\/\/[^"]*\/-\/[^"]*\.tgz"/g;

const checkOnly = process.argv.includes("--check");

const original = readFileSync(LOCK_PATH, "utf8");
const matches = original.match(PROXY_RESOLUTION)?.length ?? 0;

if (checkOnly) {
  if (matches > 0) {
    console.error(
      `bun.lock contains ${matches} proxied resolution URL(s). Run: bun run strip-lock-proxy`,
    );
    process.exit(1);
  }
  console.log("bun.lock is clean — no proxy URLs found.");
  process.exit(0);
}

if (matches === 0) {
  console.log("bun.lock is already clean — no proxy URLs found.");
  process.exit(0);
}

const stripped = original.replace(PROXY_RESOLUTION, '$1""');
writeFileSync(LOCK_PATH, stripped);
console.log(`Stripped ${matches} proxy URL(s) from bun.lock.`);
