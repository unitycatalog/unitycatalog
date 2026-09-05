import type { ReactNode } from "react";
import { vi } from "vitest";

// A minimal stand-in for @tanstack/react-router so components that use Link /
// useParams / useNavigate / Navigate can render without a real router tree.
// Test files opt in with:  vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

// Shared spies tests can import to assert navigation and control params.
export const navigateMock = vi.fn();
export let paramsValue: Record<string, string> = {};

export function setParams(params: Record<string, string>) {
  paramsValue = params;
}

export function Link({
  to,
  params,
  children,
  ...rest
}: {
  to?: string;
  params?: Record<string, string>;
  children?: ReactNode;
  [key: string]: unknown;
}) {
  // Render a plain anchor whose href is the interpolated route, so tests can
  // assert link targets without the real router.
  let href = to ?? "#";
  if (params) {
    for (const [k, v] of Object.entries(params)) href = href.replace(`$${k}`, v);
  }
  const { className } = rest as { className?: string };
  return (
    <a href={href} className={className}>
      {children}
    </a>
  );
}

export function useParams() {
  return paramsValue;
}

export function useNavigate() {
  return navigateMock;
}

export function Navigate({ to }: { to: string }) {
  return <div data-testid="navigate" data-to={to} />;
}

export function Outlet() {
  return <div data-testid="outlet" />;
}
