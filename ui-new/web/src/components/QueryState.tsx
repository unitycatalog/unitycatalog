import type { ReactNode } from "react";
import { Skeleton } from "@/components/ui/skeleton";

// QueryState renders the loading / error boilerplate shared by every page that
// wraps a single TanStack Query, delegating to `children` only once data is
// present. Keeps the pages free of repeated isLoading/error branches.
export function QueryState({
  isLoading,
  error,
  children,
}: {
  isLoading: boolean;
  error: Error | null;
  children: ReactNode;
}) {
  if (isLoading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-6 w-64" />
        <Skeleton className="h-24 w-full" />
      </div>
    );
  }
  if (error) {
    return (
      <p className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">{error.message}</p>
    );
  }
  return <>{children}</>;
}
