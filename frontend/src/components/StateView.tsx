"use client";

import type { ReactNode } from "react";
import { ApiError } from "@/lib/api/http";

interface Props {
  isLoading: boolean;
  error?: unknown;
  isEmpty?: boolean;
  emptyText?: string;
  onRetry?: () => void;
  children: ReactNode;
}

function errorText(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.code === "NETWORK_ERROR") {
      return "Cannot reach the Backend API. Check NEXT_PUBLIC_API_BASE_URL and CORS.";
    }
    return `${error.message} (${error.code})`;
  }
  return "Something went wrong.";
}

export function StateView({ isLoading, error, isEmpty, emptyText, onRetry, children }: Props) {
  if (error) {
    return (
      <div role="alert" className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-800">
        <p>{errorText(error)}</p>
        {onRetry && (
          <button
            onClick={onRetry}
            className="mt-2 rounded border border-red-300 bg-white px-3 py-1 text-xs font-medium hover:bg-red-100"
          >
            Retry
          </button>
        )}
      </div>
    );
  }
  if (isLoading) {
    return <div className="h-40 animate-pulse rounded-lg bg-slate-200" aria-busy="true" />;
  }
  if (isEmpty) {
    return (
      <div className="rounded-lg border border-dashed border-slate-300 p-6 text-center text-sm text-slate-500">
        {emptyText ?? "No data yet."}
      </div>
    );
  }
  return <>{children}</>;
}