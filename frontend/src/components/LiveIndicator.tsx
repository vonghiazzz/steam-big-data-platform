"use client";

import { POLL_INTERVAL_MS } from "@/lib/config";
import { formatDateTime } from "@/lib/format";

interface Props {
  updatedAt: number | null;
  isValidating: boolean;
  onRefresh: () => void;
}

export function LiveIndicator({ updatedAt, isValidating, onRefresh }: Props) {
  return (
    <div className="flex flex-wrap items-center gap-3 text-sm text-slate-600">
      <span className="flex items-center gap-2">
        <span className="relative flex h-2.5 w-2.5">
          {isValidating && (
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-75" />
          )}
          <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-emerald-500" />
        </span>
        Auto-refresh every {POLL_INTERVAL_MS / 1000}s
      </span>
      <span className="text-slate-400">·</span>
      <span>Updated: {updatedAt ? formatDateTime(new Date(updatedAt).toISOString()) : "—"}</span>
      <button
        onClick={onRefresh}
        className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium hover:bg-slate-50"
      >
        Refresh now
      </button>
    </div>
  );
}