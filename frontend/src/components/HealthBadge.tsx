"use client";

import { useHealth } from "@/hooks/useApi";
import { USE_MOCK } from "@/lib/config";

export function HealthBadge() {
  const { data, error, isLoading } = useHealth();

  let label = "Checking…";
  let tone = "bg-slate-100 text-slate-600";
  if (error) {
    label = "API unreachable";
    tone = "bg-red-100 text-red-700";
  } else if (data) {
    const ok = data.data.status === "UP" && data.data.mongodb === "UP";
    label = ok ? "API UP" : `API ${data.data.status} / Mongo ${data.data.mongodb}`;
    tone = ok ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700";
  } else if (!isLoading) {
    label = "Unknown";
  }

  return (
    <div className="flex items-center gap-2">
      {USE_MOCK && (
        <span className="rounded-full bg-violet-100 px-2.5 py-0.5 text-xs font-medium text-violet-700">
          Mock data
        </span>
      )}
      <span className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${tone}`}>{label}</span>
    </div>
  );
}