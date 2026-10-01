"use client";

import { Bar, CartesianGrid, ComposedChart, Legend, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { formatPercent, formatTime } from "@/lib/format";
import type { RealtimeGameMetric } from "@/types/api";

export function GameTrendChart({ rows }: { rows: RealtimeGameMetric[] }) {
  // The contract does not guarantee order: always sort by window_start (ISO UTC sorts lexicographically).
  const data = [...rows]
    .sort((a, b) => a.window_start.localeCompare(b.window_start))
    .map((r) => ({
      time: formatTime(r.window_start),
      "Recommendation rate": r.recommendation_rate,
      Reviews: r.review_count,
    }));

  return (
    <div className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ left: 0, right: 8 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" tick={{ fontSize: 12 }} />
          <YAxis yAxisId="rate" domain={[0, 1]} tickFormatter={(v: number) => formatPercent(v, 0)} width={48} />
          <YAxis yAxisId="count" orientation="right" allowDecimals={false} width={32} />
          <Tooltip
            formatter={(value, name) =>
              name === "Recommendation rate" && typeof value === "number"
                ? [formatPercent(value), name]
                : [String(value ?? ""), name]
            }
          />
          <Legend />
          <Bar yAxisId="count" dataKey="Reviews" fill="#cbd5e1" />
          <Line yAxisId="rate" dataKey="Recommendation rate" stroke="#0f766e" strokeWidth={2} dot />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}