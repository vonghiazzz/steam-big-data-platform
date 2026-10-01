"use client";

import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { formatPercent } from "@/lib/format";

export interface RateDatum {
  label: string;
  rate: number; // 0..1
  reviews: number;
}

interface Props {
  data: RateDatum[];
  rowHeight?: number;
}

const shorten = (s: string) => (s.length > 24 ? `${s.slice(0, 23)}…` : s);

export function RateBarChart({ data, rowHeight = 28 }: Props) {
  const height = Math.max(160, data.length * rowHeight + 48);
  return (
    <div style={{ height }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} layout="vertical" margin={{ left: 8, right: 24 }}>
          <CartesianGrid strokeDasharray="3 3" horizontal={false} />
          <XAxis type="number" domain={[0, 1]} tickFormatter={(v: number) => formatPercent(v, 0)} />
          <YAxis
            type="category"
            dataKey="label"
            width={170}
            interval={0}
            tick={{ fontSize: 12 }}
            tickFormatter={(v: string) => shorten(v)}
          />
          <Tooltip
            formatter={(value) => (typeof value === "number" ? formatPercent(value) : String(value ?? ""))}
          />
          <Bar dataKey="rate" name="Recommendation rate" fill="#0f766e" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}