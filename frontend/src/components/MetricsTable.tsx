import type { ReactNode } from "react";
import { formatNumber, formatPercent } from "@/lib/format";
import type { ReviewMetrics } from "@/types/api";

interface ExtraColumn<T> {
  header: string;
  render: (row: T) => ReactNode;
}

interface Props<T extends ReviewMetrics> {
  rows: T[];
  labelHeader: string;
  getLabel: (row: T) => string;
  extraColumns?: ExtraColumn<T>[];
}

export function MetricsTable<T extends ReviewMetrics>({ rows, labelHeader, getLabel, extraColumns = [] }: Props<T>) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[560px] text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wide text-slate-500">
            <th className="py-2 pr-3">{labelHeader}</th>
            {extraColumns.map((c) => (
              <th key={c.header} className="py-2 pr-3 text-right">
                {c.header}
              </th>
            ))}
            <th className="py-2 pr-3 text-right">Reviews</th>
            <th className="py-2 pr-3 text-right">Positive</th>
            <th className="py-2 pr-3 text-right">Negative</th>
            <th className="py-2 text-right">Recommend</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={getLabel(r)} className="border-b border-slate-100 last:border-0">
              <td className="py-2 pr-3 font-medium">{getLabel(r)}</td>
              {extraColumns.map((c) => (
                <td key={c.header} className="py-2 pr-3 text-right tabular-nums">
                  {c.render(r)}
                </td>
              ))}
              <td className="py-2 pr-3 text-right tabular-nums">{formatNumber(r.review_count)}</td>
              <td className="py-2 pr-3 text-right tabular-nums text-emerald-700">
                {formatNumber(r.positive_reviews)}
              </td>
              <td className="py-2 pr-3 text-right tabular-nums text-red-700">
                {formatNumber(r.negative_reviews)}
              </td>
              <td className="py-2 text-right font-medium tabular-nums">{formatPercent(r.recommendation_rate)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}