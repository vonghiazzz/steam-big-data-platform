import { formatNumber, formatPercent, formatWindow } from "@/lib/format";
import type { RealtimeGameMetric } from "@/types/api";

interface Props {
  rows: RealtimeGameMetric[];
  resolveName: (appid: number) => string;
}

export function RealtimeMetricsTable({ rows, resolveName }: Props) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[560px] text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wide text-slate-500">
            <th className="py-2 pr-3">Game</th>
            <th className="py-2 pr-3">Window (VN time)</th>
            <th className="py-2 pr-3 text-right">Reviews</th>
            <th className="py-2 pr-3 text-right">Positive</th>
            <th className="py-2 pr-3 text-right">Negative</th>
            <th className="py-2 text-right">Recommend</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={`${r.appid}-${r.window_start}`} className="border-b border-slate-100 last:border-0">
              <td className="py-2 pr-3 font-medium">{resolveName(r.appid)}</td>
              <td className="py-2 pr-3 tabular-nums text-slate-600">{formatWindow(r.window_start, r.window_end)}</td>
              <td className="py-2 pr-3 text-right tabular-nums">{formatNumber(r.review_count)}</td>
              <td className="py-2 pr-3 text-right tabular-nums text-emerald-700">{formatNumber(r.positive_reviews)}</td>
              <td className="py-2 pr-3 text-right tabular-nums text-red-700">{formatNumber(r.negative_reviews)}</td>
              <td className="py-2 text-right font-medium tabular-nums">{formatPercent(r.recommendation_rate)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}