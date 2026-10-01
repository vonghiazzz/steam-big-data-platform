import type { GameMetric } from "@/types/api";
import { formatNumber, formatPercent } from "@/lib/format";

export function GamesTable({ rows }: { rows: GameMetric[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[640px] text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wide text-slate-500">
            <th className="py-2 pr-3">#</th>
            <th className="py-2 pr-3">Game</th>
            <th className="py-2 pr-3 text-right">Reviews</th>
            <th className="py-2 pr-3 text-right">Positive</th>
            <th className="py-2 pr-3 text-right">Negative</th>
            <th className="py-2 text-right">Recommend</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((g, i) => (
            <tr key={g.appid} className="border-b border-slate-100 last:border-0">
              <td className="py-2 pr-3 tabular-nums text-slate-400">{i + 1}</td>
              <td className="py-2 pr-3">
                <span className="font-medium">{g.game_name}</span>
                <span className="ml-2 text-xs text-slate-400">#{g.appid}</span>
              </td>
              <td className="py-2 pr-3 text-right tabular-nums">{formatNumber(g.review_count)}</td>
              <td className="py-2 pr-3 text-right tabular-nums text-emerald-700">
                {formatNumber(g.positive_reviews)}
              </td>
              <td className="py-2 pr-3 text-right tabular-nums text-red-700">
                {formatNumber(g.negative_reviews)}
              </td>
              <td className="py-2 text-right font-medium tabular-nums">
                {formatPercent(g.recommendation_rate)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}