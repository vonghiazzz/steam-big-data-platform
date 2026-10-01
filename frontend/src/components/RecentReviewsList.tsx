import { formatDateTime, formatPlaytimeMinutes, formatTime } from "@/lib/format";
import type { RecentReview } from "@/types/api";

interface Props {
  reviews: RecentReview[];
  resolveName: (appid: number) => string;
}

function Badge({ children }: { children: React.ReactNode }) {
  return <span className="rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-600">{children}</span>;
}

export function RecentReviewsList({ reviews, resolveName }: Props) {
  return (
    <ul className="max-h-[560px] divide-y divide-slate-100 overflow-y-auto">
      {reviews.map((r) => (
        <li key={r.recommendationid} className="flex items-start gap-3 py-3">
          <span
            aria-label={r.voted_up ? "Recommended" : "Not recommended"}
            className={`mt-0.5 inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-sm ${
              r.voted_up ? "bg-emerald-100" : "bg-red-100"
            }`}
          >
            {r.voted_up ? "👍" : "👎"}
          </span>
          <div className="min-w-0 flex-1">
            <p className="truncate text-sm font-medium">{resolveName(r.appid)}</p>
            <p className="mt-0.5 text-xs text-slate-500">
              Playtime at review {formatPlaytimeMinutes(r.playtime_at_review)} · total{" "}
              {formatPlaytimeMinutes(r.playtime_forever)}
            </p>
            <div className="mt-1 flex flex-wrap gap-1">
              {r.steam_purchase === true && <Badge>Steam purchase</Badge>}
              {r.steam_purchase === false && <Badge>Other source</Badge>}
              {r.received_for_free === true && <Badge>Received for free</Badge>}
            </div>
          </div>
          <div className="shrink-0 text-right text-xs text-slate-500">
            <p>{formatDateTime(r.timestamp_created)}</p>
            {r.stream_ingested_at && <p className="text-slate-400">ingested {formatTime(r.stream_ingested_at)}</p>}
          </div>
        </li>
      ))}
    </ul>
  );
}