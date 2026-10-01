"use client";

import Link from "next/link";
import { GamesTable } from "@/components/GamesTable";
import { Section } from "@/components/Section";
import { StatCard } from "@/components/StatCard";
import { StateView } from "@/components/StateView";
import { useFreePaid, useTopGames } from "@/hooks/useApi";
import { formatNumber, formatPercent } from "@/lib/format";

export default function OverviewPage() {
  const freePaid = useFreePaid();
  const top = useTopGames(5);

  // FREE and PAID do not overlap, so their sum is the project total.
  // (Do NOT sum platforms/categories: they are multi-membership.)
  const rows = freePaid.data?.data ?? [];
  const games = rows.reduce((s, r) => s + r.game_count, 0);
  const reviews = rows.reduce((s, r) => s + r.review_count, 0);
  const positive = rows.reduce((s, r) => s + r.positive_reviews, 0);
  const negative = rows.reduce((s, r) => s + r.negative_reviews, 0);
  const rate = reviews === 0 ? null : positive / reviews;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Overview</h1>
        <p className="text-sm text-slate-500">Historical snapshot of Steam games and review behaviour.</p>
      </div>

      <StateView
        isLoading={freePaid.isLoading}
        error={freePaid.error}
        isEmpty={rows.length === 0}
        onRetry={() => freePaid.mutate()}
      >
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
          <StatCard label="Games" value={formatNumber(games)} hint="FREE + PAID" />
          <StatCard label="Reviews" value={formatNumber(reviews)} />
          <StatCard label="Positive" value={formatNumber(positive)} />
          <StatCard label="Negative" value={formatNumber(negative)} />
          <StatCard label="Recommendation rate" value={formatPercent(rate)} />
        </div>

        <div className="mt-4">
          <div className="flex h-3 overflow-hidden rounded-full bg-red-200" title="Positive vs negative">
            <div className="bg-emerald-500" style={{ width: `${(rate ?? 0) * 100}%` }} />
          </div>
          <p className="mt-1 text-xs text-slate-500">Green = positive, red = negative</p>
        </div>
      </StateView>

      <Section
        title="Top 5 games"
        description="Ranked by recommendation rate."
        action={
          <Link href="/games" className="text-sm font-medium text-teal-700 hover:underline">
            View all →
          </Link>
        }
      >
        <StateView
          isLoading={top.isLoading}
          error={top.error}
          isEmpty={(top.data?.data.length ?? 0) === 0}
          onRetry={() => top.mutate()}
        >
          <GamesTable rows={top.data?.data ?? []} />
        </StateView>
      </Section>
    </div>
  );
}