"use client";

import { useState } from "react";
import { GamesTable } from "@/components/GamesTable";
import { RateBarChart } from "@/components/RateBarChart";
import { Section } from "@/components/Section";
import { StateView } from "@/components/StateView";
import { useTopGames } from "@/hooks/useApi";

const LIMITS = [10, 20, 50]; // contract: limit 1..50

export default function TopGamesPage() {
  const [limit, setLimit] = useState(10);
  const { data, error, isLoading, mutate } = useTopGames(limit);
  const rows = data?.data ?? [];

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">Top Games</h1>
          <p className="text-sm text-slate-500">Games ranked by recommendation rate.</p>
        </div>
        <label className="flex items-center gap-2 text-sm">
          Show
          <select
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value))}
            className="rounded-md border border-slate-300 bg-white px-2 py-1"
          >
            {LIMITS.map((l) => (
              <option key={l} value={l}>
                Top {l}
              </option>
            ))}
          </select>
        </label>
      </div>

      <StateView isLoading={isLoading} error={error} isEmpty={rows.length === 0} onRetry={() => mutate()}>
        <Section title="Recommendation rate">
          <RateBarChart data={rows.map((g) => ({ label: g.game_name, rate: g.recommendation_rate, reviews: g.review_count }))} />
        </Section>
        <div className="mt-6">
          <Section title="Details">
            <GamesTable rows={rows} />
          </Section>
        </div>
      </StateView>
    </div>
  );
}