"use client";

import { useMemo, useState } from "react";
import { GameTrendChart } from "@/components/GameTrendChart";
import { LiveIndicator } from "@/components/LiveIndicator";
import { RealtimeMetricsTable } from "@/components/RealtimeMetricsTable";
import { RecentReviewsList } from "@/components/RecentReviewsList";
import { Section } from "@/components/Section";
import { StateView } from "@/components/StateView";
import { useGameNames } from "@/hooks/useGameNames";
import { useRealtimeGame, useRealtimeGames, useRecentReviews, useTopGames } from "@/hooks/useApi";

export default function RealtimePage() {
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const stamp = () => setUpdatedAt(Date.now());

  const [filterAppid, setFilterAppid] = useState<number | undefined>(undefined);
  const [selectedAppid, setSelectedAppid] = useState<number | null>(null);

  const resolveName = useGameNames();
  const allGames = useTopGames(50);
  const gameOptions = useMemo(
    () => [...(allGames.data?.data ?? [])].sort((a, b) => a.game_name.localeCompare(b.game_name)),
    [allGames.data],
  );

  const reviews = useRecentReviews(20, filterAppid, stamp);
  const windows = useRealtimeGames(20, stamp);

  // Default trend game = first game in the latest windows (derived, no effect needed).
  const trendAppid = selectedAppid ?? windows.data?.data[0]?.appid ?? null;
  const trend = useRealtimeGame(trendAppid, 24, stamp);

  const refreshAll = () => {
    void reviews.mutate();
    void windows.mutate();
    void trend.mutate();
  };

  const reviewRows = reviews.data?.data ?? [];
  const windowRows = windows.data?.data ?? [];
  const trendRows = trend.data?.data ?? [];

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <h1 className="text-2xl font-semibold">Realtime</h1>
        <p className="text-sm text-slate-500">
          New reviews observed after the historical snapshot, and one-hour recommendation windows per game.
        </p>
        <LiveIndicator
          updatedAt={updatedAt}
          isValidating={reviews.isValidating || windows.isValidating || trend.isValidating}
          onRefresh={refreshAll}
        />
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Section
          title="Recent Reviews"
          description="Newest first."
          action={
            <select
              aria-label="Filter reviews by game"
              value={filterAppid ?? ""}
              onChange={(e) => setFilterAppid(e.target.value === "" ? undefined : Number(e.target.value))}
              className="max-w-[200px] rounded-md border border-slate-300 bg-white px-2 py-1 text-sm"
            >
              <option value="">All games</option>
              {gameOptions.map((g) => (
                <option key={g.appid} value={g.appid}>
                  {g.game_name}
                </option>
              ))}
            </select>
          }
        >
          <StateView
            isLoading={reviews.isLoading}
            error={reviews.error}
            isEmpty={reviewRows.length === 0}
            emptyText="No new reviews yet. Realtime only shows reviews observed after the historical snapshot, so an empty list is normal."
            onRetry={() => reviews.mutate()}
          >
            <RecentReviewsList reviews={reviewRows} resolveName={resolveName} />
          </StateView>
        </Section>

        <Section title="Realtime Game Metrics" description="Latest one-hour windows (times shown in Vietnam time).">
          <StateView
            isLoading={windows.isLoading}
            error={windows.error}
            isEmpty={windowRows.length === 0}
            emptyText="No completed or in-progress windows yet."
            onRetry={() => windows.mutate()}
          >
            <RealtimeMetricsTable rows={windowRows} resolveName={resolveName} />
          </StateView>
        </Section>
      </div>

      <Section
        title="Recommendation trend"
        description="Recommendation rate (line) and review count (bars) per one-hour window."
        action={
          <select
            aria-label="Choose game for trend"
            value={trendAppid ?? ""}
            onChange={(e) => setSelectedAppid(e.target.value === "" ? null : Number(e.target.value))}
            className="max-w-[220px] rounded-md border border-slate-300 bg-white px-2 py-1 text-sm"
          >
            {trendAppid !== null && !gameOptions.some((g) => g.appid === trendAppid) && (
              <option value={trendAppid}>{resolveName(trendAppid)}</option>
            )}
            {gameOptions.map((g) => (
              <option key={g.appid} value={g.appid}>
                {g.game_name}
              </option>
            ))}
          </select>
        }
      >
        <StateView
          isLoading={trendAppid !== null && trend.isLoading}
          error={trend.error}
          isEmpty={trendAppid === null || trendRows.length === 0}
          emptyText="No windows for this game yet."
          onRetry={() => trend.mutate()}
        >
          <GameTrendChart rows={trendRows} />
        </StateView>
      </Section>
    </div>
  );
}