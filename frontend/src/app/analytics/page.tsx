"use client";

import { useMemo, useState } from "react";
import { MetricsTable } from "@/components/MetricsTable";
import { RateBarChart } from "@/components/RateBarChart";
import { Section } from "@/components/Section";
import { StateView } from "@/components/StateView";
import {
  useCategories,
  useFreePaid,
  useGenres,
  usePlatforms,
  usePlaytime,
  usePurchase,
} from "@/hooks/useApi";
import { formatHours, formatNumber, formatPercent } from "@/lib/format";
import type { ReviewMetrics } from "@/types/api";

const toChart = <T extends ReviewMetrics>(rows: T[], getLabel: (r: T) => string) =>
  rows.map((r) => ({ label: getLabel(r), rate: r.recommendation_rate, reviews: r.review_count }));

const byRateDesc = <T extends ReviewMetrics>(rows: T[]) =>
  [...rows].sort((a, b) => b.recommendation_rate - a.recommendation_rate);

const prettify = (s: string) => s.replace(/_/g, " ");

// Display order only. The API already provides the bucket; the frontend never derives it.
const PLAYTIME_ORDER = ["0-2h", "2-10h", "10-50h", "50h+", "MISSING"];

const SECTIONS = [
  ["genres", "Genre"],
  ["playtime", "Playtime"],
  ["free-paid", "Free vs Paid"],
  ["platforms", "Platform"],
  ["categories", "Category"],
  ["purchase", "Purchase Source"],
] as const;

function GenreSection() {
  const { data, error, isLoading, mutate } = useGenres();
  const rows = useMemo(() => byRateDesc(data?.data ?? []), [data]);
  return (
    <Section
      title="Genre Analytics"
      description="Ranked by recommendation rate. UNKNOWN is a valid genre value (games without a genre), not an error."
    >
      <StateView isLoading={isLoading} error={error} isEmpty={rows.length === 0} onRetry={() => mutate()}>
        <RateBarChart data={toChart(rows, (r) => r.genre)} />
        <div className="mt-4">
          <MetricsTable rows={rows} labelHeader="Genre" getLabel={(r) => r.genre} />
        </div>
      </StateView>
    </Section>
  );
}

function PlaytimeSection() {
  const { data, error, isLoading, mutate } = usePlaytime();
  const rows = useMemo(() => {
    const rank = (b: string) => {
      const i = PLAYTIME_ORDER.indexOf(b);
      return i === -1 ? PLAYTIME_ORDER.length : i;
    };
    return [...(data?.data ?? [])].sort((a, b) => rank(a.playtime_bucket) - rank(b.playtime_bucket));
  }, [data]);
  return (
    <Section
      title="Playtime Analytics"
      description="Recommendation rate by playtime bucket. MISSING means no playtime data for those reviews."
    >
      <StateView isLoading={isLoading} error={error} isEmpty={rows.length === 0} onRetry={() => mutate()}>
        <RateBarChart data={toChart(rows, (r) => r.playtime_bucket)} />
        <div className="mt-4">
          <MetricsTable
            rows={rows}
            labelHeader="Playtime bucket"
            getLabel={(r) => r.playtime_bucket}
            extraColumns={[{ header: "Avg playtime", render: (r) => formatHours(r.avg_playtime_hours) }]}
          />
        </div>
      </StateView>
    </Section>
  );
}

function FreePaidSection() {
  const { data, error, isLoading, mutate } = useFreePaid();
  const rows = data?.data ?? [];
  return (
    <Section title="Free vs Paid" description="Comparison between free and paid games.">
      <StateView isLoading={isLoading} error={error} isEmpty={rows.length === 0} onRetry={() => mutate()}>
        <div className="grid gap-4 sm:grid-cols-2">
          {rows.map((r) => (
            <div key={r.game_type} className="rounded-lg border border-slate-200 p-4">
              <div className="flex items-baseline justify-between">
                <h3 className="text-base font-semibold">{r.game_type}</h3>
                <span className="text-sm text-slate-500">{formatNumber(r.game_count)} games</span>
              </div>
              <p className="mt-2 text-3xl font-semibold tabular-nums">{formatPercent(r.recommendation_rate)}</p>
              <p className="text-xs text-slate-500">recommendation rate</p>
              <div className="mt-3 flex h-2.5 overflow-hidden rounded-full bg-red-200">
                <div className="bg-emerald-500" style={{ width: `${r.recommendation_rate * 100}%` }} />
              </div>
              <p className="mt-2 text-xs text-slate-500">
                {formatNumber(r.review_count)} reviews · {formatNumber(r.positive_reviews)} positive ·{" "}
                {formatNumber(r.negative_reviews)} negative
              </p>
            </div>
          ))}
        </div>
      </StateView>
    </Section>
  );
}

function PlatformSection() {
  const { data, error, isLoading, mutate } = usePlatforms();
  const rows = useMemo(() => byRateDesc(data?.data ?? []), [data]);
  return (
    <Section
      title="Platform Analytics"
      description="A game can support several platforms, so a review counts once per platform and the totals below can exceed the project total. This is expected."
    >
      <StateView isLoading={isLoading} error={error} isEmpty={rows.length === 0} onRetry={() => mutate()}>
        <RateBarChart data={toChart(rows, (r) => r.platform)} />
        <div className="mt-4">
          <MetricsTable rows={rows} labelHeader="Platform" getLabel={(r) => r.platform} />
        </div>
      </StateView>
    </Section>
  );
}

function CategorySection() {
  const { data, error, isLoading, mutate } = useCategories();
  const [showAll, setShowAll] = useState(false);
  const sorted = useMemo(
    () => [...(data?.data ?? [])].sort((a, b) => b.review_count - a.review_count),
    [data],
  );
  const rows = showAll ? sorted : sorted.slice(0, 15);
  return (
    <Section
      title="Category Analytics"
      description="Steam categories overlap (a game has many), so totals are not additive. Sorted by review count."
      action={
        sorted.length > 15 ? (
          <button
            onClick={() => setShowAll((v) => !v)}
            className="rounded-md border border-slate-300 px-3 py-1 text-sm hover:bg-slate-50"
          >
            {showAll ? "Show top 15" : `Show all ${sorted.length}`}
          </button>
        ) : undefined
      }
    >
      <StateView isLoading={isLoading} error={error} isEmpty={sorted.length === 0} onRetry={() => mutate()}>
        <RateBarChart data={toChart(rows, (r) => r.category)} />
        <div className="mt-4">
          <MetricsTable rows={rows} labelHeader="Category" getLabel={(r) => r.category} />
        </div>
      </StateView>
    </Section>
  );
}

function PurchaseSection() {
  const { data, error, isLoading, mutate } = usePurchase();
  const rows = data?.data ?? [];
  return (
    <Section title="Purchase Source" description="Reviews from Steam purchases versus other sources.">
      <StateView isLoading={isLoading} error={error} isEmpty={rows.length === 0} onRetry={() => mutate()}>
        <RateBarChart data={toChart(rows, (r) => prettify(r.purchase_source))} />
        <div className="mt-4">
          <MetricsTable rows={rows} labelHeader="Source" getLabel={(r) => prettify(r.purchase_source)} />
        </div>
      </StateView>
    </Section>
  );
}

export default function AnalyticsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Analytics</h1>
        <p className="text-sm text-slate-500">Historical aggregates from the Gold analytics layer.</p>
        <nav className="mt-3 flex flex-wrap gap-2">
          {SECTIONS.map(([id, label]) => (
            <a
              key={id}
              href={`#${id}`}
              className="rounded-full border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-600 hover:bg-slate-100"
            >
              {label}
            </a>
          ))}
        </nav>
      </div>

      <div id="genres" className="scroll-mt-20"><GenreSection /></div>
      <div id="playtime" className="scroll-mt-20"><PlaytimeSection /></div>
      <div id="free-paid" className="scroll-mt-20"><FreePaidSection /></div>
      <div id="platforms" className="scroll-mt-20"><PlatformSection /></div>
      <div id="categories" className="scroll-mt-20"><CategorySection /></div>
      <div id="purchase" className="scroll-mt-20"><PurchaseSection /></div>
    </div>
  );
}