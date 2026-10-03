import { USE_MOCK } from "@/lib/config";
import * as mock from "@/mocks/handlers";
import type {
  CategoryMetric,
  CursorMeta,
  FreePaidMetric,
  GameMetric,
  GenreMetric,
  Health,
  ListResponse,
  ObjectResponse,
  PlatformMetric,
  PlaytimeMetric,
  PurchaseMetric,
  RealtimeGameMetric,
  RecentReview,
} from "@/types/api";
import { ApiError, apiGet } from "./http";

// The real Backend does not fully follow API_CONTRACT_V1 yet (no `meta`, different health body,
// page/page_size pagination, no `limit` on realtime games). The adapters below normalise real
// responses to the contract shape, so components and mocks stay unchanged.
// Remove an adapter once the Backend conforms.

type Params = Record<string, string | number | undefined>;

interface RawList<T> {
  data?: T[];
  meta?: { limit?: number };
  limit?: number; // games/top returns `limit` at the top level
}

async function getList<T>(path: string, params?: Params): Promise<ListResponse<T>> {
  const raw = await apiGet<RawList<T>>(path, params);
  const data = Array.isArray(raw.data) ? raw.data : [];
  const limit = raw.meta?.limit ?? raw.limit;
  return { data, meta: limit === undefined ? { count: data.length } : { count: data.length, limit } };
}

const newestFirst = (rows: RealtimeGameMetric[]) =>
  [...rows].sort((a, b) => b.window_start.localeCompare(a.window_start));

async function getHealth(): Promise<ObjectResponse<Health>> {
  const raw = await apiGet<{ data?: Partial<Health>; status?: string; mongodb?: string }>("/api/health");
  const body = raw.data ?? raw;
  const status = (body.status ?? "UNKNOWN").toUpperCase();
  const up = status === "UP" || status === "HEALTHY";
  // The Backend only answers "healthy" after a successful MongoDB ping.
  return { data: { status: up ? "UP" : status, mongodb: (body.mongodb ?? (up ? "UP" : "UNKNOWN")).toUpperCase() } };
}

interface RawReviewPage {
  data?: RecentReview[];
  meta?: Partial<CursorMeta>;
  page?: number;
  page_size?: number;
  total?: number;
}

async function getRecentReviews(limit: number, appid?: number): Promise<ListResponse<RecentReview, CursorMeta>> {
  // Contract uses `limit`; the current Backend uses `page_size`. Send both (unknown params are ignored).
  const raw = await apiGet<RawReviewPage>("/api/realtime/reviews", { limit, page_size: limit, appid });
  const data = raw.data ?? [];
  const pageSize = raw.page_size ?? limit;
  const hasMore = raw.meta?.has_more ?? (raw.total !== undefined && (raw.page ?? 1) * pageSize < raw.total);
  return {
    data,
    meta: { limit: raw.meta?.limit ?? pageSize, next_cursor: raw.meta?.next_cursor ?? null, has_more: hasMore },
  };
}

async function getRealtimeGames(limit: number): Promise<ListResponse<RealtimeGameMetric>> {
  const res = await getList<RealtimeGameMetric>("/api/realtime/games", { limit });
  // The Backend ignores `limit` and returns every window: cap here.
  const data = newestFirst(res.data).slice(0, limit);
  return { data, meta: { count: data.length, limit } };
}

async function getRealtimeGame(appid: number, limit: number): Promise<ListResponse<RealtimeGameMetric>> {
  try {
    const res = await getList<RealtimeGameMetric>(`/api/realtime/games/${appid}`, { limit });
    const data = newestFirst(res.data).slice(0, limit);
    return { data, meta: { count: data.length, limit } };
  } catch (e) {
    // The Backend answers 404 when a game has no realtime windows yet: an empty state, not an error.
    if (e instanceof ApiError && e.status === 404) return { data: [], meta: { count: 0, limit } };
    throw e;
  }
}

export const api = {
  health: () => (USE_MOCK ? mock.mockHealth() : getHealth()),

  topGames: (limit = 10) =>
    USE_MOCK ? mock.mockTopGames(limit) : getList<GameMetric>("/api/analytics/games/top", { limit }),

  genres: () => (USE_MOCK ? mock.mockGenres() : getList<GenreMetric>("/api/analytics/genres")),
  playtime: () => (USE_MOCK ? mock.mockPlaytime() : getList<PlaytimeMetric>("/api/analytics/playtime")),
  freePaid: () => (USE_MOCK ? mock.mockFreePaid() : getList<FreePaidMetric>("/api/analytics/free-paid")),
  platforms: () => (USE_MOCK ? mock.mockPlatforms() : getList<PlatformMetric>("/api/analytics/platforms")),
  categories: () => (USE_MOCK ? mock.mockCategories() : getList<CategoryMetric>("/api/analytics/categories")),
  purchase: () => (USE_MOCK ? mock.mockPurchase() : getList<PurchaseMetric>("/api/analytics/purchase")),

  recentReviews: (limit = 20, appid?: number) =>
    USE_MOCK ? mock.mockRecentReviews(limit, appid) : getRecentReviews(limit, appid),

  realtimeGames: (limit = 20) => (USE_MOCK ? mock.mockRealtimeGames(limit) : getRealtimeGames(limit)),

  realtimeGame: (appid: number, limit = 24) =>
    USE_MOCK ? mock.mockRealtimeGame(appid, limit) : getRealtimeGame(appid, limit),
};