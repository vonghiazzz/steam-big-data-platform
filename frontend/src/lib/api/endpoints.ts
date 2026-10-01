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
import { apiGet } from "./http";

export const api = {
  health: () =>
    USE_MOCK ? mock.mockHealth() : apiGet<ObjectResponse<Health>>("/api/health"),

  topGames: (limit = 10) =>
    USE_MOCK
      ? mock.mockTopGames(limit)
      : apiGet<ListResponse<GameMetric>>("/api/analytics/games/top", { limit }),

  genres: () =>
    USE_MOCK ? mock.mockGenres() : apiGet<ListResponse<GenreMetric>>("/api/analytics/genres"),

  playtime: () =>
    USE_MOCK ? mock.mockPlaytime() : apiGet<ListResponse<PlaytimeMetric>>("/api/analytics/playtime"),

  freePaid: () =>
    USE_MOCK ? mock.mockFreePaid() : apiGet<ListResponse<FreePaidMetric>>("/api/analytics/free-paid"),

  platforms: () =>
    USE_MOCK ? mock.mockPlatforms() : apiGet<ListResponse<PlatformMetric>>("/api/analytics/platforms"),

  categories: () =>
    USE_MOCK ? mock.mockCategories() : apiGet<ListResponse<CategoryMetric>>("/api/analytics/categories"),

  purchase: () =>
    USE_MOCK ? mock.mockPurchase() : apiGet<ListResponse<PurchaseMetric>>("/api/analytics/purchase"),

  recentReviews: (limit = 20, appid?: number) =>
    USE_MOCK
      ? mock.mockRecentReviews(limit, appid)
      : apiGet<ListResponse<RecentReview, CursorMeta>>("/api/realtime/reviews", { limit, appid }),

  realtimeGames: (limit = 20) =>
    USE_MOCK
      ? mock.mockRealtimeGames(limit)
      : apiGet<ListResponse<RealtimeGameMetric>>("/api/realtime/games", { limit }),

  realtimeGame: (appid: number, limit = 24) =>
    USE_MOCK
      ? mock.mockRealtimeGame(appid, limit)
      : apiGet<ListResponse<RealtimeGameMetric>>(`/api/realtime/games/${appid}`, { limit }),
};