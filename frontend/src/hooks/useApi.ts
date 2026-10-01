"use client";

import useSWR from "swr";
import { api } from "@/lib/api/endpoints";
import { API_BASE_URL, POLL_INTERVAL_MS, USE_MOCK } from "@/lib/config";

// Include the data source in the key so switching mock <-> real never reuses stale cache.
const src = USE_MOCK ? "mock" : API_BASE_URL;

const historical = { revalidateOnFocus: false, shouldRetryOnError: false } as const;
const realtime = {
  refreshInterval: POLL_INTERVAL_MS,
  keepPreviousData: true,
  errorRetryInterval: POLL_INTERVAL_MS,
} as const;

export const useHealth = () =>
  useSWR([src, "health"], () => api.health(), { refreshInterval: 30_000 });

export const useTopGames = (limit = 10) =>
  useSWR([src, "games/top", limit], () => api.topGames(limit), historical);
export const useGenres = () => useSWR([src, "genres"], () => api.genres(), historical);
export const usePlaytime = () => useSWR([src, "playtime"], () => api.playtime(), historical);
export const useFreePaid = () => useSWR([src, "free-paid"], () => api.freePaid(), historical);
export const usePlatforms = () => useSWR([src, "platforms"], () => api.platforms(), historical);
export const useCategories = () => useSWR([src, "categories"], () => api.categories(), historical);
export const usePurchase = () => useSWR([src, "purchase"], () => api.purchase(), historical);

export const useRecentReviews = (limit = 20, appid?: number, onSuccess?: () => void) =>
  useSWR([src, "realtime/reviews", limit, appid ?? null], () => api.recentReviews(limit, appid), {
    ...realtime,
    onSuccess,
  });

export const useRealtimeGames = (limit = 20, onSuccess?: () => void) =>
  useSWR([src, "realtime/games", limit], () => api.realtimeGames(limit), { ...realtime, onSuccess });

export const useRealtimeGame = (appid: number | null, limit = 24, onSuccess?: () => void) =>
  useSWR(
    appid === null ? null : [src, "realtime/game", appid, limit],
    () => api.realtimeGame(appid as number, limit),
    { ...realtime, onSuccess },
  );