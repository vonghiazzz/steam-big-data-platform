import { ApiError } from "@/lib/api/http";
import type {
  CursorMeta,
  Health,
  ListMeta,
  ListResponse,
  ObjectResponse,
  RealtimeGameMetric,
  RecentReview,
} from "@/types/api";
import {
  MOCK_CATEGORIES,
  MOCK_FREE_PAID,
  MOCK_GAMES,
  MOCK_GENRES,
  MOCK_PLATFORMS,
  MOCK_PLAYTIME,
  MOCK_PURCHASE,
  mulberry32,
  toMetrics,
} from "./data";

const delay = (ms = 250) => new Promise((resolve) => setTimeout(resolve, ms));

function checkLimit(limit: number, max: number) {
  if (!Number.isInteger(limit) || limit < 1 || limit > max) {
    throw new ApiError(400, "INVALID_ARGUMENT", `limit must be between 1 and ${max}`);
  }
}

function list<T>(rows: T[]): ListResponse<T> {
  return { data: rows, meta: { count: rows.length } };
}

const toIso = (ms: number) => new Date(ms).toISOString().replace(/\.\d{3}Z$/, "Z");

// ---------- Historical ----------
export async function mockHealth(): Promise<ObjectResponse<Health>> {
  await delay(100);
  return { data: { status: "UP", mongodb: "UP" } };
}

export async function mockTopGames(limit: number): Promise<ListResponse<(typeof MOCK_GAMES)[number]>> {
  await delay();
  checkLimit(limit, 50);
  const data = [...MOCK_GAMES]
    .sort((a, b) => b.recommendation_rate - a.recommendation_rate || b.review_count - a.review_count)
    .slice(0, limit);
  return { data, meta: { count: data.length, limit } as ListMeta };
}

export const mockGenres = async () => (await delay(), list(MOCK_GENRES));
export const mockPlaytime = async () => (await delay(), list(MOCK_PLAYTIME));
export const mockFreePaid = async () => (await delay(), list(MOCK_FREE_PAID));
export const mockPlatforms = async () => (await delay(), list(MOCK_PLATFORMS));
export const mockCategories = async () => (await delay(), list(MOCK_CATEGORIES));
export const mockPurchase = async () => (await delay(), list(MOCK_PURCHASE));

// ---------- Realtime ----------
const SLOT_MS = 45_000;
const HOUR_MS = 3_600_000;
const ACTIVE_APPIDS = [570, 730, 4000, 413150, 1086940, 1091500, 1172470, 578080, 431960, 438100, 381210, 108600];

function reviewForSlot(slot: number, forcedAppid?: number): RecentReview {
  const rng = mulberry32(slot);
  const appid = forcedAppid ?? ACTIVE_APPIDS[Math.floor(rng() * ACTIVE_APPIDS.length)];
  const votedUp = rng() < 0.74;
  const hasPlaytime = rng() > 0.08;
  const atReview = Math.floor(rng() * 3000);
  const extra = Math.floor(rng() * 6000);
  const purchase = rng();
  const free = rng();
  const noIngest = rng() < 0.05;
  const created = slot * SLOT_MS;
  return {
    recommendationid: String(200_000_000 + slot),
    appid,
    voted_up: votedUp,
    playtime_at_review: hasPlaytime ? atReview : null,
    playtime_forever: hasPlaytime ? atReview + extra : null,
    steam_purchase: purchase < 0.08 ? null : purchase < 0.85,
    received_for_free: free < 0.08 ? null : free < 0.15,
    timestamp_created: toIso(created),
    stream_ingested_at: noIngest ? null : toIso(created + 5_000),
  };
}

export async function mockRecentReviews(
  limit: number,
  appid?: number,
): Promise<ListResponse<RecentReview, CursorMeta>> {
  await delay();
  checkLimit(limit, 100);
  const nowSlot = Math.floor(Date.now() / SLOT_MS);
  const data = Array.from({ length: limit }, (_, i) => reviewForSlot(nowSlot - i, appid));
  const last = data[data.length - 1];
  return { data, meta: { limit, next_cursor: last ? last.recommendationid : null, has_more: true } };
}

function windowFor(appid: number, hourIndex: number): RealtimeGameMetric {
  const rng = mulberry32(appid * 31 + hourIndex);
  const count = 1 + Math.floor(rng() * 8);
  const pos = Math.round(count * (0.5 + rng() * 0.5));
  return {
    appid,
    window_start: toIso(hourIndex * HOUR_MS),
    window_end: toIso((hourIndex + 1) * HOUR_MS),
    ...toMetrics(pos, count - pos),
  };
}

export async function mockRealtimeGames(limit: number): Promise<ListResponse<RealtimeGameMetric>> {
  await delay();
  checkLimit(limit, 100);
  const hour = Math.floor(Date.now() / HOUR_MS);
  const data = ACTIVE_APPIDS.map((appid) => windowFor(appid, hour))
    .sort((a, b) => b.review_count - a.review_count)
    .slice(0, limit);
  return { data, meta: { count: data.length, limit } };
}

export async function mockRealtimeGame(appid: number, limit: number): Promise<ListResponse<RealtimeGameMetric>> {
  await delay();
  checkLimit(limit, 100);
  if (!MOCK_GAMES.some((g) => g.appid === appid)) {
    throw new ApiError(404, "NOT_FOUND", `Game ${appid} was not found`);
  }
  const hour = Math.floor(Date.now() / HOUR_MS);
  // Newest first. The contract does not guarantee order, so charts must sort by window_start.
  const data = Array.from({ length: limit }, (_, i) => windowFor(appid, hour - i));
  return { data, meta: { count: data.length, limit } };
}