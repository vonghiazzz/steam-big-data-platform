// ---------- Envelopes ----------
export interface ListMeta { count: number; limit?: number }
export interface CursorMeta { limit: number; next_cursor: string | null; has_more: boolean }
export interface ObjectResponse<T> { data: T }
export interface ListResponse<T, M = ListMeta> { data: T[]; meta: M }

export type ApiErrorCode =
  | "INVALID_ARGUMENT"
  | "NOT_FOUND"
  | "SERVICE_UNAVAILABLE"
  | (string & {});
export interface ApiErrorBody { error: { code: ApiErrorCode; message: string } }

// ---------- Health ----------
export interface Health { status: string; mongodb: string }

// ---------- Historical analytics ----------
export interface ReviewMetrics {
  review_count: number;
  positive_reviews: number;
  negative_reviews: number;
  recommendation_rate: number; // 0..1
}

export interface GameMetric extends ReviewMetrics { appid: number; game_name: string }
export interface GenreMetric extends ReviewMetrics { genre: string } // "UNKNOWN" is valid

export type PlaytimeBucket = "0-2h" | "2-10h" | "10-50h" | "50h+" | "MISSING";
export interface PlaytimeMetric extends ReviewMetrics {
  playtime_bucket: PlaytimeBucket; // never derive buckets on the frontend
  avg_playtime_hours: number | null;
}

export interface FreePaidMetric extends ReviewMetrics {
  game_type: "FREE" | "PAID";
  game_count: number;
}
export interface PlatformMetric extends ReviewMetrics { platform: string }   // multi-membership
export interface CategoryMetric extends ReviewMetrics { category: string }   // multi-membership
export interface PurchaseMetric extends ReviewMetrics { purchase_source: string }

// ---------- Realtime ----------
export interface RecentReview {
  recommendationid: string; // always string
  appid: number;
  voted_up: boolean;
  playtime_at_review: number | null;
  playtime_forever: number | null;
  steam_purchase: boolean | null;
  received_for_free: boolean | null;
  timestamp_created: string;        // ISO-8601 UTC
  stream_ingested_at: string | null; // ISO-8601 UTC
}

export interface RealtimeGameMetric extends ReviewMetrics {
  appid: number;
  window_start: string; // ISO-8601 UTC
  window_end: string;   // ISO-8601 UTC
}