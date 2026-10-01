// NEXT_PUBLIC_* values are inlined at build time: restart `npm run dev` after editing .env.local
export const API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE_URL ?? "").replace(/\/$/, "");
export const USE_MOCK = process.env.NEXT_PUBLIC_USE_MOCK !== "false";

export const POLL_INTERVAL_MS = 15_000; // realtime polling (contract: 10-30s)
export const DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh";