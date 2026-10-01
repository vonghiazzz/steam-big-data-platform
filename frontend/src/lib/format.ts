import { DEFAULT_TIMEZONE } from "@/lib/config";

const numberFmt = new Intl.NumberFormat("en-US");

export const formatNumber = (n: number | null | undefined) =>
  n === null || n === undefined ? "—" : numberFmt.format(n);

/** recommendation_rate is 0..1 in the API. */
export const formatPercent = (rate: number | null | undefined, digits = 1) =>
  rate === null || rate === undefined ? "—" : `${(rate * 100).toFixed(digits)}%`;

function parse(iso: string | null | undefined): Date | null {
  if (!iso) return null;
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? null : d;
}

/** API sends UTC ISO-8601; display in Asia/Ho_Chi_Minh. */
export function formatDateTime(iso: string | null | undefined): string {
  const d = parse(iso);
  if (!d) return "—";
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: DEFAULT_TIMEZONE,
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(d);
}

export function formatTime(iso: string | null | undefined): string {
  const d = parse(iso);
  if (!d) return "—";
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: DEFAULT_TIMEZONE,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(d);
}

export const formatWindow = (start: string, end: string) =>
  `${formatTime(start)} – ${formatTime(end)}`;

export function formatRelative(iso: string | null | undefined, now = Date.now()): string {
  const d = parse(iso);
  if (!d) return "—";
  const sec = Math.max(0, Math.round((now - d.getTime()) / 1000));
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

/**
 * playtime_at_review / playtime_forever. ASSUMPTION: minutes (Steam API convention).
 * Confirm with the Backend owner; if wrong, only this function needs to change.
 */
export function formatPlaytimeMinutes(min: number | null | undefined): string {
  if (min === null || min === undefined) return "—";
  if (min < 60) return `${min}m`;
  const h = Math.floor(min / 60);
  const m = min % 60;
  return m === 0 ? `${h}h` : `${h}h ${m}m`;
}

export const formatHours = (h: number | null | undefined) =>
  h === null || h === undefined ? "—" : `${h.toFixed(1)}h`;