import type {
  CategoryMetric,
  FreePaidMetric,
  GameMetric,
  GenreMetric,
  PlatformMetric,
  PlaytimeMetric,
  PurchaseMetric,
  ReviewMetrics,
} from "@/types/api";

/** Deterministic PRNG so mock data is stable between reloads. */
export function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function toMetrics(pos: number, neg: number): ReviewMetrics {
  const total = pos + neg;
  return {
    review_count: total,
    positive_reviews: pos,
    negative_reviews: neg,
    recommendation_rate: total === 0 ? 0 : Math.round((pos / total) * 1e6) / 1e6,
  };
}

// The 50 appids of the project snapshot (from data/raw/bronze_ready/reviews_by_game).
const APPIDS = [
  1049590, 1085660, 108600, 1086940, 1091500, 1142710, 1144200, 1151340, 1172470, 1285190,
  1286830, 1295660, 1361210, 1643320, 1973530, 2054970, 2246340, 2357570, 236390, 2399420,
  2399830, 2456740, 2483190, 2537590, 2767030, 2807960, 284160, 289070, 3028330, 304390,
  3405690, 3595270, 376210, 381210, 394360, 4000, 4032350, 413150, 431960, 4356430,
  438100, 440900, 489830, 534380, 570, 578080, 594650, 648800, 730, 949230,
];

// Only well-known titles are named; the rest use a placeholder (real names come from the API).
const KNOWN_NAMES: Record<number, string> = {
  570: "Dota 2",
  730: "Counter-Strike 2",
  4000: "Garry's Mod",
  413150: "Stardew Valley",
  431960: "Wallpaper Engine",
  438100: "VRChat",
  578080: "PUBG: BATTLEGROUNDS",
  1086940: "Baldur's Gate 3",
  1091500: "Cyberpunk 2077",
  1172470: "Apex Legends",
  108600: "Project Zomboid",
  284160: "BeamNG.drive",
  289070: "Sid Meier's Civilization VI",
  236390: "War Thunder",
  381210: "Dead by Daylight",
  489830: "The Elder Scrolls V: Skyrim Special Edition",
  648800: "Raft",
  1085660: "Destiny 2",
  394360: "Hearts of Iron IV",
  376210: "The Isle",
};

export const MOCK_GAMES: GameMetric[] = APPIDS.map((appid) => {
  const rng = mulberry32(appid);
  const neg = appid === 413150 ? 4 : 4 + Math.floor(rng() * 170); // Stardew = 496/4 as in the contract
  return {
    appid,
    game_name: KNOWN_NAMES[appid] ?? `Game ${appid}`,
    ...toMetrics(500 - neg, neg),
  };
});

const GENRE_NAMES = [
  "Action", "Adventure", "Casual", "Free To Play", "Indie", "Massively Multiplayer", "RPG",
  "Racing", "Simulation", "Sports", "Strategy", "Early Access", "Violent", "Gore",
  "Utilities", "Animation & Modeling", "UNKNOWN",
];

export const MOCK_GENRES: GenreMetric[] = GENRE_NAMES.map((genre, i) => {
  const rng = mulberry32(1000 + i);
  const total = 300 + Math.floor(rng() * 12000);
  const pos = Math.round(total * (0.6 + rng() * 0.3));
  return { genre, ...toMetrics(pos, total - pos) };
});

// Sums to 25,000 reviews / 18,321 positive, like the real baseline.
export const MOCK_PLAYTIME: PlaytimeMetric[] = [
  { playtime_bucket: "0-2h", ...toMetrics(293, 624), avg_playtime_hours: 0.895 },
  { playtime_bucket: "2-10h", ...toMetrics(2800, 1400), avg_playtime_hours: 5.12 },
  { playtime_bucket: "10-50h", ...toMetrics(6100, 2000), avg_playtime_hours: 24.37 },
  { playtime_bucket: "50h+", ...toMetrics(8900, 2600), avg_playtime_hours: 188.64 },
  { playtime_bucket: "MISSING", ...toMetrics(228, 55), avg_playtime_hours: null },
];

export const MOCK_FREE_PAID: FreePaidMetric[] = [
  { game_type: "FREE", game_count: 13, ...toMetrics(4522, 1978) },
  { game_type: "PAID", game_count: 37, ...toMetrics(13799, 4701) },
];

// Multi-membership: the sum across platforms is intentionally > 25,000.
export const MOCK_PLATFORMS: PlatformMetric[] = [
  { platform: "WINDOWS", ...toMetrics(18321, 6679) },
  { platform: "MAC", ...toMetrics(6900, 2300) },
  { platform: "LINUX", ...toMetrics(5600, 2000) },
];

const CATEGORY_NAMES = [
  "Single-player", "Multi-player", "PvP", "Online PvP", "Co-op", "Online Co-op",
  "Shared/Split Screen", "Steam Achievements", "Steam Trading Cards", "Steam Cloud",
  "Full controller support", "Partial Controller Support", "Steam Workshop",
  "In-App Purchases", "Includes level editor", "Remote Play Together",
  "Cross-Platform Multiplayer", "Family Sharing", "Captions available",
  "Steam Leaderboards", "Stats", "Commentary available", "MMO", "VR Supported",
];

export const MOCK_CATEGORIES: CategoryMetric[] = CATEGORY_NAMES.map((category, i) => {
  const rng = mulberry32(2000 + i);
  const total = 200 + Math.floor(rng() * 16000);
  const pos = Math.round(total * (0.6 + rng() * 0.3));
  return { category, ...toMetrics(pos, total - pos) };
});

export const MOCK_PURCHASE: PurchaseMetric[] = [
  { purchase_source: "STEAM_PURCHASE", ...toMetrics(15714, 5380) },
  { purchase_source: "OTHER_SOURCE", ...toMetrics(2607, 1299) },
];