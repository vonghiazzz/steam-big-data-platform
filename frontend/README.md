# Steam Analytics Dashboard (Frontend V1)

Next.js (App Router) + TypeScript + Tailwind dashboard for the Steam Big Data Platform.

The frontend only calls the Backend API (`docs/API_CONTRACT_V1.md`). It never connects to
MongoDB, HDFS, Kafka or Spark.

## Run

```bash
cd frontend
npm install
cp .env.example .env.local   # then edit if needed
npm run dev                  # http://localhost:3000
```

Production check:

```bash
npm run lint && npx tsc --noEmit
npm run build && npm run start
```

## Environment variables

| Variable | Meaning |
| --- | --- |
| `NEXT_PUBLIC_API_BASE_URL` | Backend API base URL, e.g. `http://localhost:8080`. No trailing path. |
| `NEXT_PUBLIC_USE_MOCK` | `true` = built-in mock data (default), `false` = call the real Backend API. |

`NEXT_PUBLIC_*` values are read at startup/build time: restart `npm run dev` after editing `.env.local`.
The Backend must allow CORS for the frontend origin (e.g. `http://localhost:3000`).

## Switch from mock to the real API

1. Start the Backend API.
2. In `.env.local` set `NEXT_PUBLIC_API_BASE_URL=http://localhost:<backend-port>` and `NEXT_PUBLIC_USE_MOCK=false`.
3. Restart `npm run dev`.

## Pages

| Route | Content |
| --- | --- |
| `/` | Overview: totals, positive/negative bar, top 5 games |
| `/games` | Top Games (Top 10/20/50) |
| `/analytics` | Genre, Playtime, Free vs Paid, Platform, Category, Purchase Source |
| `/realtime` | Recent Reviews, Realtime Game Metrics, per-game trend (polling every 15s) |

## Structure

```
src/
  app/            routes (layout + 4 pages)
  components/     shared UI (tables, charts, StateView, NavBar, ...)
  hooks/          SWR hooks (historical: no polling, realtime: polling) + game-name lookup
  lib/            config, formatters, api/http.ts (client + ApiError), api/endpoints.ts
  mocks/          mock data + handlers returning the exact contract shapes
  types/api.ts    TypeScript types of the API contract
```

## Contract notes handled by the UI

- Envelope `{ data, meta }`, errors `{ error: { code, message } }`.
- `recommendation_rate` is 0..1 (shown as %). Timestamps are UTC ISO-8601, shown in `Asia/Ho_Chi_Minh`.
- `recommendationid` is a string. Nullable review fields render as `—`.
- `UNKNOWN` (genre) and `MISSING` (playtime bucket) are valid values, not errors.
- Platform and Category are multi-membership: totals are never summed or validated against 25,000.
- Realtime responses carry only `appid`; names come from `/api/analytics/games/top?limit=50`.
- An empty realtime list is a valid state (no new reviews since the snapshot).

## Backend compatibility

Playtime fields (`playtime_at_review`, `playtime_forever`) are in minutes (verified on the MongoDB samples).

The current Backend does not fully follow `API_CONTRACT_V1.md`. `src/lib/api/endpoints.ts` contains adapters
that normalise real responses to the contract shape (no `meta`, `{"status":"healthy"}` health body,
`page`/`page_size` review pagination, no `limit` on realtime games, 404 for a game without windows).
Remove an adapter once the Backend conforms.