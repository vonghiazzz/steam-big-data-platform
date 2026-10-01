"use client";

import { useCallback, useMemo } from "react";
import { useTopGames } from "@/hooks/useApi";

/** Returns a resolver appid -> game name. Falls back to "App <appid>". */
export function useGameNames() {
  const { data } = useTopGames(50);
  const names = useMemo(
    () => new Map<number, string>((data?.data ?? []).map((g) => [g.appid, g.game_name])),
    [data],
  );
  return useCallback((appid: number) => names.get(appid) ?? `App ${appid}`, [names]);
}