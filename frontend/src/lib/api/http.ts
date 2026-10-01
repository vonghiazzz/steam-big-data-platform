import { API_BASE_URL } from "@/lib/config";
import type { ApiErrorBody } from "@/types/api";

export class ApiError extends Error {
  constructor(
    public readonly status: number, // 0 = network error
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

type Params = Record<string, string | number | undefined | null>;

export async function apiGet<T>(path: string, params?: Params): Promise<T> {
  const qs = new URLSearchParams();
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value !== undefined && value !== null) qs.set(key, String(value));
  }
  const query = qs.toString();
  const url = `${API_BASE_URL}${path}${query ? `?${query}` : ""}`;

  let res: Response;
  try {
    res = await fetch(url, { headers: { Accept: "application/json" }, cache: "no-store" });
  } catch {
    throw new ApiError(0, "NETWORK_ERROR", "Cannot reach the Backend API");
  }

  if (!res.ok) {
    let code = "UNKNOWN_ERROR";
    let message = `HTTP ${res.status}`;
    try {
      const body = (await res.json()) as Partial<ApiErrorBody>;
      if (body.error) {
        code = body.error.code;
        message = body.error.message;
      }
    } catch {
      /* body was not JSON: keep defaults */
    }
    throw new ApiError(res.status, code, message);
  }
  return (await res.json()) as T;
}