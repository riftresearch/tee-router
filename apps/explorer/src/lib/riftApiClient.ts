export type U256 = string;

export type Chain =
  | { kind: "BITCOIN" }
  | { kind: "EVM"; chainId: number }
  | { kind: string; [key: string]: unknown };

export type Token =
  | { kind: "NATIVE"; decimals: number }
  | { kind: "TOKEN"; address: string; decimals: number }
  | { kind: string; decimals?: number; [key: string]: unknown };

export type Currency = {
  chain: Chain;
  token: Token;
};

export type TeeStatus = {
  status: string;
};

export type RawOtcJson = {
  status?: string;
  metadata?: {
    start_asset?: string;
    end_asset?: string;
    integrator_name?: string;
  };
  userDepositStatus?: unknown;
  settlementStatus?: unknown;
  latestRefund?: unknown;
  createdAt?: string;
  updatedAt?: string;
  failureReason?: string | null;
};

export type Order = {
  orderId: string;
  routeType: string;
  provider: string | null;
  path: string[];
  status: string;
  senderAddress: string | null;
  destinationAddress: string | null;
  refundAddress: string | null;
  otcSwapId: string | null;
  swapperSwapId: string | null;
  cowOrderUid: string | null;
  sellCurrency: Currency;
  buyCurrency: Currency;
  quoteMode: string;
  quotedSellAmount: U256;
  quotedBuyAmount: U256;
  quotedMinimumBuyAmount: U256;
  quotedMaximumSellAmount: U256 | null;
  executedSellAmount: U256 | null;
  executedBuyAmount: U256 | null;
  executedFeeAmount: U256 | null;
  depositTxHash: string | null;
  payoutTxHash: string | null;
  refundTxHash: string | null;
  rawRouterJson?: unknown;
  rawExternalDEXOrderJson?: unknown;
  rawOtcJson?: RawOtcJson | null;
  rawSwapperJson?: unknown;
  createdAt: string;
  updatedAt: string;
  lastSourceUpdateAt: string;
  terminalAt: string | null;
  orderType: string | null;
  externalDexStatus: unknown;
  teeOtcStatus: TeeStatus | null;
  teeSwapperStatus: TeeStatus | null;
};

export type ErrorResponse = {
  error?: string;
  message?: string;
};

export type Result<T, E = ErrorResponse> =
  | { ok: true; data: T }
  | { ok: false; error: E; status: number };

const RIFT_API_URL =
  process.env.NEXT_PUBLIC_RIFT_API_URL ?? "https://api.rift.trade";
const MAX_RIFT_API_RESPONSE_BYTES = 1024 * 1024;
const RIFT_API_TIMEOUT_MS = 10_000;

export async function getOrder(orderId: string): Promise<Result<Order>> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), RIFT_API_TIMEOUT_MS);
  try {
    const response = await fetch(`${RIFT_API_URL}/order/${orderId}`, {
      method: "GET",
      headers: { "Content-Type": "application/json" },
      signal: controller.signal,
    });

    const contentType = response.headers.get("content-type") ?? "";
    const body = await readLimitedResponseText(response, MAX_RIFT_API_RESPONSE_BYTES);
    if (body.truncated) {
      return {
        ok: false,
        error: { error: `Rift API response exceeded ${MAX_RIFT_API_RESPONSE_BYTES} bytes` },
        status: response.ok ? 502 : response.status,
      };
    }
    const data = contentType.includes("application/json")
      ? parseJsonBody(body.text)
      : { error: body.text };

    if (response.ok) {
      return { ok: true, data: data as Order };
    }

    return {
      ok: false,
      error: data as ErrorResponse,
      status: response.status,
    };
  } catch (error) {
    if (isAbortError(error)) {
      return {
        ok: false,
        error: { error: `Rift API request timed out after ${RIFT_API_TIMEOUT_MS}ms` },
        status: 504,
      };
    }
    return { ok: false, error: { error: String(error) }, status: 0 };
  } finally {
    clearTimeout(timeout);
  }
}

type LimitedResponseText = {
  text: string;
  truncated: boolean;
};

async function readLimitedResponseText(
  response: Response,
  maxBytes: number,
): Promise<LimitedResponseText> {
  if (!response.body) return { text: "", truncated: false };

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > maxBytes) {
        await reader.cancel().catch(() => undefined);
        return { text: "", truncated: true };
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  return {
    text: new TextDecoder().decode(concatChunks(chunks, totalBytes)),
    truncated: false,
  };
}

function concatChunks(chunks: Uint8Array[], totalBytes: number): Uint8Array {
  const output = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

function parseJsonBody(text: string): unknown {
  if (!text) return undefined;
  return JSON.parse(text) as unknown;
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}
