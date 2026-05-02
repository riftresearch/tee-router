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

export async function getOrder(orderId: string): Promise<Result<Order>> {
  try {
    const response = await fetch(`${RIFT_API_URL}/order/${orderId}`, {
      method: "GET",
      headers: { "Content-Type": "application/json" },
    });

    const contentType = response.headers.get("content-type") ?? "";
    const data = contentType.includes("application/json")
      ? await response.json()
      : { error: await response.text() };

    if (response.ok) {
      return { ok: true, data: data as Order };
    }

    return {
      ok: false,
      error: data as ErrorResponse,
      status: response.status,
    };
  } catch (error) {
    return { ok: false, error: { error: String(error) }, status: 0 };
  }
}
