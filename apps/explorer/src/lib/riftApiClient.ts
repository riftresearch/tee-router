export type U256 = string;

export type SwapStatus =
  | "waiting_for_deposit"
  | "deposit_confirming"
  | "initiating_payout"
  | "confirming_payout"
  | "swap_complete"
  | "refunding_user"
  | "failed";

export type UserDepositStatus = {
  tx_hash: string;
  amount: U256;
  deposit_detected_at: string;
  confirmations: number;
  last_checked: string;
  confirmed_at?: string;
};

export type SettlementStatus = {
  tx_hash: string;
  broadcast_at: string;
  confirmations: number;
  completed_at?: string;
};

export type LatestRefund = {
  tx_hash: string;
  broadcast_at: string;
  confirmations: number;
  confirmed_at?: string;
};

export type SwapRates = {
  liquidity_fee_bps: number;
  protocol_fee_bps: number;
  network_fee_sats: number;
};

export type SwapFees = {
  liquidity_fee: U256;
  protocol_fee: U256;
  network_fee: U256;
};

export type LotCurrency = {
  chain: "bitcoin" | "ethereum" | "base" | "solana";
  token: { type: "Native" } | { type: "Address"; data: string };
  decimals: number;
};

export type Lot = {
  currency: LotCurrency;
  amount: U256;
};

export type SwapQuote = {
  id: string;
  market_maker_id: string;
  from: Lot;
  to: Lot;
  rates: SwapRates;
  fees: SwapFees;
  min_input: U256;
  max_input: U256;
  affiliate?: string;
  expires_at: string;
  created_at: string;
};

type MaybeEmpty<T> = T | Record<string, never>;

export type Swap = {
  id: string;
  market_maker_id: string;
  quote: SwapQuote;
  metadata: MaybeEmpty<{ start_asset?: string }>;
  deposit_vault_salt: string;
  deposit_vault_address: string;
  mm_nonce: string;
  user_destination_address: string;
  refund_address: string;
  status: SwapStatus;
  user_deposit_status?: MaybeEmpty<UserDepositStatus>;
  settlement_status?: MaybeEmpty<SettlementStatus>;
  latest_refund?: MaybeEmpty<LatestRefund>;
  failure_reason?: string | null;
  failure_at?: string | null;
  created_at: string;
  updated_at: string;
};

export type ErrorResponse = {
  error: string;
};

export type Result<T, E = ErrorResponse> =
  | { ok: true; data: T }
  | { ok: false; error: E; status: number };

export function hasData<T extends object>(
  obj: T | Record<string, never> | undefined
): obj is T {
  return obj !== undefined && Object.keys(obj).length > 0;
}

const RIFT_API_URL =
  process.env.NEXT_PUBLIC_RIFT_API_URL ??
  "https://router-gateway-production.up.railway.app";

export async function getSwap(swapId: string): Promise<Result<Swap>> {
  try {
    const response = await fetch(`${RIFT_API_URL}/order/${swapId}`, {
      method: "GET",
      headers: { "Content-Type": "application/json" },
    });

    const data = await response.json();

    if (response.ok) {
      return { ok: true, data: data as Swap };
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
