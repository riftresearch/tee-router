"use client";

import { useState } from "react";
import Image from "next/image";
import { formatUnits } from "viem";
import { getSwap, hasData, type Swap } from "@/lib/riftApiClient";

type DisplayStatus = "pending" | "completed" | "failed" | "unknown";

interface SwapDisplay {
  id: string;
  status: DisplayStatus;
  rawStatus: string;
  fromAmount: string;
  fromAsset: string;
  fromChain: string;
  toAmount: string;
  toAsset: string;
  toChain: string;
  createdAt: string;
  completedAt?: string;
  txHash?: string;
  depositAddress?: string;
  destinationAddress?: string;
}

function mapSwapStatus(status: Swap["status"]): DisplayStatus {
  switch (status) {
    case "swap_complete":
      return "completed";
    case "failed":
    case "refunding_user":
      return "failed";
    case "waiting_for_deposit":
    case "deposit_confirming":
    case "initiating_payout":
    case "confirming_payout":
      return "pending";
    default:
      return "unknown";
  }
}

function getAssetName(
  chain: string,
  token: { type: string; data?: string }
): string {
  if (token.type === "Native") {
    switch (chain) {
      case "bitcoin":
        return "BTC";
      case "ethereum":
        return "ETH";
      case "base":
        return "ETH";
      case "solana":
        return "SOL";
      default:
        return chain.toUpperCase();
    }
  }
  return token.data?.slice(0, 8) || "Token";
}

function formatAmount(amount: string, decimals: number): string {
  try {
    return formatUnits(BigInt(amount), decimals);
  } catch {
    return amount;
  }
}

function StatusIcon({ status }: { status: DisplayStatus }) {
  switch (status) {
    case "completed":
      return (
        <svg className="w-5 h-5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      );
    case "failed":
      return (
        <svg className="w-5 h-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      );
    case "pending":
      return (
        <svg className="w-5 h-5 text-yellow-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      );
    default:
      return (
        <svg className="w-5 h-5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      );
  }
}

function getStatusColor(status: DisplayStatus): string {
  switch (status) {
    case "completed":
      return "text-green-500";
    case "failed":
      return "text-red-500";
    case "pending":
      return "text-yellow-500";
    default:
      return "text-gray-500";
  }
}

export default function Explorer() {
  const [searchQuery, setSearchQuery] = useState("");
  const [isSearching, setIsSearching] = useState(false);
  const [swapDisplay, setSwapDisplay] = useState<SwapDisplay | null>(null);
  const [searchError, setSearchError] = useState<string | null>(null);

  const handleSearch = async () => {
    if (!searchQuery.trim()) return;

    setIsSearching(true);
    setSearchError(null);
    setSwapDisplay(null);

    try {
      const result = await getSwap(searchQuery.trim());

      if (result?.ok && result.data) {
        const swap = result.data;
        const quote = swap.quote;

        const fromCurrency = quote.from.currency;
        const toCurrency = quote.to.currency;

        setSwapDisplay({
          id: swap.id,
          status: mapSwapStatus(swap.status),
          rawStatus: swap.status.replace(/_/g, " "),
          fromAmount: formatAmount(quote.from.amount, fromCurrency.decimals),
          fromAsset: getAssetName(fromCurrency.chain, fromCurrency.token),
          fromChain: fromCurrency.chain,
          toAmount: formatAmount(quote.to.amount, toCurrency.decimals),
          toAsset: getAssetName(toCurrency.chain, toCurrency.token),
          toChain: toCurrency.chain,
          createdAt: swap.created_at,
          completedAt: hasData(swap.settlement_status)
            ? swap.settlement_status.completed_at
            : undefined,
          txHash: hasData(swap.settlement_status)
            ? swap.settlement_status.tx_hash
            : undefined,
          depositAddress: swap.deposit_vault_address,
          destinationAddress: swap.user_destination_address,
        });
      } else {
        setSearchError("Swap not found");
      }
    } catch (error) {
      console.error("[Explorer] Search error:", error);
      setSearchError("Failed to fetch swap details");
    } finally {
      setIsSearching(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      handleSearch();
    }
  };

  return (
    <div 
      className="w-screen h-screen flex flex-col overflow-hidden relative"
      style={{
        backgroundImage: "url('/images/rift_background_low.webp')",
        backgroundSize: "cover",
        backgroundPosition: "center",
        backgroundRepeat: "no-repeat",
      }}
    >

      {/* Header */}
      <header className="relative z-10 flex flex-col items-center pt-8 pb-4 md:pt-12 md:pb-6">
        <a href="https://rift.trade" className="mb-2 hover:opacity-80 transition-opacity">
          <Image
            src="/images/rift_logo.svg"
            alt="RIFT"
            width={156}
            height={56}
            className="h-12 w-auto md:h-14"
            style={{ width: "auto" }}
            priority
          />
        </a>
        <p className="font-aux-mono text-[10px] text-[#999999] tracking-tight">
          TRANSACTION EXPLORER
        </p>
      </header>

      {/* Main Content */}
      <main className="relative z-10 flex-1 flex items-start md:items-center justify-center px-4 pt-4 pb-20 md:pb-24 overflow-y-auto">
        <div className="w-full max-w-2xl flex flex-col gap-4">
          {/* Search Box */}
          <div className="glass-card rounded-2xl p-4 transition-all hover:border-[#444]">
            <div className="flex items-center gap-3">
              <svg
                className="w-5 h-5 text-gray-500 flex-shrink-0"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                />
              </svg>
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={handleKeyPress}
                placeholder="Search by Swap ID..."
                className="flex-1 bg-transparent text-white font-aux-mono text-sm placeholder:text-[#4b5563] focus:outline-none"
              />
              {isSearching ? (
                <div className="w-5 h-5 border-2 border-gray-500 border-t-transparent rounded-full animate-spin" />
              ) : (
                <button
                  onClick={handleSearch}
                  className="bg-[#e76d00] hover:bg-[#c55b19] text-white font-nostromo text-[11px] px-4 py-1.5 rounded-lg transition-colors tracking-wide"
                >
                  SEARCH
                </button>
              )}
            </div>
          </div>

          {/* Results Box */}
          <div className="glass-card rounded-2xl p-6 min-h-[320px] flex flex-col">
            {isSearching ? (
              <div className="flex-1 flex items-center justify-center">
                <div className="w-8 h-8 border-2 border-gray-500 border-t-transparent rounded-full animate-spin" />
              </div>
            ) : searchError ? (
              <div className="flex-1 flex flex-col items-center justify-center gap-3">
                <svg
                  className="w-10 h-10 text-red-500"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
                <p className="font-aux-mono text-sm text-gray-500">
                  {searchError}
                </p>
              </div>
            ) : swapDisplay ? (
              <div className="flex flex-col gap-5">
                {/* Status Header */}
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <StatusIcon status={swapDisplay.status} />
                    <span
                      className={`font-nostromo text-base uppercase ${getStatusColor(
                        swapDisplay.status
                      )}`}
                    >
                      {swapDisplay.rawStatus}
                    </span>
                  </div>
                  {swapDisplay.txHash && (
                    <a
                      href={`https://mempool.space/tx/${swapDisplay.txHash}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-1.5 text-gray-500 hover:text-gray-300 transition-colors"
                    >
                      <span className="font-aux-mono text-[11px]">
                        View on Mempool
                      </span>
                      <svg
                        className="w-3 h-3"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          strokeWidth={2}
                          d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
                        />
                      </svg>
                    </a>
                  )}
                </div>

                {/* Swap ID */}
                <div className="flex flex-col gap-1">
                  <span className="font-aux-mono text-[10px] text-gray-500 uppercase">
                    Swap ID
                  </span>
                  <span className="font-aux-mono text-xs text-white break-all">
                    {swapDisplay.id}
                  </span>
                </div>

                {/* From / To */}
                <div className="flex flex-col md:flex-row gap-5">
                  <div className="flex-1 flex flex-col gap-1">
                    <div className="flex items-center gap-1.5">
                      <span className="font-aux-mono text-[10px] text-gray-500 uppercase">
                        From
                      </span>
                      <span className="font-aux-mono text-[9px] text-[#4b5563] capitalize">
                        ({swapDisplay.fromChain})
                      </span>
                    </div>
                    <span className="text-2xl text-white tracking-tight">
                      {parseFloat(swapDisplay.fromAmount).toLocaleString(
                        undefined,
                        { maximumFractionDigits: 6 }
                      )}{" "}
                      {swapDisplay.fromAsset}
                    </span>
                  </div>
                  <div className="flex-1 flex flex-col gap-1">
                    <div className="flex items-center gap-1.5">
                      <span className="font-aux-mono text-[10px] text-gray-500 uppercase">
                        To
                      </span>
                      <span className="font-aux-mono text-[9px] text-[#4b5563] capitalize">
                        ({swapDisplay.toChain})
                      </span>
                    </div>
                    <span
                      className={`text-2xl tracking-tight ${
                        swapDisplay.toAsset === "BTC"
                          ? "text-[#f7931a]"
                          : "text-white"
                      }`}
                    >
                      {parseFloat(swapDisplay.toAmount).toLocaleString(
                        undefined,
                        { maximumFractionDigits: 8 }
                      )}{" "}
                      {swapDisplay.toAsset}
                    </span>
                  </div>
                </div>

                {/* Deposit Address */}
                {swapDisplay.depositAddress && (
                  <div className="flex flex-col gap-1">
                    <span className="font-aux-mono text-[10px] text-gray-500 uppercase">
                      Deposit Address
                    </span>
                    <span className="font-aux-mono text-[11px] text-[#9ca3af] break-all">
                      {swapDisplay.depositAddress}
                    </span>
                  </div>
                )}

                {/* Destination Address */}
                {swapDisplay.destinationAddress && (
                  <div className="flex flex-col gap-1">
                    <span className="font-aux-mono text-[10px] text-gray-500 uppercase">
                      Destination Address
                    </span>
                    <span className="font-aux-mono text-[11px] text-[#9ca3af] break-all">
                      {swapDisplay.destinationAddress}
                    </span>
                  </div>
                )}

                {/* Timestamps */}
                <div className="flex flex-col md:flex-row gap-5">
                  <div className="flex-1 flex flex-col gap-1">
                    <span className="font-aux-mono text-[10px] text-gray-500 uppercase">
                      Created
                    </span>
                    <span className="font-aux-mono text-xs text-white">
                      {new Date(swapDisplay.createdAt).toLocaleString()}
                    </span>
                  </div>
                  {swapDisplay.completedAt && (
                    <div className="flex-1 flex flex-col gap-1">
                      <span className="font-aux-mono text-[10px] text-gray-500 uppercase">
                        Completed
                      </span>
                      <span className="font-aux-mono text-xs text-green-500">
                        {new Date(swapDisplay.completedAt).toLocaleString()}
                      </span>
                    </div>
                  )}
                </div>
              </div>
            ) : (
              <div className="flex-1 flex flex-col items-center justify-center gap-3">
                <svg
                  className="w-10 h-10 text-[#4b5563]"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                  />
                </svg>
                <p className="font-aux-mono text-sm text-[#4b5563] text-center">
                  Enter a Swap ID to view transaction details
                </p>
              </div>
            )}
          </div>
        </div>
      </main>

      {/* Footer */}
      <footer className="relative z-10 w-full py-3 px-6 flex justify-between items-end md:fixed md:bottom-0">
        <div className="flex flex-col gap-0.5">
          <span
            className="font-aux-mono text-[10px] text-[#6b7280] animate-typewriter-left"
            style={{ animationDelay: "0.1s" }}
          >
            {"///"}
          </span>
          <a
            href="https://x.com/riftdex"
            target="_blank"
            rel="noopener noreferrer"
            className="font-aux-mono text-[10px] text-[#9ca3af] hover:text-[#d1d5db] transition-colors animate-typewriter-left"
            style={{ animationDelay: "0.3s" }}
          >
            @RIFTDEX {">"}
          </a>
          <span
            className="font-aux-mono text-[10px] text-[#6b7280] animate-typewriter-left"
            style={{ animationDelay: "0.5s" }}
          >
            BACKED BY ◈ PARADIGM,
          </span>
          <span
            className="font-aux-mono text-[10px] text-[#6b7280] animate-typewriter-left"
            style={{ animationDelay: "0.7s" }}
          >
            WTG VENTURES, EDGE CAPITAL
          </span>
        </div>

        <div className="hidden md:flex flex-col items-end gap-0.5">
          <span
            className="font-aux-mono text-[10px] text-[#6b7280] animate-typewriter-right"
            style={{ animationDelay: "0.1s" }}
          >
            {"///"}
          </span>
          <a
            href="https://github.com/rift-labs-inc"
            target="_blank"
            rel="noopener noreferrer"
            className="font-aux-mono text-[10px] text-[#9ca3af] hover:text-[#d1d5db] transition-colors animate-typewriter-right"
            style={{ animationDelay: "0.3s" }}
          >
            GITHUB {">"}
          </a>
          <a
            href="https://rift.trade"
            className="font-aux-mono text-[10px] text-[#9ca3af] hover:text-[#d1d5db] transition-colors animate-typewriter-right"
            style={{ animationDelay: "0.5s" }}
          >
            BACK TO RIFT {">"}
          </a>
          <span
            className="font-aux-mono text-[10px] text-[#6b7280] animate-typewriter-right"
            style={{ animationDelay: "0.7s" }}
          >
            TRANSACTION EXPLORER
          </span>
        </div>
      </footer>
    </div>
  );
}
