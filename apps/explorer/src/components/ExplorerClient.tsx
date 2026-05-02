"use client";

import Image from "next/image";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { formatUnits } from "viem";
import { getOrder, type Currency, type Order } from "@/lib/riftApiClient";

type DisplayStatus = "pending" | "completed" | "failed" | "unknown";

type AssetMeta = {
  ticker?: string;
  amount?: string;
  decimals?: number;
};

type OrderDisplay = {
  id: string;
  status: DisplayStatus;
  rawStatus: string;
  routeType: string;
  provider: string;
  path: string;
  quoteMode: string;
  sellAmount: string;
  sellAsset: string;
  sellChain: string;
  buyAmount: string;
  buyAsset: string;
  buyChain: string;
  minBuyAmount: string;
  destinationAddress?: string;
  refundAddress?: string;
  senderAddress?: string;
  depositTxHash?: string;
  payoutTxHash?: string;
  refundTxHash?: string;
  createdAt: string;
  updatedAt: string;
  terminalAt?: string;
  otcStatus?: string;
  swapperStatus?: string;
  integrator?: string;
};

function prettify(value: string | null | undefined): string {
  if (!value) return "UNKNOWN";
  return value.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function mapOrderStatus(order: Order): DisplayStatus {
  const status = [
    order.status,
    order.teeOtcStatus?.status,
    order.teeSwapperStatus?.status,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (status.match(/complete|completed|settled|filled|success/)) return "completed";
  if (status.match(/fail|failed|refund|cancel|expired/)) return "failed";
  if (status.match(/unfilled|waiting|deposit|confirm|initiating|pending|created/)) {
    return "pending";
  }

  return "unknown";
}

function chainName(currency: Currency): string {
  if (currency.chain.kind === "BITCOIN") return "Bitcoin";
  if (currency.chain.kind === "EVM") {
    switch (currency.chain.chainId) {
      case 1:
        return "Ethereum";
      case 8453:
        return "Base";
      case 42161:
        return "Arbitrum";
      default:
        return `EVM ${currency.chain.chainId}`;
    }
  }
  return prettify(currency.chain.kind);
}

function tokenName(currency: Currency, meta?: AssetMeta): string {
  if (meta?.ticker) return meta.ticker;
  if (currency.token.kind === "NATIVE") {
    if (currency.chain.kind === "BITCOIN") return "BTC";
    if (currency.chain.kind === "EVM") return "ETH";
    return "Native";
  }
  if (currency.token.kind === "TOKEN") {
    const knownTokens: Record<string, string> = {
      "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": "cbBTC",
    };
    const address =
      typeof currency.token.address === "string"
        ? currency.token.address.toLowerCase()
        : "";
    return knownTokens[address] ?? "Token";
  }
  return "Asset";
}

function decimalsFor(currency: Currency, meta?: AssetMeta): number {
  return meta?.decimals ?? currency.token.decimals ?? 0;
}

function formatAmount(amount: string | null | undefined, decimals: number): string {
  if (!amount) return "Pending";
  try {
    const formatted = formatUnits(BigInt(amount), decimals);
    const [whole, fraction] = formatted.split(".");
    if (!fraction) return Number(whole).toLocaleString();
    const trimmed = fraction.replace(/0+$/, "").slice(0, 8);
    return `${Number(whole).toLocaleString()}${trimmed ? `.${trimmed}` : ""}`;
  } catch {
    return amount;
  }
}

function parseAssetMeta(value: string | undefined): AssetMeta | undefined {
  if (!value) return undefined;
  try {
    return JSON.parse(value) as AssetMeta;
  } catch {
    return undefined;
  }
}

function dateLabel(value: string | null | undefined): string {
  if (!value) return "Pending";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function txUrl(hash: string, kind: "deposit" | "payout" | "refund"): string {
  if (kind === "deposit" || kind === "refund") {
    return `https://mempool.space/tx/${hash}`;
  }
  return `https://basescan.org/tx/${hash}`;
}

function toOrderDisplay(order: Order): OrderDisplay {
  const startMeta = parseAssetMeta(order.rawOtcJson?.metadata?.start_asset);
  const endMeta = parseAssetMeta(order.rawOtcJson?.metadata?.end_asset);
  const sellDecimals = decimalsFor(order.sellCurrency, startMeta);
  const buyDecimals = decimalsFor(order.buyCurrency, endMeta);
  const rawStatus =
    order.teeOtcStatus?.status ??
    order.teeSwapperStatus?.status ??
    order.status;

  return {
    id: order.orderId,
    status: mapOrderStatus(order),
    rawStatus: prettify(rawStatus),
    routeType: prettify(order.routeType),
    provider: order.provider ?? "RIFT",
    path: order.path.length ? order.path.map(prettify).join(" -> ") : "Direct",
    quoteMode: prettify(order.quoteMode),
    sellAmount: formatAmount(
      order.executedSellAmount ?? order.quotedSellAmount,
      sellDecimals
    ),
    sellAsset: tokenName(order.sellCurrency, startMeta),
    sellChain: chainName(order.sellCurrency),
    buyAmount: formatAmount(order.executedBuyAmount ?? order.quotedBuyAmount, buyDecimals),
    buyAsset: tokenName(order.buyCurrency, endMeta),
    buyChain: chainName(order.buyCurrency),
    minBuyAmount: formatAmount(order.quotedMinimumBuyAmount, buyDecimals),
    destinationAddress: order.destinationAddress ?? undefined,
    refundAddress: order.refundAddress ?? undefined,
    senderAddress: order.senderAddress ?? undefined,
    depositTxHash: order.depositTxHash ?? undefined,
    payoutTxHash: order.payoutTxHash ?? undefined,
    refundTxHash: order.refundTxHash ?? undefined,
    createdAt: order.createdAt,
    updatedAt: order.updatedAt,
    terminalAt: order.terminalAt ?? undefined,
    otcStatus: order.teeOtcStatus?.status,
    swapperStatus: order.teeSwapperStatus?.status,
    integrator: order.rawOtcJson?.metadata?.integrator_name,
  };
}

function StatusIcon({ status }: { status: DisplayStatus }) {
  switch (status) {
    case "completed":
      return (
        <svg className="h-5 w-5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      );
    case "failed":
      return (
        <svg className="h-5 w-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      );
    case "pending":
      return (
        <svg className="h-5 w-5 text-yellow-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      );
    default:
      return (
        <svg className="h-5 w-5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8.228 9c.549-1.165 1.924-2 3.522-2 2.071 0 3.75 1.343 3.75 3 0 1.134-.786 2.12-1.946 2.63-.818.36-1.554 1.03-1.554 1.924V15m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      );
  }
}

function statusColor(status: DisplayStatus): string {
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

function assetIcon(asset: string): string {
  switch (asset.toLowerCase()) {
    case "btc":
      return "/images/BTC_icon.svg";
    case "cbbtc":
      return "/images/cbBTC_icon.svg";
    default:
      return "/images/cbBTC_icon.svg";
  }
}

function chainIcon(chain: string): string | undefined {
  if (chain.toLowerCase() === "base") return "/images/base_logo.svg";
  return undefined;
}

function AssetPill({ asset, chain }: { asset: string; chain: string }) {
  const networkIcon = chainIcon(chain);

  return (
    <span className="inline-flex items-center gap-2 rounded-md border border-[#3a3e47] bg-black/20 px-2.5 py-1 font-aux-mono text-xs uppercase tracking-[0.08em] text-[#d8dde6]">
      <Image src={assetIcon(asset)} alt="" width={18} height={18} className="h-[18px] w-[18px]" />
      {asset}
      {networkIcon && (
        <Image src={networkIcon} alt="" width={14} height={14} className="h-[14px] w-[14px] rounded-full" />
      )}
    </span>
  );
}

function AssetAmount({
  label,
  amount,
  asset,
  chain,
  align = "left",
  tone = "text-white",
}: {
  label: string;
  amount: string;
  asset: string;
  chain: string;
  align?: "left" | "right";
  tone?: string;
}) {
  return (
    <div className={`flex flex-col gap-3 ${align === "right" ? "md:items-end" : ""}`}>
      <span className="font-aux-mono text-[11px] uppercase tracking-[0.16em] text-[#777f8d]">
        {label}
      </span>
      <div className={`flex flex-wrap items-baseline gap-x-3 gap-y-2 ${align === "right" ? "md:justify-end" : ""}`}>
        <span className={`font-aux-mono text-3xl leading-none tracking-normal md:text-4xl ${tone}`}>
          {amount}
        </span>
        <AssetPill asset={asset} chain={chain} />
      </div>
      <span className="font-aux-mono text-xs uppercase tracking-[0.12em] text-[#69717e]">
        {chain}
      </span>
    </div>
  );
}

function Detail({ label, value }: { label: string; value?: string | null }) {
  if (!value) return null;
  return (
    <div className="flex min-w-0 flex-col gap-2 border-l border-[#3a3e47] pl-4">
      <span className="font-aux-mono text-[10px] uppercase tracking-[0.14em] text-[#777f8d]">
        {label}
      </span>
      <span className="break-all font-aux-mono text-sm leading-6 text-[#d8dde6]">
        {value}
      </span>
    </div>
  );
}

function Metric({
  label,
  value,
  tone = "text-white",
}: {
  label: string;
  value: string;
  tone?: string;
}) {
  return (
    <div className="flex flex-col gap-2 border-l border-[#3a3e47] pl-4">
      <span className="font-aux-mono text-[10px] uppercase tracking-[0.14em] text-[#777f8d]">
        {label}
      </span>
      <span className={`font-aux-mono text-base leading-6 ${tone}`}>{value}</span>
    </div>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="flex flex-col gap-4">
      <div className="flex items-center gap-3">
        <h2 className="font-aux-mono text-[11px] uppercase tracking-[0.18em] text-[#8e98a8]">
          {title}
        </h2>
        <div className="h-px flex-1 bg-[#30333a]" />
      </div>
      {children}
    </section>
  );
}

export default function ExplorerClient({
  initialOrderId,
}: {
  initialOrderId?: string;
}) {
  const router = useRouter();
  const [searchQuery, setSearchQuery] = useState(initialOrderId ?? "");
  const [isSearching, setIsSearching] = useState(Boolean(initialOrderId));
  const [orderDisplay, setOrderDisplay] = useState<OrderDisplay | null>(null);
  const [searchError, setSearchError] = useState<string | null>(null);

  useEffect(() => {
    if (!initialOrderId) return;

    const orderId = initialOrderId;
    let isActive = true;

    async function loadOrder() {
      try {
        const result = await getOrder(orderId);

        if (!isActive) return;

        if (result.ok) {
          setOrderDisplay(toOrderDisplay(result.data));
        } else if (result.status === 404) {
          setSearchError("Order not found");
        } else {
          setSearchError(
            result.error.error ?? result.error.message ?? "Failed to fetch order"
          );
        }
      } catch (error) {
        if (!isActive) return;
        console.error("[Explorer] Search error:", error);
        setSearchError("Failed to fetch order details");
      } finally {
        if (isActive) {
          setIsSearching(false);
        }
      }
    }

    void loadOrder();

    return () => {
      isActive = false;
    };
  }, [initialOrderId]);

  const handleSearch = () => {
    const trimmedQuery = searchQuery.trim();
    if (!trimmedQuery) return;

    router.push(`/order/${encodeURIComponent(trimmedQuery)}`);
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      handleSearch();
    }
  };

  return (
    <div
      className="relative flex h-screen w-screen flex-col overflow-hidden"
      style={{
        backgroundImage: "url('/images/rift_background_low.webp')",
        backgroundPosition: "center",
        backgroundRepeat: "no-repeat",
        backgroundSize: "cover",
      }}
    >
      <div className="relative z-10 mx-auto flex min-h-0 w-full max-w-[1600px] flex-1 flex-col px-4 py-4 md:w-[70vw] md:px-0 md:py-6">
        <header className="flex shrink-0 items-center gap-4 pb-4">
          <a
            href="https://rift.trade"
            className="flex h-11 w-28 shrink-0 items-center transition-opacity hover:opacity-80 md:h-12 md:w-32"
            aria-label="RIFT"
          >
            <Image
              src="/images/rift_logo.svg"
              alt="RIFT"
              width={128}
              height={27}
              className="h-auto w-full"
              priority
            />
          </a>

          <div className="glass-card flex min-w-0 flex-1 items-center gap-3 rounded-xl px-4 py-3 transition-all hover:border-[#444]">
              <svg className="h-5 w-5 flex-shrink-0 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={handleKeyPress}
                placeholder="Search by Order ID..."
                className="min-w-0 flex-1 bg-transparent font-aux-mono text-sm text-white placeholder:text-[#4b5563] focus:outline-none"
              />
              {isSearching ? (
                <div className="h-5 w-5 rounded-full border-2 border-gray-500 border-t-transparent animate-spin" />
              ) : (
                <button
                  onClick={handleSearch}
                  className="rounded-lg bg-[#e76d00] px-[18px] py-[7px] font-nostromo text-[13px] tracking-wide text-white transition-colors hover:bg-[#c55b19]"
                >
                  SEARCH
                </button>
              )}
          </div>
        </header>

        <main className="flex min-h-0 flex-1">
          <div className="glass-card flex min-h-0 w-full flex-1 flex-col overflow-y-auto rounded-2xl p-6 md:p-8">
            {isSearching ? (
              <div className="flex flex-1 items-center justify-center">
                <div className="h-8 w-8 rounded-full border-2 border-gray-500 border-t-transparent animate-spin" />
              </div>
            ) : searchError ? (
              <div className="flex flex-1 flex-col items-center justify-center gap-3">
                <svg className="h-10 w-10 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <p className="font-aux-mono text-sm text-gray-500">{searchError}</p>
              </div>
            ) : orderDisplay ? (
              <div className="flex min-h-full flex-col gap-7">
                <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                  <div className="flex items-center gap-3">
                    <StatusIcon status={orderDisplay.status} />
                    <span className={`font-aux-mono text-2xl uppercase tracking-[0.08em] ${statusColor(orderDisplay.status)}`}>
                      {orderDisplay.rawStatus}
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-2 font-aux-mono text-[11px] uppercase tracking-[0.12em] text-[#a8b0bd]">
                    <span className="rounded-md border border-[#3a3e47] bg-black/20 px-3 py-1.5">
                      {orderDisplay.routeType}
                    </span>
                    <span className="rounded-md border border-[#3a3e47] bg-black/20 px-3 py-1.5">
                      {orderDisplay.path}
                    </span>
                    <span className="rounded-md border border-[#3a3e47] bg-black/20 px-3 py-1.5">
                      {orderDisplay.quoteMode}
                    </span>
                  </div>
                </div>

                <Section title="Order ID">
                  <p className="break-all border-l border-[#3a3e47] pl-4 font-aux-mono text-sm leading-6 text-white md:text-base">
                    {orderDisplay.id}
                  </p>
                </Section>

                <Section title="Route">
                  <div className="grid gap-6 md:grid-cols-[1fr_auto_1fr] md:items-center">
                    <AssetAmount
                      label="Sell"
                      amount={orderDisplay.sellAmount}
                      asset={orderDisplay.sellAsset}
                      chain={orderDisplay.sellChain}
                    />
                    <div className="hidden h-px w-12 bg-[#545862] md:block" />
                    <AssetAmount
                      label="Buy"
                      amount={orderDisplay.buyAmount}
                      asset={orderDisplay.buyAsset}
                      chain={orderDisplay.buyChain}
                      align="right"
                      tone="text-[#f7931a]"
                    />
                  </div>
                </Section>

                <Section title="Execution">
                  <div className="grid gap-5 md:grid-cols-3">
                    <Metric label="Minimum Buy" value={`${orderDisplay.minBuyAmount} ${orderDisplay.buyAsset}`} />
                    <Metric label="Provider" value={orderDisplay.provider} />
                    <Metric label="Integrator" value={orderDisplay.integrator ?? "Unknown"} />
                  </div>
                </Section>

                <Section title="Addresses">
                  <div className="grid gap-5 md:grid-cols-2">
                    <Detail label="Destination Address" value={orderDisplay.destinationAddress} />
                    <Detail label="Refund Address" value={orderDisplay.refundAddress} />
                    <Detail label="Sender Address" value={orderDisplay.senderAddress} />
                  </div>
                </Section>

                <Section title="Status Timeline">
                  <div className="grid gap-5 md:grid-cols-3">
                    <Metric label="Created" value={dateLabel(orderDisplay.createdAt)} />
                    <Metric label="Updated" value={dateLabel(orderDisplay.updatedAt)} />
                    <Metric label="Terminal" value={dateLabel(orderDisplay.terminalAt)} />
                  </div>
                  <div className="grid gap-5 md:grid-cols-2">
                    <Detail label="TEE OTC Status" value={orderDisplay.otcStatus ? prettify(orderDisplay.otcStatus) : undefined} />
                    <Detail label="TEE Swapper Status" value={orderDisplay.swapperStatus ? prettify(orderDisplay.swapperStatus) : undefined} />
                  </div>
                </Section>

                {(orderDisplay.depositTxHash || orderDisplay.payoutTxHash || orderDisplay.refundTxHash) && (
                  <Section title="Transactions">
                    <div className="flex flex-wrap gap-3">
                      {orderDisplay.depositTxHash && (
                        <a className="rounded-md border border-[#343944] bg-black/25 px-4 py-3 font-aux-mono text-sm text-[#d8dde6] transition-colors hover:border-[#e76d00] hover:text-white" href={txUrl(orderDisplay.depositTxHash, "deposit")} target="_blank" rel="noopener noreferrer">
                          Deposit Tx {">"}
                        </a>
                      )}
                      {orderDisplay.payoutTxHash && (
                        <a className="rounded-md border border-[#343944] bg-black/25 px-4 py-3 font-aux-mono text-sm text-[#d8dde6] transition-colors hover:border-[#e76d00] hover:text-white" href={txUrl(orderDisplay.payoutTxHash, "payout")} target="_blank" rel="noopener noreferrer">
                          Payout Tx {">"}
                        </a>
                      )}
                      {orderDisplay.refundTxHash && (
                        <a className="rounded-md border border-[#343944] bg-black/25 px-4 py-3 font-aux-mono text-sm text-[#d8dde6] transition-colors hover:border-[#e76d00] hover:text-white" href={txUrl(orderDisplay.refundTxHash, "refund")} target="_blank" rel="noopener noreferrer">
                          Refund Tx {">"}
                        </a>
                      )}
                    </div>
                  </Section>
                )}
              </div>
            ) : (
              <div className="flex flex-1 flex-col items-center justify-center gap-3">
                <svg className="h-10 w-10 text-[#4b5563]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
                <p className="text-center font-aux-mono text-sm text-[#4b5563]">
                  Enter an Order ID to view route status
                </p>
              </div>
            )}
          </div>
        </main>
      </div>

    </div>
  );
}
