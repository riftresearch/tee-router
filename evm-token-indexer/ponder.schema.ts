import { index, onchainTable } from "ponder";

export const erc20TransferRaw = onchainTable(
  "erc20_transfer_raw",
  (t) => ({
    id: t.text().primaryKey(),
    chainId: t.integer().notNull(),
    tokenAddress: t.hex().notNull(),
    fromAddress: t.hex().notNull(),
    toAddress: t.hex().notNull(),
    amount: t.bigint().notNull(),
    blockNumber: t.bigint().notNull(),
    blockHash: t.hex().notNull(),
    transactionHash: t.hex().notNull(),
    logIndex: t.integer().notNull(),
    blockTimestamp: t.bigint().notNull(),
  }),
  (table) => ({
    transferUniqIdx: index("erc20_transfer_raw_tx_log_idx").on(
      table.chainId,
      table.transactionHash,
      table.logIndex,
    ),
    toTokenBlockIdx: index("erc20_transfer_raw_to_token_block_idx").on(
      table.chainId,
      table.toAddress,
      table.tokenAddress,
      table.blockNumber,
    ),
    blockIdx: index("erc20_transfer_raw_block_idx").on(
      table.chainId,
      table.blockNumber,
    ),
  }),
);

export const activeDepositWatch = onchainTable(
  "active_deposit_watch",
  (t) => ({
    watchId: t.text().primaryKey(),
    watchTarget: t.text().notNull(),
    chainId: t.integer().notNull(),
    tokenAddress: t.hex().notNull(),
    depositAddress: t.hex().notNull(),
    minAmount: t.bigint().notNull(),
    maxAmount: t.bigint().notNull(),
    requiredAmount: t.bigint().notNull(),
    createdAt: t.bigint().notNull(),
    updatedAt: t.bigint().notNull(),
    expiresAt: t.bigint().notNull(),
  }),
  (table) => ({
    watchMatchIdx: index("active_deposit_watch_match_idx").on(
      table.chainId,
      table.depositAddress,
      table.tokenAddress,
    ),
    watchExpiryIdx: index("active_deposit_watch_expiry_idx").on(
      table.chainId,
      table.expiresAt,
    ),
  }),
);

export const detectedDepositCandidate = onchainTable(
  "detected_deposit_candidate",
  (t) => ({
    id: t.text().primaryKey(),
    watchId: t.text().notNull(),
    watchTarget: t.text().notNull(),
    chainId: t.integer().notNull(),
    tokenAddress: t.hex().notNull(),
    fromAddress: t.hex().notNull(),
    depositAddress: t.hex().notNull(),
    amount: t.bigint().notNull(),
    requiredAmount: t.bigint().notNull(),
    transactionHash: t.hex().notNull(),
    transferIndex: t.integer().notNull(),
    blockNumber: t.bigint().notNull(),
    blockHash: t.hex().notNull(),
    blockTimestamp: t.bigint().notNull(),
    status: t.text().notNull(),
    attemptCount: t.integer().notNull(),
    lastError: t.text(),
    createdAt: t.bigint().notNull(),
    deliveredAt: t.bigint(),
  }),
  (table) => ({
    pendingIdx: index("detected_deposit_candidate_pending_idx").on(
      table.status,
      table.chainId,
      table.blockNumber,
    ),
    watchIdx: index("detected_deposit_candidate_watch_idx").on(table.watchId),
  }),
);
