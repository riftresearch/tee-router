import { ponder } from "ponder:registry";
import pg from "pg";
import { MultiSource, type Source } from "./multisource";

const schemaName = process.env.DATABASE_SCHEMA ?? process.env.PONDER_SCHEMA;
const databaseUrl = process.env.DATABASE_URL;

if (!schemaName) {
  throw new Error("DATABASE_SCHEMA or PONDER_SCHEMA must be set for candidate materialization");
}

if (!databaseUrl) {
  throw new Error("DATABASE_URL must be set for candidate materialization");
}

const parsePositiveInteger = (
  value: string | undefined,
  name: string,
  defaultValue: number | undefined,
  max: number,
) => {
  const raw = value ?? defaultValue?.toString();
  if (raw === undefined || !/^\d+$/.test(raw)) {
    throw new Error(`${name} must be a positive integer`);
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > max) {
    throw new Error(`${name} must be a positive integer <= ${max}`);
  }
  return parsed;
};

const validatedChainId = parsePositiveInteger(
  process.env.PONDER_CHAIN_ID,
  "PONDER_CHAIN_ID",
  undefined,
  Number.MAX_SAFE_INTEGER,
);

const pool = new pg.Pool({
  connectionString: databaseUrl,
  application_name: "evm-token-indexer-indexing",
  max: parsePositiveInteger(
    process.env.EVM_TOKEN_INDEXER_INDEXING_POOL_SIZE,
    "EVM_TOKEN_INDEXER_INDEXING_POOL_SIZE",
    5,
    100,
  ),
});

const normalizeAddress = (address: string) =>
  address.toLowerCase() as `0x${string}`;

const parseHexAddress = (value: unknown, field: string) => {
  if (typeof value !== "string" || !/^0x[0-9a-fA-F]{40}$/.test(value)) {
    throw new Error(`${field} must be a 20-byte hex address`);
  }
  return normalizeAddress(value);
};

const quoteIdentifier = (value: string) => {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error(`Invalid SQL identifier: ${value}`);
  }
  return `"${value}"`;
};

const tableName = (table: string) =>
  `${quoteIdentifier(schemaName)}.${quoteIdentifier(table)}`;

const activeWatchTable = tableName("active_deposit_watch");
const candidateTable = tableName("detected_deposit_candidate");
const rawTransferTable = tableName("erc20_transfer_raw");
const nowSeconds = () => Math.floor(Date.now() / 1000).toString();
const transferNotifyChannel = `evm_token_indexer_transfer_${validatedChainId}`;
const TRANSFER_EVENT_TOPIC =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const TRANSFER_POLL_INTERVAL_MS = parsePositiveInteger(
  process.env.EVM_TOKEN_INDEXER_TRANSFER_POLL_INTERVAL_MS,
  "EVM_TOKEN_INDEXER_TRANSFER_POLL_INTERVAL_MS",
  60 * 60 * 1000,
  24 * 60 * 60 * 1000,
);
const TRANSFER_POLL_RECENT_BLOCKS = parsePositiveInteger(
  process.env.EVM_TOKEN_INDEXER_TRANSFER_POLL_RECENT_BLOCKS,
  "EVM_TOKEN_INDEXER_TRANSFER_POLL_RECENT_BLOCKS",
  7_200,
  1_000_000,
);
const TRANSFER_POLL_MAX_BLOCK_SPAN = parsePositiveInteger(
  process.env.EVM_TOKEN_INDEXER_TRANSFER_POLL_MAX_BLOCK_SPAN,
  "EVM_TOKEN_INDEXER_TRANSFER_POLL_MAX_BLOCK_SPAN",
  1_000,
  100_000,
);
const TRANSFER_POLL_FILTER_CHUNK_SIZE = parsePositiveInteger(
  process.env.EVM_TOKEN_INDEXER_TRANSFER_POLL_FILTER_CHUNK_SIZE,
  "EVM_TOKEN_INDEXER_TRANSFER_POLL_FILTER_CHUNK_SIZE",
  200,
  1_000,
);
const TRANSFER_MULTISOURCE_MAX_SEEN = parsePositiveInteger(
  process.env.EVM_TOKEN_INDEXER_TRANSFER_MULTISOURCE_MAX_SEEN,
  "EVM_TOKEN_INDEXER_TRANSFER_MULTISOURCE_MAX_SEEN",
  100_000,
  5_000_000,
);
const TRANSFER_MULTISOURCE_SEEN_TTL_MS = parsePositiveInteger(
  process.env.EVM_TOKEN_INDEXER_TRANSFER_MULTISOURCE_SEEN_TTL_MS,
  "EVM_TOKEN_INDEXER_TRANSFER_MULTISOURCE_SEEN_TTL_MS",
  30 * 60 * 1000,
  24 * 60 * 60 * 1000,
);

type TransferNotification = {
  id: string;
  chainId: number;
  tokenAddress: `0x${string}`;
  fromAddress: `0x${string}`;
  toAddress: `0x${string}`;
  amount: string;
  transactionHash: `0x${string}`;
  blockNumber: string;
  blockHash: `0x${string}`;
  logIndex: number;
  blockTimestamp: string;
};

type RpcTransferLog = {
  address?: string;
  topics?: string[];
  data?: string;
  blockNumber?: string;
  blockHash?: string;
  transactionHash?: string;
  logIndex?: string;
  removed?: boolean;
};

type RpcBlock = {
  timestamp?: string;
};

const publishTransfer = async (transfer: TransferNotification) => {
  await pool.query("SELECT pg_notify($1, $2)", [
    transferNotifyChannel,
    JSON.stringify(transfer),
  ]);
};

const transferMultiSource = new MultiSource<TransferNotification, string>({
  sources: [
    {
      name: "ponder-push",
      start: () => undefined,
    },
    recentTransferPollSource(),
  ],
  dedupKey: transferDedupKey,
  maxSeen: TRANSFER_MULTISOURCE_MAX_SEEN,
  seenTtlMs: TRANSFER_MULTISOURCE_SEEN_TTL_MS,
});

transferMultiSource.onEvent(async (transfer, sourceName) => {
  await persistTransfer(transfer, sourceName);
});

void transferMultiSource.start().catch((error) => {
  console.warn("failed to start transfer multisource", error);
});

ponder.on("erc20:Transfer", async ({ event }) => {
  const tokenAddress = normalizeAddress(event.log.address);
  const fromAddress = normalizeAddress(event.args.from);
  const toAddress = normalizeAddress(event.args.to);
  const amount = event.args.amount;
  const blockNumber = BigInt(event.block.number);
  const blockTimestamp = BigInt(event.block.timestamp);
  const transferIndex = Number(event.log.logIndex);
  const transactionHash = event.transaction.hash;
  const blockHash = event.block.hash;

  await transferMultiSource.emitFrom("ponder-push", {
    id: transferDedupKey({ transactionHash, logIndex: transferIndex }),
    chainId: validatedChainId,
    tokenAddress,
    fromAddress,
    toAddress,
    amount: amount.toString(),
    transactionHash,
    blockNumber: blockNumber.toString(),
    blockHash,
    logIndex: transferIndex,
    blockTimestamp: blockTimestamp.toString(),
  });
});

async function persistTransfer(transfer: TransferNotification, sourceName: string) {
  const inserted = await insertRawTransfer(transfer);
  await materializeCandidateForTransfer(transfer);

  if (inserted) {
    await publishTransfer(transfer).catch((error) => {
      console.warn(`failed to publish transfer notification from ${sourceName}`, error);
    });
  }
}

async function insertRawTransfer(transfer: TransferNotification) {
  const result = await pool.query(
    `
    INSERT INTO ${rawTransferTable} (
      id,
      chain_id,
      token_address,
      from_address,
      to_address,
      amount,
      block_number,
      block_hash,
      transaction_hash,
      log_index,
      block_timestamp
    )
    VALUES (
      $1,
      $2,
      $3,
      $4,
      $5,
      $6::numeric,
      $7::numeric,
      $8,
      $9,
      $10,
      $11::numeric
    )
    ON CONFLICT (id) DO NOTHING
    `,
    [
      transfer.id,
      transfer.chainId,
      transfer.tokenAddress,
      transfer.fromAddress,
      transfer.toAddress,
      transfer.amount,
      transfer.blockNumber,
      transfer.blockHash,
      transfer.transactionHash,
      transfer.logIndex,
      transfer.blockTimestamp,
    ],
  );
  return (result.rowCount ?? 0) > 0;
}

async function materializeCandidateForTransfer(transfer: TransferNotification) {
  await pool.query(
    `
    INSERT INTO ${candidateTable} (
      id,
      watch_id,
      watch_target,
      chain_id,
      token_address,
      from_address,
      deposit_address,
      amount,
      required_amount,
      transaction_hash,
      transfer_index,
      block_number,
      block_hash,
      block_timestamp,
      status,
      attempt_count,
      created_at
    )
    SELECT
      w.watch_id || ':' || $2 AS id,
      w.watch_id,
      w.watch_target,
      $1 AS chain_id,
      $3 AS token_address,
      $4 AS from_address,
      $5 AS deposit_address,
      $6::numeric AS amount,
      w.required_amount,
      $7 AS transaction_hash,
      $8 AS transfer_index,
      $9::numeric AS block_number,
      $10 AS block_hash,
      $11::numeric AS block_timestamp,
      'pending' AS status,
      0 AS attempt_count,
      $12::numeric AS created_at
    FROM ${activeWatchTable} w
    WHERE w.chain_id = $1
      AND w.token_address = $3
      AND w.deposit_address = $5
      AND $6::numeric >= w.min_amount
      AND $6::numeric <= w.max_amount
      AND $11::numeric >= w.created_at
      AND $11::numeric <= w.expires_at
    ON CONFLICT (id) DO NOTHING
    `,
    [
      validatedChainId,
      transfer.id,
      transfer.tokenAddress,
      transfer.fromAddress,
      transfer.toAddress,
      transfer.amount,
      transfer.transactionHash,
      transfer.logIndex,
      transfer.blockNumber,
      transfer.blockHash,
      transfer.blockTimestamp,
      nowSeconds(),
    ],
  );
}

function transferDedupKey(transfer: Pick<TransferNotification, "transactionHash" | "logIndex">) {
  return `${validatedChainId}:${transfer.transactionHash.toLowerCase()}:${transfer.logIndex}`;
}

function recentTransferPollSource(): Source<TransferNotification> {
  return {
    name: "recent-poll",
    start: (emit) => {
      if (!process.env.PONDER_RPC_URL_HTTP) {
        console.warn("PONDER_RPC_URL_HTTP is not configured; transfer poll source is disabled");
        return undefined;
      }

      let stopped = false;
      const tick = () => {
        if (stopped) return;
        void pollRecentTransfers(emit).catch((error) => {
          console.warn("transfer poll source failed", error);
        });
      };
      const first = setTimeout(tick, 1_000);
      first.unref?.();
      const timer = setInterval(tick, TRANSFER_POLL_INTERVAL_MS);
      timer.unref?.();
      return () => {
        stopped = true;
        clearTimeout(first);
        clearInterval(timer);
      };
    },
  };
}

async function pollRecentTransfers(emit: (event: TransferNotification) => Promise<void>) {
  const filters = await activeTransferFilters();
  if (filters.tokens.length === 0 || filters.recipients.length === 0) return;

  const latest = parseHexQuantity(await rawRpc<string>("eth_blockNumber", []), "latest block");
  const window = BigInt(TRANSFER_POLL_RECENT_BLOCKS);
  const from = latest >= window ? latest - window + 1n : 0n;
  const blockTimestampCache = new Map<string, string>();

  for (let blockStart = from; blockStart <= latest;) {
    const blockEnd = minBigInt(
      latest,
      blockStart + BigInt(TRANSFER_POLL_MAX_BLOCK_SPAN) - 1n,
    );

    for (const tokenChunk of chunks(filters.tokens, TRANSFER_POLL_FILTER_CHUNK_SIZE)) {
      for (const recipientChunk of chunks(
        filters.recipients,
        TRANSFER_POLL_FILTER_CHUNK_SIZE,
      )) {
        const logs = await rawRpc<RpcTransferLog[]>("eth_getLogs", [
          {
            address: tokenChunk,
            topics: [
              TRANSFER_EVENT_TOPIC,
              null,
              recipientChunk.map(addressToTopic),
            ],
            fromBlock: hexQuantity(blockStart),
            toBlock: hexQuantity(blockEnd),
          },
        ]);

        for (const log of logs) {
          if (log.removed) continue;
          await emit(await transferFromRpcLog(log, blockTimestampCache));
        }
      }
    }

    blockStart = blockEnd + 1n;
  }
}

async function activeTransferFilters() {
  const result = await pool.query<{
    token_address: string;
    deposit_address: string;
  }>(
    `
    SELECT DISTINCT token_address, deposit_address
    FROM ${activeWatchTable}
    WHERE chain_id = $1
      AND expires_at >= $2::numeric
    `,
    [validatedChainId, nowSeconds()],
  );

  return {
    tokens: uniqueSortedHexAddresses(result.rows.map((row) => row.token_address)),
    recipients: uniqueSortedHexAddresses(result.rows.map((row) => row.deposit_address)),
  };
}

async function transferFromRpcLog(
  log: RpcTransferLog,
  blockTimestampCache: Map<string, string>,
): Promise<TransferNotification> {
  const tokenAddress = parseHexAddress(log.address, "log.address") as `0x${string}`;
  const topics = Array.isArray(log.topics) ? log.topics : [];
  const topic0 = requireHex(topics[0], "log.topic0", 32);
  if (topic0 !== TRANSFER_EVENT_TOPIC) {
    throw new Error("log.topic0 is not an ERC-20 Transfer event");
  }
  const fromAddress = addressFromTopic(requireHex(topics[1], "log.topic1", 32));
  const toAddress = addressFromTopic(requireHex(topics[2], "log.topic2", 32));
  const blockNumber = parseHexQuantity(log.blockNumber, "log.blockNumber");
  const blockNumberString = blockNumber.toString();
  const transactionHash = requireHex(log.transactionHash, "log.transactionHash", 32) as `0x${string}`;
  const logIndex = Number(parseHexQuantity(log.logIndex, "log.logIndex"));
  if (!Number.isSafeInteger(logIndex)) {
    throw new Error("log.logIndex must be a safe integer");
  }
  let blockTimestamp = blockTimestampCache.get(blockNumberString);
  if (blockTimestamp === undefined) {
    const block = await rawRpc<RpcBlock>("eth_getBlockByNumber", [
      hexQuantity(blockNumber),
      false,
    ]);
    blockTimestamp = parseHexQuantity(block.timestamp, "block.timestamp").toString();
    blockTimestampCache.set(blockNumberString, blockTimestamp);
  }

  return {
    id: transferDedupKey({ transactionHash, logIndex }),
    chainId: validatedChainId,
    tokenAddress,
    fromAddress,
    toAddress,
    amount: parseHexQuantity(log.data ?? "0x0", "log.data").toString(),
    transactionHash,
    blockNumber: blockNumberString,
    blockHash: requireHex(log.blockHash, "log.blockHash", 32) as `0x${string}`,
    logIndex,
    blockTimestamp,
  };
}

async function rawRpc<T>(method: string, params: unknown[]): Promise<T> {
  const url = process.env.PONDER_RPC_URL_HTTP;
  if (!url) throw new Error("PONDER_RPC_URL_HTTP is not configured");
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  });
  if (!response.ok) {
    throw new Error(`${method} failed with HTTP ${response.status}`);
  }
  const body = (await response.json()) as {
    result?: T;
    error?: { message?: string };
  };
  if (body.error) {
    throw new Error(`${method} failed: ${body.error.message ?? "unknown RPC error"}`);
  }
  if (body.result === undefined) {
    throw new Error(`${method} returned no result`);
  }
  return body.result;
}

function uniqueSortedHexAddresses(values: string[]) {
  return [...new Set(values.map((value) => parseHexAddress(value, "address")))].sort();
}

function addressToTopic(address: string) {
  return `0x${"0".repeat(24)}${address.slice(2)}`;
}

function addressFromTopic(topic: string) {
  return `0x${topic.slice(-40)}` as `0x${string}`;
}

function requireHex(value: unknown, field: string, bytes: number) {
  if (
    typeof value !== "string" ||
    !new RegExp(`^0x[0-9a-fA-F]{${bytes * 2}}$`).test(value)
  ) {
    throw new Error(`${field} must be a ${bytes}-byte hex string`);
  }
  return value.toLowerCase();
}

function parseHexQuantity(value: unknown, field: string) {
  if (typeof value !== "string" || !/^0x[0-9a-fA-F]+$/.test(value)) {
    throw new Error(`${field} must be a hex quantity`);
  }
  return BigInt(value);
}

function hexQuantity(value: bigint) {
  return `0x${value.toString(16)}`;
}

function minBigInt(left: bigint, right: bigint) {
  return left < right ? left : right;
}

function chunks<T>(values: T[], size: number) {
  const result: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }
  return result;
}
