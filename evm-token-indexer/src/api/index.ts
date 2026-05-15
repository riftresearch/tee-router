import { db } from "ponder:api";
import { erc20TransferRaw } from "ponder:schema";
import { and, desc, eq, gte } from "ponder";
import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { randomUUID, timingSafeEqual } from "node:crypto";
import http, { type IncomingMessage } from "node:http";
import type { Socket } from "node:net";
import pg from "pg";
import { WebSocket, WebSocketServer, type RawData } from "ws";

const app = new Hono();
const schemaName = process.env.DATABASE_SCHEMA ?? process.env.PONDER_SCHEMA;
const API_KEY_MIN_LENGTH = 32;

if (!schemaName) {
  throw new Error("DATABASE_SCHEMA or PONDER_SCHEMA must be set for indexer write APIs");
}

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL must be set for indexer write APIs");
}

const pool = new pg.Pool({
  connectionString: databaseUrl,
  application_name: "evm-token-indexer-api",
  max: parsePositiveInteger(
    process.env.EVM_TOKEN_INDEXER_API_POOL_SIZE,
    "EVM_TOKEN_INDEXER_API_POOL_SIZE",
    5,
    100,
  ),
});
const apiKey = normalizeApiKey(process.env.EVM_TOKEN_INDEXER_API_KEY);
const allowUnauthenticated =
  process.env.EVM_TOKEN_INDEXER_ALLOW_UNAUTHENTICATED === "true";

if (allowUnauthenticated && isProductionEnvironment()) {
  throw new Error(
    "EVM_TOKEN_INDEXER_ALLOW_UNAUTHENTICATED=true is not allowed in production",
  );
}

if (!apiKey && !allowUnauthenticated) {
  throw new Error(
    "EVM_TOKEN_INDEXER_API_KEY must be set for indexer APIs, or EVM_TOKEN_INDEXER_ALLOW_UNAUTHENTICATED=true must be explicit",
  );
}

const chainId = parsePositiveInteger(
  process.env.PONDER_CHAIN_ID,
  "PONDER_CHAIN_ID",
  undefined,
  Number.MAX_SAFE_INTEGER,
);
const MAX_WATCH_SYNC_SIZE = 50_000;
const MAX_PENDING_CANDIDATES_LIMIT = 1_000;
const DEFAULT_TRANSFER_LOOKUP_LIMIT = 50;
const MAX_TRANSFER_LOOKUP_LIMIT = 250;
const DEFAULT_TRANSFERS_LIMIT = 100;
const MAX_TRANSFERS_LIMIT = 1_000;
const DEFAULT_LOGS_LIMIT = 100;
const MAX_LOGS_LIMIT = 1_000;
const MAX_DECIMAL_STRING_LENGTH = 78;
const U64_MAX = (1n << 64n) - 1n;
const MAX_API_BODY_BYTES = 64 * 1024 * 1024;
const MAX_CANDIDATE_ID_LENGTH = 512;
const MAX_CANDIDATE_ERROR_LENGTH = 2_048;
const MAX_PRUNE_RELEVANT_ADDRESSES = 100_000;
const MAX_SUBSCRIBE_FILTER_VALUES = 1_000;
const DEFAULT_REORG_SAFE_BLOCKS = 1_000;
const WS_PATH = "/subscribe";
const WS_SLOW_SUBSCRIBER_BUFFER_BYTES = parsePositiveInteger(
  process.env.EVM_TOKEN_INDEXER_WS_MAX_BUFFERED_BYTES,
  "EVM_TOKEN_INDEXER_WS_MAX_BUFFERED_BYTES",
  1024 * 1024,
  64 * 1024 * 1024,
);
const RAW_LOG_POLL_INTERVAL_MS = parsePositiveInteger(
  process.env.EVM_TOKEN_INDEXER_RAW_LOG_POLL_INTERVAL_MS,
  "EVM_TOKEN_INDEXER_RAW_LOG_POLL_INTERVAL_MS",
  2_000,
  60_000,
);
const RAW_LOG_MAX_BLOCK_SPAN = parsePositiveInteger(
  process.env.EVM_TOKEN_INDEXER_RAW_LOG_MAX_BLOCK_SPAN,
  "EVM_TOKEN_INDEXER_RAW_LOG_MAX_BLOCK_SPAN",
  1_000,
  100_000,
);
const WATCH_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const CANDIDATE_ID_PATTERN = /^[A-Za-z0-9._:-]+$/;

app.use("*", async (c, next) => {
  if (c.req.path === "/health") {
    await next();
    return;
  }

  if (apiKey && isAuthorizedBearer(c.req.header("authorization"), apiKey)) {
    await next();
    return;
  }

  if (!apiKey && allowUnauthenticated) {
    await next();
    return;
  }

  return c.json(
    { error: "unauthorized" },
    401,
    { "WWW-Authenticate": "Bearer" },
  );
});
app.use(
  "*",
  bodyLimit({
    maxSize: MAX_API_BODY_BYTES,
    onError: () =>
      new Response(
        JSON.stringify({ error: `request body must be at most ${MAX_API_BODY_BYTES} bytes` }),
        {
          status: 413,
          headers: { "content-type": "application/json" },
        },
      ),
  }),
);

const quoteIdentifier = (value: string) => {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error(`Invalid SQL identifier: ${value}`);
  }
  return `"${value}"`;
};

const tableName = (table: string) =>
  `${quoteIdentifier(schemaName)}.${quoteIdentifier(table)}`;

const normalizeAddress = (address: string) => address.toLowerCase();

const nowSeconds = () => Math.floor(Date.now() / 1000).toString();
const rawRetentionSeconds = () =>
  parsePositiveInteger(
    process.env.EVM_TOKEN_INDEXER_RAW_RETENTION_SECONDS,
    "EVM_TOKEN_INDEXER_RAW_RETENTION_SECONDS",
    30 * 24 * 60 * 60,
    10 * 365 * 24 * 60 * 60,
  );

const activeWatchTable = tableName("active_deposit_watch");
const rawTransferTable = tableName("erc20_transfer_raw");
const rawLogTable = tableName("raw_log");
const candidateTable = tableName("detected_deposit_candidate");
const transferNotifyChannel = `evm_token_indexer_transfer_${chainId}`;
const logNotifyChannel = `evm_token_indexer_log_${chainId}`;

type NormalizedWatchPayload = {
  watch_id: string;
  watch_target: string;
  token_address: string;
  deposit_address: string;
  min_amount: string;
  max_amount: string;
  required_amount: string;
  created_at: string;
  updated_at: string;
  expires_at: string;
};

type TransferRow = {
  id: string;
  chain_id: number;
  token_address: string;
  from_address: string;
  to_address: string;
  amount: string;
  transaction_hash: string;
  block_number: string;
  block_hash: string;
  log_index: number;
  block_timestamp: string;
};

type RawLogRow = {
  id: string;
  chain_id: number;
  block_number: string;
  log_index: number;
  tx_hash: string;
  address: string;
  topic0: string;
  topic1: string | null;
  topic2: string | null;
  topic3: string | null;
  data: string;
  block_timestamp: string;
};

type EvmTransfer = {
  id: string;
  chainId: number;
  tokenAddress: string;
  fromAddress: string;
  toAddress: string;
  amount: string;
  transactionHash: string;
  blockNumber: string;
  blockHash: string;
  logIndex: number;
  blockTimestamp: string;
};

type EvmRawLog = {
  id: string;
  chainId: number;
  blockNumber: string;
  logIndex: number;
  txHash: string;
  address: string;
  topic0: string;
  topic1: string | null;
  topic2: string | null;
  topic3: string | null;
  data: string;
  blockTimestamp: string;
};

type Cursor = {
  blockNumber: string;
  logIndex: number;
};

type SubscribeFilter = {
  kind: "transfer";
  tokenAddresses: Set<string>;
  recipientAddresses: Set<string>;
  minAmount?: bigint;
  maxAmount?: bigint;
};

type LogSubscribeFilter = {
  kind: "log";
  addresses: Set<string>;
  topic0: string;
  topic1?: string;
  topic2?: string;
  topic3?: string;
};

type SubscriptionFilter = SubscribeFilter | LogSubscribeFilter;

type Subscription = {
  id: string;
  filter: SubscriptionFilter;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

function parsePositiveInteger(
  value: string | undefined,
  name: string,
  defaultValue: number | undefined,
  max: number,
) {
  const raw = value ?? defaultValue?.toString();
  if (raw === undefined || !/^\d+$/.test(raw)) {
    throw new Error(`${name} must be a positive integer`);
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > max) {
    throw new Error(`${name} must be a positive integer <= ${max}`);
  }
  return parsed;
}

function normalizeApiKey(value: string | undefined) {
  const trimmed = value?.trim();
  if (!trimmed) return undefined;
  if (trimmed.length < API_KEY_MIN_LENGTH) {
    throw new Error(
      `EVM_TOKEN_INDEXER_API_KEY must be at least ${API_KEY_MIN_LENGTH} characters`,
    );
  }
  return trimmed;
}

function isProductionEnvironment() {
  return (
    process.env.NODE_ENV === "production" ||
    process.env.RAILWAY_ENVIRONMENT === "production"
  );
}

function isAuthorizedBearer(header: string | undefined, expected: string) {
  const match = /^Bearer\s+(.+)$/i.exec(header ?? "");
  const token = match?.[1]?.trim();
  if (!token) return false;
  return constantTimeEquals(token, expected);
}

function constantTimeEquals(left: string, right: string) {
  const leftBytes = Buffer.from(left);
  const rightBytes = Buffer.from(right);
  return leftBytes.length === rightBytes.length && timingSafeEqual(leftBytes, rightBytes);
}

function parseQueryInteger(
  value: string | undefined,
  defaultValue: number,
  max: number,
) {
  const raw = value ?? defaultValue.toString();
  if (!/^\d+$/.test(raw)) return undefined;
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > max) {
    return undefined;
  }
  return parsed;
}

function parseOptionalU64(value: string | undefined, field: string) {
  if (value === undefined || value.length === 0) return undefined;
  return parseU64(value, field);
}

function parseU64(value: unknown, field: string) {
  if (typeof value !== "string" && typeof value !== "number") {
    throw new Error(`${field} must be a u64 integer`);
  }
  const raw = typeof value === "number" ? value.toString() : value;
  if (!/^\d+$/.test(raw)) {
    throw new Error(`${field} must be a u64 integer`);
  }
  const parsed = BigInt(raw);
  if (parsed > U64_MAX) {
    throw new Error(`${field} must be <= ${U64_MAX.toString()}`);
  }
  return raw;
}

function parseDecimalString(value: unknown, field: string) {
  if (
    typeof value !== "string" ||
    value.length > MAX_DECIMAL_STRING_LENGTH ||
    !/^\d+$/.test(value)
  ) {
    throw new Error(
      `${field} must be a non-negative integer string of at most ${MAX_DECIMAL_STRING_LENGTH} digits`,
    );
  }
  return value;
}

function parseOptionalDecimalString(value: unknown, field: string) {
  if (value === undefined || value === null || value === "") return undefined;
  return parseDecimalString(value, field);
}

function parseHexAddress(value: unknown, field: string) {
  if (typeof value !== "string" || !/^0x[0-9a-fA-F]{40}$/.test(value)) {
    throw new Error(`${field} must be a 20-byte hex address`);
  }
  return normalizeAddress(value);
}

function parseHexAddressArray(value: unknown, field: string, max: number) {
  if (!Array.isArray(value)) {
    throw new Error(`${field} must be an array`);
  }
  if (value.length > max) {
    throw new Error(`${field} must contain at most ${max} addresses`);
  }
  const seen = new Set<string>();
  for (const [index, item] of value.entries()) {
    seen.add(parseHexAddress(item, `${field}[${index}]`));
  }
  return [...seen];
}

function parseTopic(value: unknown, field: string) {
  return requireHex(value, field, 32);
}

function parseOptionalTopic(value: unknown, field: string) {
  if (value === undefined || value === null || value === "") return undefined;
  return parseTopic(value, field);
}

function transferFromRow(row: TransferRow): EvmTransfer {
  return {
    id: row.id,
    chainId: row.chain_id,
    tokenAddress: row.token_address,
    fromAddress: row.from_address,
    toAddress: row.to_address,
    amount: row.amount,
    transactionHash: row.transaction_hash,
    blockNumber: row.block_number,
    blockHash: row.block_hash,
    logIndex: row.log_index,
    blockTimestamp: row.block_timestamp,
  };
}

function rawLogFromRow(row: RawLogRow): EvmRawLog {
  return {
    id: row.id,
    chainId: row.chain_id,
    blockNumber: row.block_number,
    logIndex: row.log_index,
    txHash: row.tx_hash,
    address: row.address,
    topic0: row.topic0,
    topic1: row.topic1,
    topic2: row.topic2,
    topic3: row.topic3,
    data: row.data,
    blockTimestamp: row.block_timestamp,
  };
}

function encodeCursor(cursor: Cursor) {
  return Buffer.from(`${cursor.blockNumber}:${cursor.logIndex}`).toString("base64url");
}

function decodeCursor(value: string | undefined) {
  if (value === undefined || value.length === 0) return undefined;
  let decoded: string;
  try {
    decoded = Buffer.from(value, "base64url").toString("utf8");
  } catch {
    throw new Error("cursor must be an opaque base64url cursor");
  }
  const parts = decoded.split(":");
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new Error("cursor is malformed");
  }
  const blockNumber = parseU64(parts[0], "cursor.block_number");
  const rawLogIndex = parts[1];
  if (!/^\d+$/.test(rawLogIndex)) {
    throw new Error("cursor.log_index must be a non-negative integer");
  }
  const logIndex = Number(rawLogIndex);
  if (!Number.isSafeInteger(logIndex) || logIndex < 0) {
    throw new Error("cursor.log_index must be a safe non-negative integer");
  }
  return { blockNumber, logIndex };
}

function parseWatchPayloads(value: unknown): NormalizedWatchPayload[] {
  if (!Array.isArray(value)) {
    throw new Error("watches must be an array");
  }
  if (value.length > MAX_WATCH_SYNC_SIZE) {
    throw new Error(`watches must contain at most ${MAX_WATCH_SYNC_SIZE} entries`);
  }

  const seen = new Set<string>();
  return value.map((item, index) => {
    if (!isRecord(item)) {
      throw new Error(`watches[${index}] must be an object`);
    }
    const watchId = item.watchId;
    if (typeof watchId !== "string" || !WATCH_ID_PATTERN.test(watchId)) {
      throw new Error(`watches[${index}].watchId must be a UUID`);
    }
    if (!seen.add(watchId)) {
      throw new Error(`duplicate watchId ${watchId}`);
    }
    const watchTarget = item.watchTarget;
    if (watchTarget !== "provider_operation" && watchTarget !== "funding_vault") {
      throw new Error(`watches[${index}].watchTarget is unsupported`);
    }

    const minAmount = parseDecimalString(item.minAmount, `watches[${index}].minAmount`);
    const maxAmount = parseDecimalString(item.maxAmount, `watches[${index}].maxAmount`);
    const requiredAmount = parseDecimalString(
      item.requiredAmount,
      `watches[${index}].requiredAmount`,
    );
    if (BigInt(minAmount) > BigInt(maxAmount)) {
      throw new Error(`watches[${index}].minAmount cannot exceed maxAmount`);
    }
    if (BigInt(requiredAmount) > BigInt(maxAmount)) {
      throw new Error(`watches[${index}].requiredAmount cannot exceed maxAmount`);
    }

    return {
      watch_id: watchId,
      watch_target: watchTarget,
      token_address: parseHexAddress(item.tokenAddress, `watches[${index}].tokenAddress`),
      deposit_address: parseHexAddress(
        item.depositAddress,
        `watches[${index}].depositAddress`,
      ),
      min_amount: minAmount,
      max_amount: maxAmount,
      required_amount: requiredAmount,
      created_at: parseDecimalString(item.createdAt, `watches[${index}].createdAt`),
      updated_at: parseDecimalString(item.updatedAt, `watches[${index}].updatedAt`),
      expires_at: parseDecimalString(item.expiresAt, `watches[${index}].expiresAt`),
    };
  });
}

function parseCandidateId(value: string) {
  if (
    value.length === 0 ||
    value.length > MAX_CANDIDATE_ID_LENGTH ||
    !CANDIDATE_ID_PATTERN.test(value)
  ) {
    throw new Error(`candidate id must be 1-${MAX_CANDIDATE_ID_LENGTH} safe path characters`);
  }
  return value;
}

function parseCandidateError(body: unknown) {
  if (!isRecord(body) || body.error === undefined || body.error === null) return null;
  if (
    typeof body.error !== "string" ||
    body.error.length > MAX_CANDIDATE_ERROR_LENGTH
  ) {
    throw new Error(
      `error must be a string of at most ${MAX_CANDIDATE_ERROR_LENGTH} characters`,
    );
  }
  return body.error;
}

const badRequest = (message: string) =>
  new Response(JSON.stringify({ error: message }), {
    status: 400,
    headers: { "content-type": "application/json" },
  });

const materializeCandidates = async () => {
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
      w.watch_id || ':' || t.id AS id,
      w.watch_id,
      w.watch_target,
      t.chain_id,
      t.token_address,
      t.from_address,
      t.to_address AS deposit_address,
      t.amount,
      w.required_amount,
      t.transaction_hash,
      t.log_index AS transfer_index,
      t.block_number,
      t.block_hash,
      t.block_timestamp,
      'pending' AS status,
      0 AS attempt_count,
      $2::numeric AS created_at
    FROM ${activeWatchTable} w
    JOIN ${rawTransferTable} t
      ON t.chain_id = w.chain_id
     AND t.token_address = w.token_address
     AND t.to_address = w.deposit_address
    WHERE w.chain_id = $1
      AND t.amount >= w.min_amount
      AND t.amount <= w.max_amount
      AND t.block_timestamp >= w.created_at
      AND t.block_timestamp <= w.expires_at
    ON CONFLICT (id) DO NOTHING
    `,
    [chainId, nowSeconds()],
  );
};

const reorgSafeBlocks = () =>
  parsePositiveInteger(
    process.env.EVM_TOKEN_INDEXER_REORG_SAFE_BLOCKS,
    "EVM_TOKEN_INDEXER_REORG_SAFE_BLOCKS",
    DEFAULT_REORG_SAFE_BLOCKS,
    1_000_000,
  );

const pruneTransfers = async (relevantAddresses: string[], beforeBlock: string) => {
  const result = await pool.query(
    `
    DELETE FROM ${rawTransferTable}
    WHERE chain_id = $1
      AND block_number < $2::numeric
      AND NOT (to_address = ANY($3::text[]))
    `,
    [chainId, beforeBlock, relevantAddresses],
  );
  console.info(
    `accepted prune completed: pruned=${result.rowCount ?? 0} before_block=${beforeBlock}`,
  );
};

const latestIndexedBlock = async () => {
  const result = await pool.query<{ head_block: string | null }>(
    `
    SELECT max(block_number)::text AS head_block
    FROM ${rawTransferTable}
    WHERE chain_id = $1
    `,
    [chainId],
  );
  return result.rows[0]?.head_block ?? null;
};

app.put("/watches", async (c) => {
  const body = (await c.req.json().catch(() => undefined)) as unknown;
  let watches: NormalizedWatchPayload[];
  try {
    watches = parseWatchPayloads(isRecord(body) ? body.watches : undefined);
  } catch (error) {
    return badRequest(error instanceof Error ? error.message : "invalid watches payload");
  }

  await pool.query(
    `
    WITH incoming AS (
      SELECT *
      FROM jsonb_to_recordset($2::jsonb) AS watch(
        watch_id text,
        watch_target text,
        token_address text,
        deposit_address text,
        min_amount text,
        max_amount text,
        required_amount text,
        created_at text,
        updated_at text,
        expires_at text
      )
    ),
    upserted AS (
      INSERT INTO ${activeWatchTable} (
        watch_id,
        watch_target,
        chain_id,
        token_address,
        deposit_address,
        min_amount,
        max_amount,
        required_amount,
        created_at,
        updated_at,
        expires_at
      )
      SELECT
        watch_id,
        watch_target,
        $1,
        token_address,
        deposit_address,
        min_amount::numeric,
        max_amount::numeric,
        required_amount::numeric,
        created_at::numeric,
        updated_at::numeric,
        expires_at::numeric
      FROM incoming
      ON CONFLICT (watch_id)
      DO UPDATE SET
        watch_target = EXCLUDED.watch_target,
        chain_id = EXCLUDED.chain_id,
        token_address = EXCLUDED.token_address,
        deposit_address = EXCLUDED.deposit_address,
        min_amount = EXCLUDED.min_amount,
        max_amount = EXCLUDED.max_amount,
        required_amount = EXCLUDED.required_amount,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        expires_at = EXCLUDED.expires_at
      RETURNING watch_id
    ),
    deleted AS (
      DELETE FROM ${activeWatchTable} active
      WHERE active.chain_id = $1
        AND NOT EXISTS (
          SELECT 1
          FROM incoming
          WHERE incoming.watch_id = active.watch_id
        )
      RETURNING watch_id
    )
    SELECT
      (SELECT count(*) FROM upserted)::int AS upserted,
      (SELECT count(*) FROM deleted)::int AS deleted
    `,
    [chainId, JSON.stringify(watches)],
  );

  await materializeCandidates();

  return c.json({ synced: watches.length });
});

app.post("/candidates/materialize", async (c) => {
  await materializeCandidates();
  return c.json({ ok: true });
});

app.get("/candidates/pending", async (c) => {
  const limit = parseQueryInteger(
    c.req.query("limit"),
    100,
    MAX_PENDING_CANDIDATES_LIMIT,
  );
  if (limit === undefined) {
    return badRequest(`limit must be a positive integer <= ${MAX_PENDING_CANDIDATES_LIMIT}`);
  }
  const result = await pool.query(
    `
    SELECT
      id,
      watch_id,
      watch_target,
      chain_id,
      token_address,
      from_address,
      deposit_address,
      amount::text,
      required_amount::text,
      transaction_hash,
      transfer_index,
      block_number::text,
      block_hash,
      block_timestamp::text,
      status,
      attempt_count,
      last_error,
      created_at::text,
      delivered_at::text
    FROM ${candidateTable}
    WHERE chain_id = $1
      AND status = 'pending'
    ORDER BY block_number ASC, transfer_index ASC, id ASC
    LIMIT $2
    `,
    [chainId, limit],
  );

  return c.json({
    candidates: result.rows.map((row) => ({
      id: row.id,
      watchId: row.watch_id,
      watchTarget: row.watch_target,
      chainId: row.chain_id,
      tokenAddress: row.token_address,
      fromAddress: row.from_address,
      depositAddress: row.deposit_address,
      amount: row.amount,
      requiredAmount: row.required_amount,
      transactionHash: row.transaction_hash,
      transferIndex: row.transfer_index,
      blockNumber: row.block_number,
      blockHash: row.block_hash,
      blockTimestamp: row.block_timestamp,
      status: row.status,
      attemptCount: row.attempt_count,
      lastError: row.last_error,
      createdAt: row.created_at,
      deliveredAt: row.delivered_at,
    })),
  });
});

app.post("/candidates/:id/mark-submitted", async (c) => {
  let id: string;
  try {
    id = parseCandidateId(c.req.param("id"));
  } catch (error) {
    return badRequest(error instanceof Error ? error.message : "invalid candidate id");
  }
  await pool.query(
    `
    UPDATE ${candidateTable}
    SET status = 'submitted',
        delivered_at = $2::numeric,
        last_error = NULL
    WHERE chain_id = $1
      AND id = $3
    `,
    [chainId, nowSeconds(), id],
  );

  return c.json({ ok: true });
});

app.post("/candidates/:id/release", async (c) => {
  let id: string;
  let errorMessage: string | null;
  try {
    id = parseCandidateId(c.req.param("id"));
    errorMessage = parseCandidateError(await c.req.json().catch(() => ({})));
  } catch (error) {
    return badRequest(error instanceof Error ? error.message : "invalid candidate release");
  }
  await pool.query(
    `
    UPDATE ${candidateTable}
    SET status = 'pending',
        attempt_count = attempt_count + 1,
        last_error = $3
    WHERE chain_id = $1
      AND id = $2
    `,
    [chainId, id, errorMessage],
  );

  return c.json({ ok: true });
});

app.post("/candidates/:id/discard", async (c) => {
  let id: string;
  let errorMessage: string | null;
  try {
    id = parseCandidateId(c.req.param("id"));
    errorMessage = parseCandidateError(await c.req.json().catch(() => ({})));
  } catch (error) {
    return badRequest(error instanceof Error ? error.message : "invalid candidate discard");
  }
  await pool.query(
    `
    UPDATE ${candidateTable}
    SET status = 'discarded',
        attempt_count = attempt_count + 1,
        last_error = $3
    WHERE chain_id = $1
      AND id = $2
    `,
    [chainId, id, errorMessage],
  );

  return c.json({ ok: true });
});

app.post("/maintenance/prune-raw", async (c) => {
  const body = (await c.req.json().catch(() => ({}))) as {
    retentionSeconds?: number;
  };
  const retentionSeconds =
    body.retentionSeconds === undefined
      ? rawRetentionSeconds()
      : Number.isSafeInteger(body.retentionSeconds) && body.retentionSeconds > 0
        ? body.retentionSeconds
        : undefined;
  if (retentionSeconds === undefined) {
    return badRequest("retentionSeconds must be a positive safe integer");
  }
  const cutoff = Math.floor(Date.now() / 1000) - retentionSeconds;
  const result = await pool.query(
    `
    DELETE FROM ${rawTransferTable}
    WHERE chain_id = $1
      AND block_timestamp < $2::numeric
    `,
    [chainId, cutoff.toString()],
  );

  return c.json({ pruned: result.rowCount ?? 0, cutoff: cutoff.toString() });
});

app.get("/transfers", async (c) => {
  let toAddress: string;
  let tokenAddress: string | undefined;
  let fromBlock: string | undefined;
  let minAmount: string | undefined;
  let maxAmount: string | undefined;
  let cursor: Cursor | undefined;
  try {
    toAddress = parseHexAddress(c.req.query("to"), "to");
    const token = c.req.query("token");
    tokenAddress =
      token === undefined || token.length === 0
        ? undefined
        : parseHexAddress(token, "token");
    fromBlock = parseOptionalU64(c.req.query("from_block"), "from_block");
    minAmount = parseOptionalDecimalString(c.req.query("min_amount"), "min_amount");
    maxAmount = parseOptionalDecimalString(c.req.query("max_amount"), "max_amount");
    if (
      minAmount !== undefined &&
      maxAmount !== undefined &&
      BigInt(minAmount) > BigInt(maxAmount)
    ) {
      throw new Error("min_amount cannot exceed max_amount");
    }
    cursor = decodeCursor(c.req.query("cursor"));
  } catch (error) {
    return badRequest(error instanceof Error ? error.message : "invalid transfer query");
  }

  const limit = parseQueryInteger(
    c.req.query("limit"),
    DEFAULT_TRANSFERS_LIMIT,
    MAX_TRANSFERS_LIMIT,
  );
  if (limit === undefined) {
    return badRequest(`limit must be a positive integer <= ${MAX_TRANSFERS_LIMIT}`);
  }

  const params: unknown[] = [chainId, toAddress];
  const conditions = ["chain_id = $1", "to_address = $2"];
  if (tokenAddress !== undefined) {
    params.push(tokenAddress);
    conditions.push(`token_address = $${params.length}`);
  }
  if (fromBlock !== undefined) {
    params.push(fromBlock);
    conditions.push(`block_number >= $${params.length}::numeric`);
  }
  if (minAmount !== undefined) {
    params.push(minAmount);
    conditions.push(`amount >= $${params.length}::numeric`);
  }
  if (maxAmount !== undefined) {
    params.push(maxAmount);
    conditions.push(`amount <= $${params.length}::numeric`);
  }
  if (cursor !== undefined) {
    params.push(cursor.blockNumber, cursor.logIndex);
    const blockParam = params.length - 1;
    const logParam = params.length;
    conditions.push(
      `(block_number > $${blockParam}::numeric OR (block_number = $${blockParam}::numeric AND log_index > $${logParam}))`,
    );
  }
  params.push(limit + 1);

  const result = await pool.query<TransferRow>(
    `
    SELECT
      id,
      chain_id,
      token_address,
      from_address,
      to_address,
      amount::text,
      transaction_hash,
      block_number::text,
      block_hash,
      log_index,
      block_timestamp::text
    FROM ${rawTransferTable}
    WHERE ${conditions.join("\n      AND ")}
    ORDER BY block_number ASC, log_index ASC
    LIMIT $${params.length}
    `,
    params,
  );

  const rows = result.rows.slice(0, limit);
  const hasMore = result.rows.length > limit;
  const last = rows.at(-1);
  return c.json({
    transfers: rows.map(transferFromRow),
    nextCursor:
      hasMore && last
        ? encodeCursor({ blockNumber: last.block_number, logIndex: last.log_index })
        : null,
    hasMore,
  });
});

app.get("/logs", async (c) => {
  let address: string;
  let topic0: string;
  let topic1: string | undefined;
  let topic2: string | undefined;
  let topic3: string | undefined;
  let fromBlock: string | undefined;
  let cursor: Cursor | undefined;
  try {
    address = parseHexAddress(c.req.query("address"), "address");
    topic0 = parseTopic(c.req.query("topic0"), "topic0");
    topic1 = parseOptionalTopic(c.req.query("topic1"), "topic1");
    topic2 = parseOptionalTopic(c.req.query("topic2"), "topic2");
    topic3 = parseOptionalTopic(c.req.query("topic3"), "topic3");
    fromBlock = parseOptionalU64(c.req.query("from_block"), "from_block");
    cursor = decodeCursor(c.req.query("cursor"));
  } catch (error) {
    return badRequest(error instanceof Error ? error.message : "invalid log query");
  }

  const limit = parseQueryInteger(
    c.req.query("limit"),
    DEFAULT_LOGS_LIMIT,
    MAX_LOGS_LIMIT,
  );
  if (limit === undefined) {
    return badRequest(`limit must be a positive integer <= ${MAX_LOGS_LIMIT}`);
  }

  const params: unknown[] = [chainId, address, topic0];
  const conditions = ["chain_id = $1", "address = $2", "topic0 = $3"];
  for (const [column, value] of [
    ["topic1", topic1],
    ["topic2", topic2],
    ["topic3", topic3],
  ] as const) {
    if (value !== undefined) {
      params.push(value);
      conditions.push(`${column} = $${params.length}`);
    }
  }
  if (fromBlock !== undefined) {
    params.push(fromBlock);
    conditions.push(`block_number >= $${params.length}::numeric`);
  }
  if (cursor !== undefined) {
    params.push(cursor.blockNumber, cursor.logIndex);
    const blockParam = params.length - 1;
    const logParam = params.length;
    conditions.push(
      `(block_number > $${blockParam}::numeric OR (block_number = $${blockParam}::numeric AND log_index > $${logParam}))`,
    );
  }
  params.push(limit + 1);

  const result = await pool.query<RawLogRow>(
    `
    SELECT
      id,
      chain_id,
      block_number::text,
      log_index,
      tx_hash,
      address,
      topic0,
      topic1,
      topic2,
      topic3,
      data,
      block_timestamp::text
    FROM ${rawLogTable}
    WHERE ${conditions.join("\n      AND ")}
    ORDER BY block_number ASC, log_index ASC
    LIMIT $${params.length}
    `,
    params,
  );

  const rows = result.rows.slice(0, limit);
  const hasMore = result.rows.length > limit;
  const last = rows.at(-1);
  return c.json({
    logs: rows.map(rawLogFromRow),
    nextCursor:
      hasMore && last
        ? encodeCursor({ blockNumber: last.block_number, logIndex: last.log_index })
        : null,
    hasMore,
  });
});

app.post("/prune", async (c) => {
  const body = (await c.req.json().catch(() => undefined)) as unknown;
  let relevantAddresses: string[];
  let beforeBlock: string;
  try {
    if (!isRecord(body)) {
      throw new Error("request body must be an object");
    }
    relevantAddresses = parseHexAddressArray(
      body.relevant_addresses,
      "relevant_addresses",
      MAX_PRUNE_RELEVANT_ADDRESSES,
    );
    beforeBlock = parseU64(body.before_block, "before_block");
  } catch (error) {
    return badRequest(error instanceof Error ? error.message : "invalid prune request");
  }

  const headBlock = await latestIndexedBlock();
  if (headBlock === null) {
    return c.json({ error: "cannot prune before any indexed transfer exists" }, 409);
  }

  const horizon = BigInt(reorgSafeBlocks());
  const highestPrunableBlock = BigInt(headBlock) > horizon ? BigInt(headBlock) - horizon : 0n;
  if (BigInt(beforeBlock) > highestPrunableBlock) {
    return badRequest(
      `before_block must be <= indexed head (${headBlock}) - reorg horizon (${horizon.toString()})`,
    );
  }

  setImmediate(() => {
    void pruneTransfers(relevantAddresses, beforeBlock).catch((error) => {
      console.warn("accepted prune failed", error);
    });
  });

  return c.json({
    accepted: true,
    beforeBlock,
    relevantAddressCount: relevantAddresses.length,
    reorgSafeBlocks: horizon.toString(),
  });
});

app.get("/transfers/to/:address", async (c) => {
  let address: `0x${string}`;
  try {
    address = parseHexAddress(c.req.param("address"), "address") as `0x${string}`;
  } catch (error) {
    return badRequest(error instanceof Error ? error.message : "invalid address");
  }
  const token = c.req.query("token");
  const limit = parseQueryInteger(
    c.req.query("limit"),
    DEFAULT_TRANSFER_LOOKUP_LIMIT,
    MAX_TRANSFER_LOOKUP_LIMIT,
  );
  if (limit === undefined) {
    return badRequest(`limit must be a positive integer <= ${MAX_TRANSFER_LOOKUP_LIMIT}`);
  }
  const minAmount = c.req.query("amount");

  const conditions = [
    eq(erc20TransferRaw.chainId, chainId),
    eq(erc20TransferRaw.toAddress, address),
  ];
  if (token) {
    try {
      conditions.push(
        eq(erc20TransferRaw.tokenAddress, parseHexAddress(token, "token") as `0x${string}`),
      );
    } catch (error) {
      return badRequest(error instanceof Error ? error.message : "invalid token");
    }
  }
  if (minAmount) {
    if (minAmount.length > MAX_DECIMAL_STRING_LENGTH || !/^\d+$/.test(minAmount)) {
      return badRequest(
        `amount must be a non-negative integer string of at most ${MAX_DECIMAL_STRING_LENGTH} digits`,
      );
    }
    conditions.push(gte(erc20TransferRaw.amount, BigInt(minAmount)));
  }
  const whereCondition =
    conditions.length > 1 ? and(...conditions) : conditions[0];

  const transfers = await db
    .select()
    .from(erc20TransferRaw)
    .where(whereCondition)
    .orderBy(desc(erc20TransferRaw.blockNumber), desc(erc20TransferRaw.logIndex))
    .limit(limit);

  return c.json({
    transfers: transfers.map((transfer) => ({
      id: transfer.id,
      amount: transfer.amount.toString(),
      timestamp: Number(transfer.blockTimestamp),
      from: transfer.fromAddress,
      to: transfer.toAddress,
      tokenAddress: transfer.tokenAddress,
      transactionHash: transfer.transactionHash,
      blockNumber: transfer.blockNumber.toString(),
      blockHash: transfer.blockHash,
      logIndex: transfer.logIndex,
    })),
  });
});

const wsServer = new WebSocketServer({ noServer: true });
const subscriptions = new Map<WebSocket, Subscription>();
const attachedServers = new WeakSet<http.Server>();

function startWebSocketUpgradeHandler() {
  if (process.env.EVM_TOKEN_INDEXER_DISABLE_WS === "true") return;

  wsServer.on("connection", (socket) => {
    socket.on("message", (data) => handleWebSocketMessage(socket, data));
    socket.on("close", () => subscriptions.delete(socket));
    socket.on("error", () => subscriptions.delete(socket));
  });

  startNotificationListener();
  startRawLogPoller();

  const attach = () => {
    const handles =
      (process as NodeJS.Process & { _getActiveHandles?: () => unknown[] })
        ._getActiveHandles?.() ?? [];
    let attached = false;
    for (const handle of handles) {
      if (!isHttpServer(handle) || attachedServers.has(handle)) continue;
      handle.on("upgrade", handleUpgrade);
      attachedServers.add(handle);
      attached = true;
    }
    return attached;
  };

  attach();
  const timer = setInterval(attach, 1_000);
  timer.unref?.();
}

function isHttpServer(value: unknown): value is http.Server {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Partial<http.Server>;
  return (
    typeof candidate.on === "function" &&
    typeof candidate.address === "function" &&
    typeof candidate.listeners === "function"
  );
}

function handleUpgrade(request: IncomingMessage, socket: Socket, head: Buffer) {
  if (requestPath(request) !== WS_PATH) return;

  if (!isUpgradeAuthorized(request)) {
    socket.write(
      "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer\r\nContent-Length: 0\r\n\r\n",
    );
    socket.destroy();
    return;
  }

  wsServer.handleUpgrade(request, socket, head, (ws) => {
    wsServer.emit("connection", ws, request);
  });
}

function requestPath(request: IncomingMessage) {
  try {
    return new URL(
      request.url ?? "/",
      `http://${request.headers.host ?? "localhost"}`,
    ).pathname;
  } catch {
    return undefined;
  }
}

function isUpgradeAuthorized(request: IncomingMessage) {
  const authorization = Array.isArray(request.headers.authorization)
    ? request.headers.authorization[0]
    : request.headers.authorization;

  if (apiKey && isAuthorizedBearer(authorization, apiKey)) return true;
  return !apiKey && allowUnauthenticated;
}

function handleWebSocketMessage(socket: WebSocket, data: RawData) {
  const text = rawDataToString(data);
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    sendWebSocketJson(socket, {
      kind: "error",
      error: error instanceof Error ? `invalid JSON frame: ${error.message}` : "invalid JSON frame",
    });
    return;
  }

  if (isRecord(parsed) && parsed.action === "unsubscribe") {
    const subscription = subscriptions.get(socket);
    subscriptions.delete(socket);
    sendWebSocketJson(socket, {
      kind: "unsubscribed",
      subscription_id: subscription?.id ?? null,
    });
    return;
  }

  try {
    const filter = parseSubscribeMessage(parsed);
    const subscription = { id: randomUUID(), filter };
    subscriptions.set(socket, subscription);
    sendWebSocketJson(socket, {
      kind: "subscribed",
      subscription_id: subscription.id,
    });
  } catch (error) {
    sendWebSocketJson(socket, {
      kind: "error",
      error: error instanceof Error ? error.message : "invalid subscribe frame",
    });
  }
}

function rawDataToString(data: RawData) {
  if (typeof data === "string") return data;
  if (Buffer.isBuffer(data)) return data.toString("utf8");
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  return Buffer.from(data).toString("utf8");
}

function parseSubscribeMessage(value: unknown): SubscriptionFilter {
  const payload =
    isRecord(value) && value.action === "subscribe" ? value.filter : value;
  if (!isRecord(payload)) {
    throw new Error("subscribe frame must contain a filter object");
  }

  if ("addresses" in payload || "topic0" in payload) {
    return parseLogSubscribeFilter(payload);
  }

  const tokenAddresses = parseHexAddressArray(
    payload.token_addresses,
    "filter.token_addresses",
    MAX_SUBSCRIBE_FILTER_VALUES,
  );
  const recipientAddresses = parseHexAddressArray(
    payload.recipient_addresses,
    "filter.recipient_addresses",
    MAX_SUBSCRIBE_FILTER_VALUES,
  );
  if (tokenAddresses.length === 0) {
    throw new Error("filter.token_addresses must not be empty");
  }
  if (recipientAddresses.length === 0) {
    throw new Error("filter.recipient_addresses must not be empty");
  }

  const minAmount = parseOptionalDecimalString(payload.min_amount, "filter.min_amount");
  const maxAmount = parseOptionalDecimalString(payload.max_amount, "filter.max_amount");
  if (
    minAmount !== undefined &&
    maxAmount !== undefined &&
    BigInt(minAmount) > BigInt(maxAmount)
  ) {
    throw new Error("filter.min_amount cannot exceed filter.max_amount");
  }

  return {
    kind: "transfer",
    tokenAddresses: new Set(tokenAddresses),
    recipientAddresses: new Set(recipientAddresses),
    minAmount: minAmount === undefined ? undefined : BigInt(minAmount),
    maxAmount: maxAmount === undefined ? undefined : BigInt(maxAmount),
  };
}

function parseLogSubscribeFilter(payload: Record<string, unknown>): LogSubscribeFilter {
  const addresses = parseHexAddressArray(
    payload.addresses,
    "filter.addresses",
    MAX_SUBSCRIBE_FILTER_VALUES,
  );
  if (addresses.length === 0) {
    throw new Error("filter.addresses must not be empty");
  }

  return {
    kind: "log",
    addresses: new Set(addresses),
    topic0: parseTopic(payload.topic0, "filter.topic0"),
    topic1: parseOptionalTopic(payload.topic1, "filter.topic1"),
    topic2: parseOptionalTopic(payload.topic2, "filter.topic2"),
    topic3: parseOptionalTopic(payload.topic3, "filter.topic3"),
  };
}

function startNotificationListener() {
  const connect = async () => {
    const client = new pg.Client({
      connectionString: databaseUrl,
      application_name: "evm-token-indexer-ws-listener",
    });
    let reconnecting = false;

    const reconnect = () => {
      if (reconnecting) return;
      reconnecting = true;
      void client.end().catch(() => undefined);
      setTimeout(() => {
        void connect();
      }, 1_000).unref?.();
    };

    client.on("notification", (message) => {
      if (!message.payload) return;
      if (message.channel === transferNotifyChannel) {
        let transfer: EvmTransfer;
        try {
          transfer = parseTransferNotification(JSON.parse(message.payload));
        } catch (error) {
          console.warn("invalid transfer notification", error);
          return;
        }
        broadcastTransfer(transfer);
      } else if (message.channel === logNotifyChannel) {
        let log: EvmRawLog;
        try {
          log = parseRawLogNotification(JSON.parse(message.payload));
        } catch (error) {
          console.warn("invalid log notification", error);
          return;
        }
        broadcastRawLog(log);
      }
    });
    client.on("error", (error) => {
      console.warn("transfer notification listener failed", error);
      reconnect();
    });
    client.on("end", reconnect);

    try {
      await client.connect();
      await client.query(`LISTEN ${quoteIdentifier(transferNotifyChannel)}`);
      await client.query(`LISTEN ${quoteIdentifier(logNotifyChannel)}`);
    } catch (error) {
      console.warn("indexer notification listener failed to start", error);
      reconnect();
    }
  };

  void connect();
}

function parseTransferNotification(value: unknown): EvmTransfer {
  if (!isRecord(value)) {
    throw new Error("transfer notification must be an object");
  }
  return {
    id: requireString(value.id, "id"),
    chainId: Number(parseU64(value.chainId, "chainId")),
    tokenAddress: parseHexAddress(value.tokenAddress, "tokenAddress"),
    fromAddress: parseHexAddress(value.fromAddress, "fromAddress"),
    toAddress: parseHexAddress(value.toAddress, "toAddress"),
    amount: parseDecimalString(value.amount, "amount"),
    transactionHash: requireHex(value.transactionHash, "transactionHash", 32),
    blockNumber: parseU64(value.blockNumber, "blockNumber"),
    blockHash: requireHex(value.blockHash, "blockHash", 32),
    logIndex: Number(parseU64(value.logIndex, "logIndex")),
    blockTimestamp: parseU64(value.blockTimestamp, "blockTimestamp"),
  };
}

function parseRawLogNotification(value: unknown): EvmRawLog {
  if (!isRecord(value)) {
    throw new Error("log notification must be an object");
  }
  return {
    id: requireString(value.id, "id"),
    chainId: Number(parseU64(value.chainId, "chainId")),
    blockNumber: parseU64(value.blockNumber, "blockNumber"),
    logIndex: Number(parseU64(value.logIndex, "logIndex")),
    txHash: requireHex(value.txHash, "txHash", 32),
    address: parseHexAddress(value.address, "address"),
    topic0: requireHex(value.topic0, "topic0", 32),
    topic1: nullableTopic(value.topic1, "topic1"),
    topic2: nullableTopic(value.topic2, "topic2"),
    topic3: nullableTopic(value.topic3, "topic3"),
    data: requireHexData(value.data, "data"),
    blockTimestamp: parseU64(value.blockTimestamp, "blockTimestamp"),
  };
}

function requireString(value: unknown, field: string) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${field} must be a non-empty string`);
  }
  return value;
}

function nullableTopic(value: unknown, field: string) {
  if (value === undefined || value === null) return null;
  return requireHex(value, field, 32);
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

function requireHexData(value: unknown, field: string) {
  if (typeof value !== "string" || !/^0x(?:[0-9a-fA-F]{2})*$/.test(value)) {
    throw new Error(`${field} must be hex data`);
  }
  return value.toLowerCase();
}

function broadcastTransfer(transfer: EvmTransfer) {
  for (const [socket, subscription] of subscriptions) {
    if (socket.readyState !== WebSocket.OPEN) {
      subscriptions.delete(socket);
      continue;
    }
    if (subscription.filter.kind !== "transfer") continue;
    if (!transferMatchesFilter(transfer, subscription.filter)) continue;
    sendWebSocketJson(socket, { kind: "transfer", event: transfer });
  }
}

function broadcastRawLog(log: EvmRawLog) {
  for (const [socket, subscription] of subscriptions) {
    if (socket.readyState !== WebSocket.OPEN) {
      subscriptions.delete(socket);
      continue;
    }
    if (subscription.filter.kind !== "log") continue;
    if (!rawLogMatchesFilter(log, subscription.filter)) continue;
    sendWebSocketJson(socket, { kind: "log", event: log });
  }
}

function transferMatchesFilter(transfer: EvmTransfer, filter: SubscribeFilter) {
  if (!filter.tokenAddresses.has(transfer.tokenAddress)) return false;
  if (!filter.recipientAddresses.has(transfer.toAddress)) return false;
  const amount = BigInt(transfer.amount);
  if (filter.minAmount !== undefined && amount < filter.minAmount) return false;
  if (filter.maxAmount !== undefined && amount > filter.maxAmount) return false;
  return true;
}

function rawLogMatchesFilter(log: EvmRawLog, filter: LogSubscribeFilter) {
  if (!filter.addresses.has(log.address)) return false;
  if (filter.topic0 !== log.topic0) return false;
  if (filter.topic1 !== undefined && filter.topic1 !== log.topic1) return false;
  if (filter.topic2 !== undefined && filter.topic2 !== log.topic2) return false;
  if (filter.topic3 !== undefined && filter.topic3 !== log.topic3) return false;
  return true;
}

type RpcRawLog = {
  address?: string;
  topics?: string[];
  data?: string;
  blockNumber?: string;
  transactionHash?: string;
  logIndex?: string;
  removed?: boolean;
};

type RpcBlock = {
  timestamp?: string;
};

const rawLogFilterStates = new Map<string, bigint>();
let rawLogPollerStarted = false;

function startRawLogPoller() {
  if (rawLogPollerStarted) return;
  rawLogPollerStarted = true;
  if (!process.env.PONDER_RPC_URL_HTTP) {
    console.warn("PONDER_RPC_URL_HTTP is not configured; raw log subscriptions are disabled");
    return;
  }

  const timer = setInterval(() => {
    void pollRawLogs().catch((error) => {
      console.warn("raw log poll failed", error);
    });
  }, RAW_LOG_POLL_INTERVAL_MS);
  timer.unref?.();
}

function activeLogFilters() {
  const filters = new Map<string, LogSubscribeFilter>();
  for (const subscription of subscriptions.values()) {
    if (subscription.filter.kind !== "log") continue;
    filters.set(rawLogFilterKey(subscription.filter), subscription.filter);
  }
  return filters;
}

function rawLogFilterKey(filter: LogSubscribeFilter) {
  return JSON.stringify({
    addresses: [...filter.addresses].sort(),
    topic0: filter.topic0,
    topic1: filter.topic1 ?? null,
    topic2: filter.topic2 ?? null,
    topic3: filter.topic3 ?? null,
  });
}

async function pollRawLogs() {
  const filters = activeLogFilters();
  if (filters.size === 0) return;
  for (const key of rawLogFilterStates.keys()) {
    if (!filters.has(key)) {
      rawLogFilterStates.delete(key);
    }
  }

  const latest = parseHexQuantity(await rawRpc<string>("eth_blockNumber", []), "latest block");
  const blockTimestampCache = new Map<string, string>();
  for (const [key, filter] of filters) {
    const from = rawLogFilterStates.get(key) ?? latest;
    if (from > latest) continue;
    const to = minBigInt(latest, from + BigInt(RAW_LOG_MAX_BLOCK_SPAN) - 1n);
    const logs = await rawRpc<RpcRawLog[]>("eth_getLogs", [
      {
        address: [...filter.addresses],
        topics: [
          filter.topic0,
          filter.topic1 ?? null,
          filter.topic2 ?? null,
          filter.topic3 ?? null,
        ],
        fromBlock: hexQuantity(from),
        toBlock: hexQuantity(to),
      },
    ]);
    for (const log of logs) {
      if (log.removed) continue;
      const row = await rawLogRowFromRpcLog(log, blockTimestampCache);
      const inserted = await insertRawLog(row);
      if (inserted) {
        await pool.query("SELECT pg_notify($1, $2)", [
          logNotifyChannel,
          JSON.stringify(rawLogFromRow(row)),
        ]);
      }
    }
    rawLogFilterStates.set(key, to + 1n);
  }
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
  const body = (await response.json()) as { result?: T; error?: { message?: string } };
  if (body.error) {
    throw new Error(`${method} failed: ${body.error.message ?? "unknown RPC error"}`);
  }
  if (body.result === undefined) {
    throw new Error(`${method} returned no result`);
  }
  return body.result;
}

async function rawLogRowFromRpcLog(
  log: RpcRawLog,
  blockTimestampCache: Map<string, string>,
): Promise<RawLogRow> {
  const address = parseHexAddress(log.address, "log.address");
  const topics = Array.isArray(log.topics) ? log.topics : [];
  const topic0 = requireHex(topics[0], "log.topic0", 32);
  const blockNumber = parseHexQuantity(log.blockNumber, "log.blockNumber");
  const blockNumberString = blockNumber.toString();
  const txHash = requireHex(log.transactionHash, "log.transactionHash", 32);
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
    id: `${chainId}:${txHash}:${logIndex}`,
    chain_id: chainId,
    block_number: blockNumberString,
    log_index: logIndex,
    tx_hash: txHash,
    address,
    topic0,
    topic1: topics[1] === undefined ? null : requireHex(topics[1], "log.topic1", 32),
    topic2: topics[2] === undefined ? null : requireHex(topics[2], "log.topic2", 32),
    topic3: topics[3] === undefined ? null : requireHex(topics[3], "log.topic3", 32),
    data: requireHexData(log.data ?? "0x", "log.data"),
    block_timestamp: blockTimestamp,
  };
}

async function insertRawLog(row: RawLogRow) {
  const result = await pool.query(
    `
    INSERT INTO ${rawLogTable} (
      id,
      chain_id,
      block_number,
      log_index,
      tx_hash,
      address,
      topic0,
      topic1,
      topic2,
      topic3,
      data,
      block_timestamp
    )
    VALUES (
      $1,
      $2,
      $3::numeric,
      $4,
      $5,
      $6,
      $7,
      $8,
      $9,
      $10,
      $11,
      $12::numeric
    )
    ON CONFLICT (id) DO NOTHING
    `,
    [
      row.id,
      row.chain_id,
      row.block_number,
      row.log_index,
      row.tx_hash,
      row.address,
      row.topic0,
      row.topic1,
      row.topic2,
      row.topic3,
      row.data,
      row.block_timestamp,
    ],
  );
  return (result.rowCount ?? 0) > 0;
}

async function ensureRawLogTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${rawLogTable} (
      id text PRIMARY KEY,
      chain_id integer NOT NULL,
      block_number numeric NOT NULL,
      log_index integer NOT NULL,
      tx_hash text NOT NULL,
      address text NOT NULL,
      topic0 text NOT NULL,
      topic1 text,
      topic2 text,
      topic3 text,
      data text NOT NULL,
      block_timestamp numeric NOT NULL
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS raw_log_tx_log_idx
      ON ${rawLogTable} (chain_id, tx_hash, log_index)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS raw_log_address_topic_block_idx
      ON ${rawLogTable} (chain_id, address, topic0, block_number)
  `);
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

function sendWebSocketJson(socket: WebSocket, value: unknown) {
  if (socket.readyState !== WebSocket.OPEN) return false;
  if (socket.bufferedAmount > WS_SLOW_SUBSCRIBER_BUFFER_BYTES) {
    subscriptions.delete(socket);
    socket.close(1013, "subscriber too slow");
    return false;
  }
  socket.send(JSON.stringify(value), (error) => {
    if (error) {
      subscriptions.delete(socket);
      socket.close(1011, "send failed");
    }
  });
  return true;
}

void ensureRawLogTable().catch((error) => {
  console.warn("failed to ensure raw log table", error);
});
startWebSocketUpgradeHandler();

export default app;
