import { db } from "ponder:api";
import { erc20TransferRaw } from "ponder:schema";
import { and, desc, eq, gte } from "ponder";
import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { timingSafeEqual } from "node:crypto";
import pg from "pg";

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
const MAX_DECIMAL_STRING_LENGTH = 78;
const MAX_API_BODY_BYTES = 64 * 1024 * 1024;
const MAX_CANDIDATE_ID_LENGTH = 512;
const MAX_CANDIDATE_ERROR_LENGTH = 2_048;
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
const candidateTable = tableName("detected_deposit_candidate");

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

function parseHexAddress(value: unknown, field: string) {
  if (typeof value !== "string" || !/^0x[0-9a-fA-F]{40}$/.test(value)) {
    throw new Error(`${field} must be a 20-byte hex address`);
  }
  return normalizeAddress(value);
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

export default app;
