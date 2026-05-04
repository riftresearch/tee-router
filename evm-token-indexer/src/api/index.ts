import { db } from "ponder:api";
import { erc20TransferRaw } from "ponder:schema";
import { and, count, desc, eq, gte } from "ponder";
import { Hono } from "hono";
import pg from "pg";

const app = new Hono();
const chainId = Number(process.env.PONDER_CHAIN_ID);
const schemaName = process.env.DATABASE_SCHEMA ?? process.env.PONDER_SCHEMA;

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
  max: Number(process.env.EVM_TOKEN_INDEXER_API_POOL_SIZE ?? 5),
});

type WatchPayload = {
  watchId: string;
  watchTarget: string;
  tokenAddress: string;
  depositAddress: string;
  minAmount: string;
  maxAmount: string;
  requiredAmount: string;
  createdAt: string;
  updatedAt: string;
  expiresAt: string;
};

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
  Number(process.env.EVM_TOKEN_INDEXER_RAW_RETENTION_SECONDS ?? 30 * 24 * 60 * 60);

const activeWatchTable = tableName("active_deposit_watch");
const rawTransferTable = tableName("erc20_transfer_raw");
const candidateTable = tableName("detected_deposit_candidate");

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
      AND t.block_timestamp <= w.expires_at
    ON CONFLICT (id) DO NOTHING
    `,
    [chainId, nowSeconds()],
  );
};

app.put("/watches", async (c) => {
  const body = (await c.req.json()) as { watches?: WatchPayload[] };
  const watches = body.watches ?? [];
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const watchIds = watches.map((watch) => watch.watchId);
    for (const watch of watches) {
      await client.query(
        `
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
        VALUES ($1, $2, $3, $4, $5, $6::numeric, $7::numeric, $8::numeric, $9::numeric, $10::numeric, $11::numeric)
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
        `,
        [
          watch.watchId,
          watch.watchTarget,
          chainId,
          normalizeAddress(watch.tokenAddress),
          normalizeAddress(watch.depositAddress),
          watch.minAmount,
          watch.maxAmount,
          watch.requiredAmount,
          watch.createdAt,
          watch.updatedAt,
          watch.expiresAt,
        ],
      );
    }

    if (watchIds.length === 0) {
      await client.query(`DELETE FROM ${activeWatchTable} WHERE chain_id = $1`, [
        chainId,
      ]);
    } else {
      await client.query(
        `
        DELETE FROM ${activeWatchTable}
        WHERE chain_id = $1
          AND NOT (watch_id = ANY($2::text[]))
        `,
        [chainId, watchIds],
      );
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }

  await materializeCandidates();

  return c.json({ synced: watches.length });
});

app.post("/candidates/materialize", async (c) => {
  await materializeCandidates();
  return c.json({ ok: true });
});

app.get("/candidates/pending", async (c) => {
  const limit = Math.min(Number(c.req.query("limit") ?? 100), 1_000);
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
  const id = c.req.param("id");
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
  const id = c.req.param("id");
  const body = (await c.req.json().catch(() => ({}))) as { error?: string };
  await pool.query(
    `
    UPDATE ${candidateTable}
    SET status = 'pending',
        attempt_count = attempt_count + 1,
        last_error = $3
    WHERE chain_id = $1
      AND id = $2
    `,
    [chainId, id, body.error ?? null],
  );

  return c.json({ ok: true });
});

app.post("/maintenance/prune-raw", async (c) => {
  const body = (await c.req.json().catch(() => ({}))) as {
    retentionSeconds?: number;
  };
  const retentionSeconds = body.retentionSeconds ?? rawRetentionSeconds();
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
  const address = normalizeAddress(c.req.param("address")) as `0x${string}`;
  const token = c.req.query("token");
  const page = parseInt(c.req.query("page") || "1");
  const limit = 50;
  const offset = (page - 1) * limit;
  const minAmount = c.req.query("amount");

  const conditions = [
    eq(erc20TransferRaw.chainId, chainId),
    eq(erc20TransferRaw.toAddress, address),
  ];
  if (token) {
    conditions.push(
      eq(erc20TransferRaw.tokenAddress, normalizeAddress(token) as `0x${string}`),
    );
  }
  if (minAmount) {
    conditions.push(gte(erc20TransferRaw.amount, BigInt(minAmount)));
  }
  const whereCondition =
    conditions.length > 1 ? and(...conditions) : conditions[0];

  const [transfers, countResult] = await Promise.all([
    db
      .select()
      .from(erc20TransferRaw)
      .where(whereCondition)
      .orderBy(desc(erc20TransferRaw.blockNumber))
      .limit(limit)
      .offset(offset),
    db.select({ total: count() }).from(erc20TransferRaw).where(whereCondition),
  ]);

  const total = countResult[0]?.total ?? 0;

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
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
    },
  });
});

export default app;
