import { ponder } from "ponder:registry";
import { erc20TransferRaw } from "ponder:schema";
import pg from "pg";

const chainId = Number(process.env.PONDER_CHAIN_ID);
const schemaName = process.env.DATABASE_SCHEMA ?? process.env.PONDER_SCHEMA;
const databaseUrl = process.env.DATABASE_URL;

if (!schemaName) {
  throw new Error("DATABASE_SCHEMA or PONDER_SCHEMA must be set for candidate materialization");
}

if (!databaseUrl) {
  throw new Error("DATABASE_URL must be set for candidate materialization");
}

const pool = new pg.Pool({
  connectionString: databaseUrl,
  application_name: "evm-token-indexer-indexing",
  max: Number(process.env.EVM_TOKEN_INDEXER_INDEXING_POOL_SIZE ?? 5),
});

const normalizeAddress = (address: string) =>
  address.toLowerCase() as `0x${string}`;

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
const nowSeconds = () => Math.floor(Date.now() / 1000).toString();

ponder.on("erc20:Transfer", async ({ event, context }) => {
  const tokenAddress = normalizeAddress(event.log.address);
  const fromAddress = normalizeAddress(event.args.from);
  const toAddress = normalizeAddress(event.args.to);
  const amount = event.args.amount;
  const blockNumber = BigInt(event.block.number);
  const blockTimestamp = BigInt(event.block.timestamp);
  const transferIndex = Number(event.log.logIndex);
  const transactionHash = event.transaction.hash;
  const blockHash = event.block.hash;

  await context.db
    .insert(erc20TransferRaw)
    .values({
      id: event.id,
      chainId,
      tokenAddress,
      fromAddress,
      toAddress,
      amount,
      blockNumber,
      blockHash,
      transactionHash,
      logIndex: transferIndex,
      blockTimestamp,
    })
    .onConflictDoNothing();

  await pool.query(
    `
    INSERT INTO ${candidateTable} (
      id,
      watch_id,
      watch_target,
      chain_id,
      token_address,
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
      $4 AS deposit_address,
      $5::numeric AS amount,
      w.required_amount,
      $6 AS transaction_hash,
      $7 AS transfer_index,
      $8::numeric AS block_number,
      $9 AS block_hash,
      $10::numeric AS block_timestamp,
      'pending' AS status,
      0 AS attempt_count,
      $11::numeric AS created_at
    FROM ${activeWatchTable} w
    WHERE w.chain_id = $1
      AND w.token_address = $3
      AND w.deposit_address = $4
      AND $5::numeric >= w.min_amount
      AND $5::numeric <= w.max_amount
      AND $10::numeric <= w.expires_at
    ON CONFLICT (id) DO NOTHING
    `,
    [
      chainId,
      event.id,
      tokenAddress,
      toAddress,
      amount.toString(),
      transactionHash,
      transferIndex,
      blockNumber.toString(),
      blockHash,
      blockTimestamp.toString(),
      nowSeconds(),
    ],
  );
});
