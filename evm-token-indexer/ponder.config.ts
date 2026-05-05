import { createConfig } from "ponder";
import { erc20ABI } from "./abis/erc20ABI";

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

const parseNonNegativeInteger = (
  value: string | undefined,
  name: string,
  defaultValue: number,
  max: number,
) => {
  const raw = value ?? defaultValue.toString();
  if (!/^\d+$/.test(raw)) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed < 0 || parsed > max) {
    throw new Error(`${name} must be a non-negative integer <= ${max}`);
  }
  return parsed;
};

export default createConfig({
  database: process.env.DATABASE_URL
    ? {
        kind: "postgres",
        connectionString: process.env.DATABASE_URL,
      }
    : undefined,
  chains: {
    evm: {
      id: parsePositiveInteger(
        process.env.PONDER_CHAIN_ID,
        "PONDER_CHAIN_ID",
        undefined,
        Number.MAX_SAFE_INTEGER,
      ),
      rpc: process.env.PONDER_RPC_URL_HTTP,
      ws: process.env.PONDER_WS_URL_HTTP,
      disableCache: process.env.PONDER_DISABLE_CACHE === "true",
    },
  },
  contracts: {
    erc20: {
      abi: erc20ABI,
      chain: {
        evm: {
          startBlock: parseNonNegativeInteger(
            process.env.PONDER_CONTRACT_START_BLOCK,
            "PONDER_CONTRACT_START_BLOCK",
            0,
            Number.MAX_SAFE_INTEGER,
          ),
        },
      },
    },
  },
});
