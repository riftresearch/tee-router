import { createConfig } from "ponder";
import { erc20ABI } from "./abis/erc20ABI";

export default createConfig({
  database: process.env.DATABASE_URL
    ? {
        kind: "postgres",
        connectionString: process.env.DATABASE_URL,
      }
    : undefined,
  chains: {
    evm: {
      id: Number(process.env.PONDER_CHAIN_ID),
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
          startBlock: Number(process.env.PONDER_CONTRACT_START_BLOCK ?? 0),
        },
      },
    },
  },
});
