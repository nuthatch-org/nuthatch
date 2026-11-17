import { createConfig } from "ponder";

import { WethAbi } from "./abis/WethAbi";

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1!,
    },
  },
  contracts: {
    WETH: {
      chain: "mainnet",
      abi: WethAbi,
      address: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
      startBlock: 26400000,
      endBlock: 26400999,
    },
  },
});
