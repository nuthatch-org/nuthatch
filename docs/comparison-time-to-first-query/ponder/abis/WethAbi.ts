export const WethAbi = [
  {
    name: "Approval",
    type: "event",
    inputs: [
      { name: "src", type: "address", indexed: true },
      { name: "guy", type: "address", indexed: true },
      { name: "wad", type: "uint256", indexed: false },
    ],
    anonymous: false,
  },
  {
    name: "Transfer",
    type: "event",
    inputs: [
      { name: "src", type: "address", indexed: true },
      { name: "dst", type: "address", indexed: true },
      { name: "wad", type: "uint256", indexed: false },
    ],
    anonymous: false,
  },
  {
    name: "Deposit",
    type: "event",
    inputs: [
      { name: "dst", type: "address", indexed: true },
      { name: "wad", type: "uint256", indexed: false },
    ],
    anonymous: false,
  },
  {
    name: "Withdrawal",
    type: "event",
    inputs: [
      { name: "src", type: "address", indexed: true },
      { name: "wad", type: "uint256", indexed: false },
    ],
    anonymous: false,
  },
] as const;
