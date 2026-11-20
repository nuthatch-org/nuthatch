# Agent prompts for nest pharos-eth (NEST_URL)

Each works against the nest's SQL endpoint; the agent should read the schema first (llms.txt / `/schema`).

1. Top holders: "Using the Nuthatch nest at NEST_URL, read its schema, then list the top 10 holders of the
   LP token 0xed279fdd11ca84beef15af5d39bb4d4bee23f0ca by balance with each one's percent of total supply.
   Flag any holder above 5 percent."

2. Supply check: "Using the nest at NEST_URL, replay all Transfer rows of LP token
   0x3041cbd36888becc7bbcbc0045e3b1f144466f5f (sum of value received minus sum sent per address) and
   show that the non-zero balances plus the 0x0 balance equal totalSupply. Show the SQL you used."

3. Mint and burn history: "Using the nest at NEST_URL, show by month how many LP tokens were minted
   (from = 0x0) and burned (to = 0x0) for 0xed279fdd11ca84beef15af5d39bb4d4bee23f0ca, and the month with the
   largest net change."
