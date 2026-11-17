# MiCA Art 22 figures over EURC

A nest that computes the four figures MiCA Art 22(1) asks of a token issuer each quarter, from the
token's own Transfer events, with each deduction the EBA's RTS makes (Commission Delegated
Regulation (EU) 2025/298) as its own named view. It is a worked example over EURC on Ethereum.

This is a deterministic annotation layer, not a regulatory opinion. Which addresses are a CASP's,
whose they are and where their holders live are inputs the issuer supplies; the chain cannot.

**Scope.** Art 22 binds asset-referenced tokens, and through Art 58(3) e-money tokens in a currency
that is not an official currency of a Member State. EURC is in euros, so for EURC these figures are
illustrative. For USDC the views are the same over its contract, with one step added: RTS Art
4(3)(c) values each day's transactions at that day's closing exchange rate into the home Member
State's currency. The `fiat_threshold` recipe shows the shape: a dated rate table imported with
`nuthatch offchain drop`, joined on the day.

## The figures

| Art 22(1) | View | Read it |
|---|---|---|
| (a) holders | `fiat_token_v2_2_holder_count` | at the quarter's last second, `at_time` |
| (b) value issued | `fiat_token_v2_2_outstanding` | the same; supply less frozen and `treasury` balances (needs #2963 to read `labels` at a time) |
| (c) transactions per day | `art22_quarter_transactions` | quarterly averages, over every calendar day |
| (d) means of exchange per day | `art22_quarter_means_of_exchange` | the same, per single currency area |

`art22_quarter_deductions` gives each deduction on its own line. A transfer can meet several, so the
lines overlap; (d) removes each transfer once. The size of the reserve, also under (b), is not on
the chain.

| Deduction | View | RTS |
|---|---|---|
| exchanged with the issuer or a CASP, mints and burns included | `art22_ded_exchange` | Art 3(1)(a) |
| collateral for transactions in financial instruments | `art22_ded_collateral` | Art 3(1)(b) |
| settling a derivative | `art22_ded_derivative` | Art 3(1)(c) |
| other transactions that are not payments | `art22_ded_not_payment` | Art 3(1)(d) |
| between one person's addresses | `art22_ded_same_person` | Art 3(4) |
| no custodial wallet on either side | `art22_ded_non_custodial` | recital 2 |

## What the issuer supplies

One label set, imported with `nuthatch labels import`. Each row is `address,label`, and an address
may carry several labels.

- `issuer` and `casp`: addresses where the issuer or a CASP is the counterparty to an exchange.
- `custodial`: client wallets a CASP holds. A transfer with none on either side is outside (d).
- `collateral`, `derivative`, `not_payment`: addresses whose transfers the issuer can show are
  those uses. Art 3(1)(d) needs the issuer to be able to demonstrate it on request.
- `person:<id>`: one person's addresses. The chain shows only an address paying itself.
- `area:<currency>`: where the holder lives, from the CASP's report under Art 22(3). A transfer
  counts in an area only when both parties are labelled in it (Art 3(5)); the rest is
  `unassigned`.

An address given two persons or two areas gets neither, so nothing is deducted or assigned on a
guess. The label set is the one imported now, not as it stood at the quarter's end, so keep the
set the report used.

Days are UTC. If the issuer closes its day elsewhere, change the `day` expression in
`10-art22_transfers.sql`.

## Run it

```sh
nuthatch dev --dir examples/mica-art22 --rpc https://your-mainnet-rpc
nuthatch labels import --dir examples/mica-art22 examples/mica-art22/labels-illustrative.csv
nuthatch sql --dir examples/mica-art22 "SELECT * FROM art22_quarter_means_of_exchange WHERE year = 2026 AND quarter = 3"
```

The backfill runs from EURC's deployment, block 14,807,233, since (a) and (b) need every balance.

## Q3 2026, measured

On 2026-11-16, over a nest at block 26,427,997. The labels are `labels-illustrative.csv`: Circle's
16 public addresses as `issuer`, and 284 addresses of Coinbase, Kraken, OKX, Bitvavo, Gemini and
Bitstamp as `custodial`. They come from [eth-labels](https://github.com/dawsbot/eth-labels) at
d9b21ae (MIT, `LICENSE-eth-labels`). They carry no `area` labels, so all of (d) is `unassigned`.

The quarter ends 2026-09-30 23:59:59 UTC, which `/at` places at block 26,093,723. (a) and (b) were
read there with `at_time=1790812799`; (c) and (d) take every Q3 transfer, so the nest's tip does not
move them. The runtime predated #2963, so (b) is total supply less frozen balances at that
block, without the `treasury` term; the illustrative set labels no treasury, so it would be
zero.

| Figure | Value |
|---|---|
| (a) holders | 26,278 |
| (b) EURC issued on Ethereum | 303,636,786.016376 (478 blacklisted accounts, holding 0) |
| (c) transactions | 282,630 in 92 days: 3,072.06 a day, EUR 76,249,658.740656 a day |
| (d) means of exchange | 17,442: 189.58 a day, EUR 13,603,678.677389 a day, all `unassigned` |

| Deduction | Transactions | EURC |
|---|---|---|
| exchange | 4,156 | 1,646,973,161.138064 |
| non_custodial | 265,113 | 5,751,381,085.116353 |
| same_person | 113 | 1,382,040.304445 |

(b) is Ethereum alone. Circle's September 2026 report gives 412,549,121 EURC in circulation across
nine chains, net of tokens allowed but not issued on Solana.

Supply on this nest matched Circle's December 2022 report to the token, when Ethereum was EURC's
only chain: 24,905,662 (#2943).
