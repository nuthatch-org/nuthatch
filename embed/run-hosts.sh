#!/usr/bin/env bash
# RFC-0077 S2 to S5 and RFC-0081: the plain module in target/plain (`yatr plain-module`) in Node,
# wasmtime, Chrome and Firefox, each held to the node's pins and to each other. Logs land beside it.
# Needs CHROME, FIREFOX and GECKODRIVER, as .github/actions/test-browsers exports them.
set -euo pipefail
cd "$(dirname "$0")"
out=target/plain
# Before the trap: bash 3.2 exits 0 from an EXIT trap after an unbound variable.
chrome=$CHROME firefox=$FIREFOX gecko=(--geckodriver "$GECKODRIVER")
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
rpc=../tools/fixture-rpc/target/release/fixture-rpc
$rpc recorded --port 8953 --file ../tests/fixtures/tab-parity/chain.json.gz &
$rpc recorded --fork --port 8954 --file ../tests/fixtures/tab-parity/chain.json.gz &
for p in 8953 8954; do
  for _ in $(seq 50); do curl -sf -X POST -d '{"id":1,"method":"eth_chainId"}' http://127.0.0.1:$p/ && break; sleep 0.2; done
done
m="$out/plain.wasm" d=../tests/fixtures/tab-parity host=tests/wasmtime-host/target/release/wasmtime-host
cache=(--cache "$out/cwasm")
node hosts/node/run.mjs "$m" http://127.0.0.1:8953/ $d s3 | tee "$out/node.log"
node hosts/node/run.mjs "$m" http://127.0.0.1:8954/ $d s5 | tee -a "$out/node.log"
node hosts/node/run.mjs "$m" http://127.0.0.1:8954/ $d s5 --storage bytes | tee -a "$out/node.log"
node hosts/node/run.mjs "$m" http://127.0.0.1:8953/ $d s3 --restart | tee -a "$out/node.log"
node hosts/node/run.mjs "$m" http://127.0.0.1:8954/ $d s5 --restart | tee -a "$out/node.log"
$host run "$m" http://127.0.0.1:8953/ $d s3 "${cache[@]}" | tee "$out/wasmtime.log"
$host run "$m" http://127.0.0.1:8954/ $d s5 "${cache[@]}" | tee -a "$out/wasmtime.log"
$host run "$m" http://127.0.0.1:8954/ $d s5 --storage bytes "${cache[@]}" | tee -a "$out/wasmtime.log"
$host run "$m" http://127.0.0.1:8953/ $d s3 --restart "${cache[@]}" | tee -a "$out/wasmtime.log"
$host run "$m" http://127.0.0.1:8954/ $d s5 --restart "${cache[@]}" | tee -a "$out/wasmtime.log"
node run-sw.mjs "$chrome" "$m" http://127.0.0.1:8954/ $d | tee "$out/chrome.log"
node run-sw.mjs "$chrome" "$m" http://127.0.0.1:8954/ $d --restart | tee -a "$out/chrome.log"
# RFC-0081 S5: the same legs in Firefox under geckodriver, both pinned; then RFC-0077 S5's nest
# shapes in both browsers, each in a nest worker on OPFS.
node run-sw.mjs "$firefox" "$m" http://127.0.0.1:8954/ $d "${gecko[@]}" | tee "$out/firefox.log"
node run-sw.mjs "$firefox" "$m" http://127.0.0.1:8954/ $d --restart "${gecko[@]}" | tee -a "$out/firefox.log"
node run-sw.mjs "$chrome" "$m" shapes $rpc ../tests/fixtures/embed-shapes | tee -a "$out/chrome.log"
node run-sw.mjs "$firefox" "$m" shapes $rpc ../tests/fixtures/embed-shapes "${gecko[@]}" | tee -a "$out/firefox.log"
node hosts/agree.mjs "$m" "$out/node.log" "$out/wasmtime.log" "$out/chrome.log" \
  "$out/firefox.log" --hosts node,wasmtime,chrome,firefox | tee "$out/agree.log"
# RFC-0077 S4: S4's entity nest hydrated from a published directory, at the seed's head and
# followed to the tip, equal to the node's pins (`tests/e2e_tab_mirror.rs`); refused at
# 600 MiB before any download; a postcard checked, and an altered one refused.
n4=$d/s4-nest mm=$d/s4-mirror ds=6eb46815c819ffdf1a6e06e870394eb5a4173fb55471fdfd9e3c83d79da8fdaa
node hosts/node/mirror.mjs "$m" $n4 $mm --expect $d/s4-mirror-seed.json | tee "$out/mirror.log"
node hosts/node/mirror.mjs "$m" $n4 $mm --follow --rpc http://127.0.0.1:8953/ --expect $d/s4-mirror-tip.json \
  | tee -a "$out/mirror.log"
node hosts/node/mirror.mjs "$m" $n4 $mm --memory-mib 600 --max-rows 150000 --refused | tee -a "$out/mirror.log"
node hosts/node/mirror.mjs "$m" $n4 $mm --postcard made --dataset $ds | tee -a "$out/mirror.log"
node hosts/node/mirror.mjs "$m" $n4 $mm --postcard altered --dataset $ds --refused | tee -a "$out/mirror.log"
node run-sw-mirror.mjs "$chrome" "$m" $n4 $mm --expect $d/s4-mirror-seed.json | tee -a "$out/mirror.log"
node run-sw-mirror.mjs "$chrome" "$m" $n4 $mm --rpc http://127.0.0.1:8953/ --expect $d/s4-mirror-tip.json \
  | tee -a "$out/mirror.log"
node run-sw-mirror.mjs "$chrome" "$m" $n4 $mm --refused --memory 600 --max-rows 150000 | tee -a "$out/mirror.log"
# RFC-0081 S1: the same nest `mirror+rpc`, equal to `rpc` under the same filters; no GET names
# or is chosen by a filter's value; a private filter refused; another nest's answers attached.
node hosts/node/hybrid-gates.mjs "$m" $n4 $mm http://127.0.0.1:8953/ | tee -a "$out/mirror.log"
# RFC-0081 S5: the wasmtime host's `get`: the same nest from the same mirror, at the seed's head
# and followed, equal to the node's pins; and `mirror+rpc`, equal to the Node host's answers.
wt=(--wasmtime $host "${cache[@]}") hy=(--follow --rpc http://127.0.0.1:8953/ --hybrid 0x37c9ef5d0801abdc25d9bbed096a080fa3c54e4c)
node hosts/node/mirror.mjs "$m" $n4 $mm --expect $d/s4-mirror-seed.json "${wt[@]}" | tee -a "$out/mirror.log"
node hosts/node/mirror.mjs "$m" $n4 $mm --follow --rpc http://127.0.0.1:8953/ --expect $d/s4-mirror-tip.json "${wt[@]}" \
  | tee -a "$out/mirror.log"
node hosts/node/mirror.mjs "$m" $n4 $mm "${hy[@]}" --out "$out/hybrid-node.json" | tee -a "$out/mirror.log"
node hosts/node/mirror.mjs "$m" $n4 $mm "${hy[@]}" --expect "$out/hybrid-node.json" "${wt[@]}" \
  | tee -a "$out/mirror.log"
# RFC-0081 S2: the host package as released, on the same module: S5's answers fresh and from
# its kept store; a mirrored nest reopened from its pack with no GET, an altered pack fetched again.
node --test hosts/package/verify.test.mjs | tee -a "$out/mirror.log"
node hosts/package/build.mjs "$out/nuthatch-engine.js" | tee -a "$out/mirror.log"
node hosts/package/run.mjs "$out/nuthatch-engine.js" "$m" s5 http://127.0.0.1:8954/ $d | tee -a "$out/mirror.log"
node hosts/package/run.mjs "$out/nuthatch-engine.js" "$m" reuse $n4 $mm | tee -a "$out/mirror.log"
# RFC-0081 S3: `verify` on the same nest: an answer within the filters equal, one value altered
# named, a statement over the whole table outside the filters, another NID refused before any GET.
node hosts/package/verify-fixture.mjs "$out/nuthatch-engine.js" "$m" $n4 $mm http://127.0.0.1:8953/ \
  | tee -a "$out/mirror.log"
# RFC-0081 S5: the wasmtime host's verified head from Helios's consensus core, on a recorded
# light client run that finalizes block 26,243,710: S3's nest seals under it as the node does,
# and the same run with its finalized block altered is refused and seals nothing.
gunzip -c $d/headers.json.gz $d/helios-headers.json.gz | jq -c -s add | gzip -c > "$out/helios-headers.json.gz"
$rpc recorded --port 8960 --file $d/chain.json.gz --headers "$out/helios-headers.json.gz" &
for _ in $(seq 50); do curl -sf -X POST -d '{"id":1,"method":"eth_chainId"}' http://127.0.0.1:8960/ && break; sleep 0.2; done
$host run "$m" http://127.0.0.1:8960/ $d s3 --helios $d/helios.json.gz "${cache[@]}" | tee "$out/helios.log"
$host run "$m" http://127.0.0.1:8960/ $d s3 --helios $d/helios.json.gz --forge "${cache[@]}" | tee -a "$out/helios.log"
# RFC-0077 S5: with a verified head, a forged or missing header or a log off the verified
# chain stops sealing by name, and the honest head seals the node's segment; then every nest
# shape `tests/e2e_embed_shapes.rs` writes out of the binary's tests, equal to its pins.
$rpc recorded --port 8955 --file $d/chain.json.gz --headers $d/headers.json.gz &
for _ in $(seq 50); do curl -sf -X POST -d '{"id":1,"method":"eth_chainId"}' http://127.0.0.1:8955/ && break; sleep 0.2; done
node hosts/node/verified.mjs "$m" http://127.0.0.1:8955/ $d | tee "$out/verified.log"
# RFC-0081 S4: completeness to block 26,239,840 on the receipts that range needs. A log left
# out by the RPC is found from its block's receipts; forged receipts stop sealing by name.
r4="--file $d/chain.json.gz --headers $d/headers.json.gz --receipts $d/receipts.json.gz"
$rpc recorded --port 8956 $r4 &
$rpc recorded --port 8957 $r4 --drop-log 26239830:315 &
$rpc recorded --port 8958 $r4 --drop-log 26239830:315 --forge-receipts 26239830 &
for p in 8956 8957 8958; do
  for _ in $(seq 50); do curl -sf -X POST -d '{"id":1,"method":"eth_chainId"}' http://127.0.0.1:$p/ && break; sleep 0.2; done
done
node hosts/node/complete.mjs "$m" $d http://127.0.0.1:8956/ http://127.0.0.1:8957/ http://127.0.0.1:8958/ \
  26239830 --until 26239840 | tee -a "$out/verified.log"
node hosts/node/shapes.mjs "$m" $rpc ../tests/fixtures/embed-shapes | tee "$out/shapes.log"
