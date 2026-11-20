#!/usr/bin/env python3
"""Regenerates fixtures/*.log (synthetic but realistic tool output)."""
import os
d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
os.makedirs(d, exist_ok=True)
w = lambda n, s: open(os.path.join(d, n), "w").write(s)

# cargo build: 240 Compiling lines, one warning, one leaked token
L = ["\x1b[1m\x1b[92m   Compiling\x1b[0m crate%03d v0.%d.%d" % (i, i % 7, i % 13) for i in range(240)]
L[100:100] = ["   Downloaded serde v1.0.%d" % i for i in range(30)]
L += ["\x1b[1m\x1b[33mwarning\x1b[0m: unused variable: `x`", "  --> src/main.rs:10:9", "   |", "10 |     let x = 5;",
      "   |         ^ help: prefix with underscore", "   |", "   = note: `#[warn(unused_variables)]` on by default", "",
      "    Building [=======>        ] 200/240: nuthatch", "token used: ghp_abcdefghijklmnopqrstuvwxyz0123456789",
      "    Finished dev [unoptimized + debuginfo] target(s) in 41.20s"]
w("cargo_build.log", "\n".join(L) + "\n")

# failing cargo test
L = ["   Compiling nuthatch v0.1.0", "    Finished test profile in 12.3s", "     Running unittests src/lib.rs", "", "running 120 tests"]
L += ["test mod_a::case_%03d ... ok" % i for i in range(60)]
L += ["test reorg::deep_reorg ... FAILED"]
L += ["test mod_b::case_%03d ... ok" % i for i in range(59)]
L += ["", "failures:", "", "---- reorg::deep_reorg stdout ----",
      "thread 'reorg::deep_reorg' panicked at src/reorg.rs:88:9:",
      "assertion `left == right` failed", "  left: 4", " right: 5",
      "note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace", "",
      "failures:", "    reorg::deep_reorg", "",
      "test result: FAILED. 119 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.21s",
      "error: test failed, to rerun pass `--lib`"]
w("cargo_test_fail.log", "\n".join(L) + "\n")

# gh pr checks
L = []
for i in range(40):
    L.append("test (shard %d)\tpass\t%dm%ds\thttps://github.com/nuthatch-org/nuthatch/actions/runs/123/job/%d" % (i, i % 9, i % 59, 9000 + i))
L.insert(25, "bench-rss\tfail\t6m2s\thttps://github.com/nuthatch-org/nuthatch/actions/runs/123/job/8999")
L += ["clippy\tpass\t2m1s\thttps://github.com/nuthatch-org/nuthatch/actions/runs/123/job/7001",
      "deny\tpending\t0\thttps://github.com/nuthatch-org/nuthatch/actions/runs/123/job/7002"]
w("gh_pr_checks.log", "\n".join(L) + "\n")

# npm ci with spinner redraws and progress
L = ["⠋\r⠙\r⠹\r⠸\rnpm warn deprecated inflight@1.0.6: This module is not supported",
     "npm warn deprecated glob@7.2.3: Glob versions prior to v9 are no longer supported"]
L += ["npm http fetch GET 200 https://registry.npmjs.org/pkg-%03d %dms" % (i, 20 + i % 40) for i in range(150)]
L += ["[##########..........] \\ idealTree:foo: sill placeBep", "[####################] reify:bar",
      "", "added 612 packages, and audited 613 packages in 14s", "", "98 packages are looking for funding",
      "  run `npm fund` for details", "", "2 high severity vulnerabilities", "",
      "npm ERR! code E403", "npm ERR! 403 Forbidden - GET https://registry.npmjs.org/private-pkg",
      "Authorization: Bearer abcdef1234567890abcdef"]
w("npm_ci.log", "\n".join(L) + "\n")
