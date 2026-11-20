#!/usr/bin/env python3
"""Fake `gh api -i ...` for offline tests. Scenario from env FAKE_GH_SCENARIO (json file)."""
import json, os, sys
sc = json.load(open(os.environ["FAKE_GH_SCENARIO"]))
a = sys.argv[1:]
path = next(x for x in a[a.index("GET") + 1:] if not x.startswith("-"))
q = ""
if "-f" in a:
    q = " ".join(a[i + 1] for i, x in enumerate(a) if x == "-f")
inm = any(x.startswith("If-None-Match") for x in a)
for key, resp in sc.items():
    if path == key or (key.endswith("*") and path.startswith(key[:-1])):
        if inm and resp.get("etag"):
            print("HTTP/2.0 304 Not Modified\r\n\r\n"); sys.exit(1)
        st = resp.get("status", 200)
        print(f"HTTP/2.0 {st} X\r\netag: {resp.get('etag','')}\r\n\r\n{json.dumps(resp.get('body', {}))}")
        sys.exit(0 if st < 400 else 1)
print('HTTP/2.0 404 Not Found\r\n\r\n{"message":"Not Found"}'); sys.exit(1)
