import json, os, subprocess, sys, tempfile, unittest, stat
HERE = os.path.dirname(os.path.abspath(__file__))
NH = os.path.join(HERE, "..", "nh-delta")

def item(n, title, author, created, updated, pr=False, state="open", merged=None):
    d = {"number": n, "title": title, "user": {"login": author}, "state": state, "created_at": created,
         "updated_at": updated, "closed_at": None, "html_url": f"https://github.com/nuthatch-org/r/{n}"}
    if pr:
        d["pull_request"] = {"merged_at": merged}
    return d

SC = {
    "orgs/nuthatch-org/repos": {"etag": '"o"', "body": [{"full_name": "nuthatch-org/r", "archived": False}]},
    "repos/nuthatch-org/r/issues": {"etag": '"i"', "body": [
        item(1, "human issue", "alice", "2026-11-01T00:00:00Z", "2026-11-19T01:00:00Z"),
        item(2, "bump x", "dependabot[bot]", "2026-11-19T00:30:00Z", "2026-11-19T00:40:00Z", pr=True),
        item(3, "old", "bob", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")]},
    "repos/nuthatch-org/r/issues/1/comments": {"body": [
        {"user": {"login": "carol"}, "created_at": "2026-11-19T01:00:00Z"},
        {"user": {"login": "dependabot[bot]"}, "created_at": "2026-11-19T01:00:00Z"}]},
    "repos/nuthatch-org/r/pulls/2": {"body": {"head": {"sha": "abc"}}},
    "repos/nuthatch-org/r/commits/abc/check-runs": {"body": {"check_runs": [{"status": "completed", "conclusion": "failure"}]}},
    "repos/nuthatch-org/r/issues/2": {"body": item(2, "bump x", "dependabot[bot]", "2026-11-19T00:30:00Z", "2026-11-19T00:40:00Z", pr=True)},
    "repos/nuthatch-org/r/issues/*": {"body": []},
    "repos/nuthatch-org/r/pulls/*": {"body": []},
    "search/issues": {"status": 403, "body": {"message": "forbidden"}},
    "notifications": {"status": 403, "body": {"message": "no"}},
}

class T(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        os.makedirs(self.d + "/bin")
        os.symlink(os.path.join(HERE, "fake_gh.py"), self.d + "/bin/gh")
        self.sc = self.d + "/sc.json"
        with open(self.sc, "w") as f:
            json.dump(SC, f)

    def run_nh(self, *args):
        env = dict(os.environ, PATH=self.d + "/bin:" + os.environ["PATH"], FAKE_GH_SCENARIO=self.sc,
                   NH_DELTA_STATE_DIR=self.d + "/state")
        return subprocess.run([sys.executable, NH, *args], capture_output=True, text=True, env=env)

    def test_digest_bots_and_graceful_403(self):
        p = self.run_nh("--since", "2026-11-19T00:00:00Z", "--dry")
        self.assertEqual(p.returncode, 0, p.stderr)
        o = p.stdout
        self.assertIn('r#1 issue "human issue" @alice — 1 new comment by @carol', o)
        self.assertIn("bots: 1 dependabot[bot] items", o)
        self.assertNotIn("r#2 PR", o); self.assertNotIn("r#3", o)
        self.assertIn("search unavailable (HTTP 403)", o)
        self.assertIn("notifications: skipped (HTTP 403)", o)
        self.assertFalse(os.path.exists(self.d + "/state/state.json"))  # --dry
        self.assertLess(len(o.splitlines()), 20)

    def test_state_advances_and_empty_digest(self):
        self.assertEqual(self.run_nh("--since", "2026-11-19T00:00:00Z").returncode, 0)
        with open(self.d + "/state/state.json") as f:
            st = json.load(f)
        self.assertIn("last_run", st)
        o = self.run_nh().stdout   # ETags now match -> 304 -> nothing new
        self.assertIn("nothing new since " + st["last_run"], o)

    def test_since_forms_and_bad_input(self):
        for v in ("2026-11-19", "2026-11-19T00:00:00+02:00", "2026-11-19T00:00:00"):
            self.assertEqual(self.run_nh("--since", v, "--dry").returncode, 0, v)
        p = self.run_nh("--since", "yesterday", "--dry")
        self.assertEqual(p.returncode, 2); self.assertNotIn("Traceback", p.stderr)

    def test_json(self):
        p = self.run_nh("--since", "2026-11-19T00:00:00Z", "--dry", "--json")
        d = json.loads(p.stdout)
        self.assertEqual({e["number"] for e in d["org"]}, {1, 2})

if __name__ == "__main__":
    unittest.main()
