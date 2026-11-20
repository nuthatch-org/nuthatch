import os, subprocess, sys, unittest
T = os.path.dirname(os.path.abspath(__file__))
SQ = os.path.join(T, "..", "squeeze")
FX = os.path.join(T, "fixtures")

def run(name=None, args=(), env=None, text=None):
    e = dict(os.environ, **(env or {}))
    inp = text if text is not None else open(os.path.join(FX, name)).read()
    p = subprocess.run([sys.executable, SQ, *args], input=inp, capture_output=True, text=True, env=e)
    return p.stdout

class T(unittest.TestCase):
    def test_all_fixtures_shrink_and_keep_errors(self):
        for name, must in [("cargo_build.log", ["warning: unused variable"]),
                           ("cargo_test_fail.log", ["deep_reorg ... FAILED", "panicked at src/reorg.rs:88:9", "test result: FAILED", "error: test failed"]),
                           ("gh_pr_checks.log", ["bench-rss\tfail"]),
                           ("npm_ci.log", ["npm ERR! code E403", "403 Forbidden", "npm warn deprecated glob"])]:
            raw = open(os.path.join(FX, name)).read()
            out = run(name)
            for m in must:
                self.assertIn(m, out, f"{name}: {m}")
            self.assertLess(len(out.splitlines()), len(raw.splitlines()) // 2, name)
            self.assertLessEqual(len(out.splitlines()), 152)
            self.assertIn("[squeeze] lines in=", out)

    def test_collapse_counts(self):
        out = run("cargo_build.log")
        self.assertIn("[x100", out); self.assertIn("[x140", out); self.assertIn("[x30", out)
        self.assertNotIn("\x1b", out)
        self.assertNotIn("Building [=", out)

    def test_secrets_redacted(self):
        txt = ("a ghp_abcdefghijklmnopqrstuvwxyz0123 b github_pat_11ABCDEFG0abcdefghij_klmnop c gho_abcdefghijklmnop12 "
               "sk-ant-api03-abcdefghijklmnop Authorization: Bearer abc.def.ghi1234567 gqk_live_abcdef123456\n")
        out = run(text=txt)
        for s in ["ghp_a", "github_pat_11", "gho_a", "sk-ant", "abc.def.ghi", "gqk_live"]:
            self.assertNotIn(s, out)
        self.assertIn("REDACTED", out)

    def test_never_drop_errors_beyond_cap(self):
        txt = "".join("line %d error: thing%d broke\n" % (i, i * 7919) for i in range(300))
        out = run(text=txt, env={"SQUEEZE_MAX": "20"})
        self.assertEqual(sum("error: thing" in l for l in out.splitlines()), 300)

    def test_cap_respected(self):
        txt = "".join("unique line %d %s\n" % (i, "x" * (i % 17)) for i in range(1000))
        out = run(text=txt, env={"SQUEEZE_MAX": "50"})
        self.assertLessEqual(len(out.splitlines()), 53)
        self.assertIn("omitted", out)
        self.assertIn("unique line 0\n", out); self.assertIn("unique line 999", out)

    def test_wrapper_exit_code(self):
        for code in (0, 3):
            p = subprocess.run([sys.executable, SQ, "--", "sh", "-c", f"echo hi; echo err >&2; exit {code}"], capture_output=True, text=True)
            self.assertEqual(p.returncode, code)
            self.assertIn("hi", p.stdout); self.assertIn("exit=%d" % code, p.stdout)
        p = subprocess.run([sys.executable, SQ, "--", "no-such-cmd-xyz"], capture_output=True, text=True)
        self.assertEqual(p.returncode, 127)

if __name__ == "__main__":
    unittest.main()
