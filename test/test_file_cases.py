# test/test_file_cases.py
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MAIN_SCRIPT = os.path.join(PROJECT_ROOT, "main.py")
EXAMPLES_DIR = os.path.join(os.path.dirname(__file__), "examples")

REQUIRED_FILES = ("src.json", "etl.json", "dst.json")
OPTIONAL_EXPECT = "expect.json"
OPTIONAL_OPTIONS = "options.json"  # optional per-case options, e.g. {"delimiter": " | "}

class FileLevelETLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(MAIN_SCRIPT):
            raise unittest.SkipTest(f"main script not found at {MAIN_SCRIPT}")
        if not os.path.isdir(EXAMPLES_DIR):
            raise unittest.SkipTest(f"examples dir not found at {EXAMPLES_DIR}")

    def _discover_cases(self):
        cases = []
        for entry in sorted(os.listdir(EXAMPLES_DIR)):
            case_dir = os.path.join(EXAMPLES_DIR, entry)
            if not os.path.isdir(case_dir):
                continue
            req_paths = {name: os.path.join(case_dir, name) for name in REQUIRED_FILES}
            if all(os.path.exists(p) for p in req_paths.values()):
                expect_path = os.path.join(case_dir, OPTIONAL_EXPECT)
                options_path = os.path.join(case_dir, OPTIONAL_OPTIONS)
                options = {}
                if os.path.exists(options_path):
                    with open(options_path, "r", encoding="utf-8") as f:
                        options = json.load(f)
                cases.append({
                    "name": entry,
                    "dir": case_dir,
                    "src": req_paths["src.json"],
                    "etl": req_paths["etl.json"],
                    "dst": req_paths["dst.json"],
                    "expect": expect_path if os.path.exists(expect_path) else None,
                    "options": options,
                })
        return cases

    def test_examples(self):
        cases = self._discover_cases()
        self.assertTrue(len(cases) > 0, "No test cases found under test/examples/*/")
        for case in cases:
            with self.subTest(case=case["name"]):
                with tempfile.TemporaryDirectory(prefix=f"etl_{case['name']}_") as tmpdir:
                    tmp_dst = os.path.join(tmpdir, "dst.json")
                    shutil.copyfile(case["dst"], tmp_dst)
                    tmp_out = os.path.join(tmpdir, "out.json")

                    cmd = [
                        sys.executable,
                        MAIN_SCRIPT,
                        "--etl", case["etl"],
                        "--src", case["src"],
                        "--dst", tmp_dst,
                        "--out", tmp_out,
                    ]

                    # Optional per-case options (currently only delimiter supported)
                    delimiter = case["options"].get("delimiter")
                    if delimiter is not None:
                        cmd += ["--delimiter", delimiter]

                    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    if proc.returncode != 0:
                        self.fail(
                            f"ETL run failed for case '{case['name']}'\n"
                            f"CMD: {' '.join(cmd)}\n"
                            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
                        )

                    # Validate
                    with open(tmp_out, "r", encoding="utf-8") as f:
                        got = json.load(f)

                    if case["expect"] is not None:
                        with open(case["expect"], "r", encoding="utf-8") as f:
                            expected = json.load(f)
                        self.assertEqual(got, expected, f"Output mismatch for case '{case['name']}'")
                    else:
                        # At least assert the result is a dict or array (valid JSON already guaranteed)
                        self.assertTrue(isinstance(got, (dict, list)), "Output should be object or array")
