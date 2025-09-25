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

REQUIRED_FILES = ("src.json", "etl.json")
OPTIONAL_DST = "dst.json"
OPTIONAL_EXPECT = "expect.json"
OPTIONAL_OPTIONS = "options.json"  # e.g. {"delimiter": " | "}

def _list_case_dirs():
    if not os.path.isdir(EXAMPLES_DIR):
        return []
    # Only immediate subdirectories (ignore hidden/OS files)
    return sorted(
        d for d in os.listdir(EXAMPLES_DIR)
        if os.path.isdir(os.path.join(EXAMPLES_DIR, d)) and not d.startswith(".")
    )

class FileLevelETLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(MAIN_SCRIPT):
            raise unittest.SkipTest(f"main script not found at {MAIN_SCRIPT}")
        if not os.path.isdir(EXAMPLES_DIR):
            raise unittest.SkipTest(f"examples dir not found at {EXAMPLES_DIR}")

    def _discover_cases_or_fail(self):
        case_dirs = _list_case_dirs()
        self.assertTrue(case_dirs, f"No subdirectories found under {EXAMPLES_DIR} — add at least one test case folder.")
        self.maxDiff = None

        cases = []
        missing = []  # collect missing required files per case dir

        for entry in case_dirs:
            case_dir = os.path.join(EXAMPLES_DIR, entry)

            # required files must exist; if not, record error for this dir
            req_missing = []
            src_path = os.path.join(case_dir, "src.json")
            etl_path = os.path.join(case_dir, "etl.json")
            if not os.path.exists(src_path): req_missing.append("src.json")
            if not os.path.exists(etl_path): req_missing.append("etl.json")

            if req_missing:
                missing.append((entry, req_missing))
                # still append a stub so count alignment check can report properly
                continue

            # optional files
            dst_path = os.path.join(case_dir, OPTIONAL_DST)
            expect_path = os.path.join(case_dir, OPTIONAL_EXPECT)
            options_path = os.path.join(case_dir, OPTIONAL_OPTIONS)

            options = {}
            if os.path.exists(options_path):
                with open(options_path, "r", encoding="utf-8") as f:
                    options = json.load(f)

            cases.append({
                "name": entry,
                "dir": case_dir,
                "src": src_path,
                "etl": etl_path,
                "dst": dst_path if os.path.exists(dst_path) else None,
                "expect": expect_path if os.path.exists(expect_path) else None,
                "options": options,
            })

        # Assert that every subdirectory is runnable; if not, show detailed reasons.
        if missing:
            msgs = []
            for entry, req_missing in missing:
                msgs.append(f"- {entry}: missing {', '.join(req_missing)}")
            self.fail(
                "Some example case directories are missing required files:\n" +
                "\n".join(msgs) +
                f"\n\nEach case folder must contain: {', '.join(REQUIRED_FILES)}"
            )

        # Also assert count alignment (belt-and-suspenders)
        self.assertEqual(
            len(cases), len(case_dirs),
            f"Discovered {len(cases)} runnable cases but found {len(case_dirs)} subdirectories. "
            "Every subdirectory is expected to be a runnable case."
        )

        return cases

    def test_examples(self):
        cases = self._discover_cases_or_fail()
        for case in cases:
            with self.subTest(case=case["name"]):
                with tempfile.TemporaryDirectory(prefix=f"etl_{case['name']}_") as tmpdir:
                    tmp_dst = os.path.join(tmpdir, "dst.json")
                    tmp_out = os.path.join(tmpdir, "out.json")

                    # If the case has a dst.json, copy it; else leave tmp_dst absent so the app creates {}
                    if case["dst"] is not None:
                        shutil.copyfile(case["dst"], tmp_dst)
                    # Build command
                    cmd = [
                        sys.executable,
                        MAIN_SCRIPT,
                        "--etl", case["etl"],
                        "--src", case["src"],
                        "--dst", tmp_dst,   # always pass a path; may not exist yet
                        "--out", tmp_out,
                    ]
                    # Optional per-case options (currently supports --delimiter)
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

                    # Validate output exists & parse
                    with open(tmp_out, "r", encoding="utf-8") as f:
                        got = json.load(f)

                    # Compare to expect.json if present
                    if case["expect"] is not None:
                        with open(case["expect"], "r", encoding="utf-8") as f:
                            expected = json.load(f)
                        self.assertEqual(got, expected, f"Output mismatch for case '{case['name']}'")
                    else:
                        self.assertTrue(isinstance(got, (dict, list)), "Output should be object or array")

if __name__ == "__main__":
    unittest.main()
