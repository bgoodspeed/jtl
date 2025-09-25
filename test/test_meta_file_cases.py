# test/test_meta_file_cases.py
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MAIN_SCRIPT = os.path.join(PROJECT_ROOT, "main.py")
META_DIR = os.path.join(os.path.dirname(__file__), "meta-examples")

def _list_case_dirs():
    if not os.path.isdir(META_DIR):
        return []
    return sorted(
        d for d in os.listdir(META_DIR)
        if os.path.isdir(os.path.join(META_DIR, d)) and not d.startswith(".")
    )

def _deep_merge(a, b):
    """Deep-merge dict b into dict a (returns new dict)."""
    if not isinstance(a, dict) or not isinstance(b, dict):
        return b
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def _load_etl(path):
    """Return (mappings:list, ctx:dict). Supports array form with trailing {ctx} or object form."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    mappings, ctx = [], {}
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "ctx" in item and len(item) == 1:
                if item["ctx"]:
                    ctx = _deep_merge(ctx, item["ctx"])
            else:
                mappings.append(item)
    elif isinstance(data, dict):
        mappings = data.get("mappings", [])
        ctx = data.get("ctx", {}) or {}
    else:
        raise ValueError(f"Unsupported ETL format in {path}")
    if not isinstance(mappings, list):
        raise ValueError(f"'mappings' must be a list in {path}")
    if not isinstance(ctx, dict):
        raise ValueError(f"'ctx' must be an object in {path}")
    return mappings, ctx

def _write_temp_etl(out_path, mappings, ctx):
    """Write object-form ETL with merged ctx."""
    etl = {"mappings": mappings}
    if ctx:
        etl["ctx"] = ctx
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(etl, f, ensure_ascii=False, indent=2)

class MetaETLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(MAIN_SCRIPT):
            raise unittest.SkipTest(f"main script not found at {MAIN_SCRIPT}")
        if not os.path.isdir(META_DIR):
            raise unittest.SkipTest(f"meta-examples dir not found at {META_DIR}")

    def _discover_cases_or_fail(self):
        case_dirs = _list_case_dirs()
        self.maxDiff = None
        self.assertTrue(case_dirs, f"No subdirectories found under {META_DIR} — add at least one meta case folder.")
        runnable = []
        missing = []
        for entry in case_dirs:
            case_dir = os.path.join(META_DIR, entry)
            meta_path = os.path.join(case_dir, "meta.json")
            if not os.path.exists(meta_path):
                missing.append(f"- {entry}: missing meta.json")
                continue
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
            except Exception as e:
                self.fail(f"Failed to parse {meta_path}: {e}")
            steps = meta.get("steps")
            if not isinstance(steps, list) or not steps:
                missing.append(f"- {entry}: 'steps' must be a non-empty array")
                continue
            runnable.append({"name": entry, "dir": case_dir, "meta": meta})
        if missing:
            self.fail("Some meta case directories are invalid:\n" + "\n".join(missing))
        self.assertEqual(len(runnable), len(case_dirs),
                         f"Discovered {len(runnable)} runnable meta cases but found {len(case_dirs)} subdirectories.")
        return runnable

    def test_meta_examples(self):
        cases = self._discover_cases_or_fail()
        for case in cases:
            with self.subTest(case=case["name"]):
                self._run_case(case)

    def _abs(self, base_dir, rel):
        return os.path.join(base_dir, rel) if rel not in (None, "$prev") else rel

    def _run_case(self, case):
        meta = case["meta"]
        meta_ctx = meta.get("ctx", {}) or {}
        steps = meta["steps"]

        # expect.json is test artifact (NOT part of meta spec)
        expect_path = os.path.join(case["dir"], "expect.json")
        has_expect = os.path.exists(expect_path)

        with tempfile.TemporaryDirectory(prefix=f"meta_{case['name']}_") as tmpdir:
            prev_out = None
            for idx, step in enumerate(steps, 1):
                etl_rel = step.get("etl")
                src_rel = step.get("src")
                dst_rel = step.get("dst")
                options = step.get("options", {}) or {}
                step_ctx = step.get("ctx", {}) or {}

                if not etl_rel or not src_rel:
                    self.fail(f"Step {idx} missing required keys 'etl' and/or 'src'")

                etl_path = self._abs(case["dir"], etl_rel)
                if not os.path.exists(etl_path):
                    self.fail(f"ETL file not found for step {idx}: {etl_path}")

                # Load sub-ETL to extract mappings + etl.ctx
                mappings, etl_ctx = _load_etl(etl_path)
                # Merge ctx: meta < etl < step
                eff_ctx = _deep_merge(_deep_merge(meta_ctx, etl_ctx), step_ctx)

                # Write synthesized ETL with merged ctx to temp
                tmp_etl = os.path.join(tmpdir, f"etl_step{idx}.json")
                _write_temp_etl(tmp_etl, mappings, eff_ctx)

                # Resolve src
                if src_rel == "$prev":
                    if prev_out is None:
                        self.fail(f"Step {idx} uses src '$prev' but no previous step output exists")
                    src_path = prev_out
                else:
                    src_path = self._abs(case["dir"], src_rel)
                    if not os.path.exists(src_path):
                        self.fail(f"Source file not found for step {idx}: {src_path}")

                # Prepare dst path (temp). If dst given and exists in case dir, copy it; else leave missing.
                if dst_rel == "$prev":
                    if prev_out is None:
                        self.fail(f"Step {idx} uses dst '$prev' but no previous step output exists")
                    tmp_dst = prev_out
                else:
                    tmp_dst = os.path.join(tmpdir, f"dst_step{idx}.json")
                    dst_abs = self._abs(case["dir"], dst_rel) if dst_rel else None
                    if dst_abs and os.path.exists(dst_abs):
                        shutil.copyfile(dst_abs, tmp_dst)

                # OUT path
                tmp_out = os.path.join(tmpdir, f"out_step{idx}.json")

                # Build CLI command
                cmd = [sys.executable, MAIN_SCRIPT, "--etl", tmp_etl, "--src", src_path, "--dst", tmp_dst, "--out", tmp_out]
                if "delimiter" in options:
                    cmd += ["--delimiter", options["delimiter"]]

                proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if proc.returncode != 0:
                    self.fail(
                        f"Meta step {idx} failed in case '{case['name']}'\n"
                        f"CMD: {' '.join(cmd)}\n"
                        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
                    )

                prev_out = tmp_out  # chain

            # Final output = prev_out
            with open(prev_out, "r", encoding="utf-8") as f:
                final_got = json.load(f)

            if has_expect:
                with open(expect_path, "r", encoding="utf-8") as f:
                    expected = json.load(f)
                self.assertEqual(final_got, expected, f"Final output mismatch for case '{case['name']}'")
            else:
                self.assertTrue(isinstance(final_got, (dict, list)), "Final output should be object or array")

if __name__ == "__main__":
    unittest.main()
