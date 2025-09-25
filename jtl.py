#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import json
import os
import re
import sys
from copy import deepcopy

import jq

# -------------------------
# JQ path parsing (concrete lvalue paths only)
# -------------------------

_PATH_TOKEN_RE = re.compile(
    r"""
    (?:
        \.([A-Za-z_][A-Za-z0-9_]*)         # .name
      | \[\s*([0-9]+)\s*\]                 # [123]
      | \[\s*"((?:[^"\\]|\\.)*)"\s*\]      # ["quoted\"key"]
      | \[\s*'((?:[^'\\]|\\.)*)'\s*\]      # ['quoted\'key']
    )
    """,
    re.VERBOSE,
)

def _unescape(s: str) -> str:
    return bytes(s, "utf-8").decode("unicode_escape")

def parse_jq_path(path: str):
    if not path or path[0] != '.':
        raise ValueError(f"Destination path must start with '.': {path}")
    i = 0
    segs = []
    while i < len(path):
        m = _PATH_TOKEN_RE.match(path, i)
        if not m:
            if i == 0 and path == '.':
                return []
            raise ValueError(f"Unsupported or non-concrete dst path near: '{path[i:]}' (full: {path})")
        name, idx, dq, sq = m.groups()
        if name is not None:
            segs.append(name)
        elif idx is not None:
            segs.append(int(idx))
        elif dq is not None:
            segs.append(_unescape(dq))
        elif sq is not None:
            segs.append(_unescape(sq))
        i = m.end()
    return segs

# -------------------------
# JSON helpers
# -------------------------

def deep_merge(dst, src):
    """Deep-merge src into dst (dicts only). Returns merged in place on dicts; otherwise src."""
    if not isinstance(dst, dict) or not isinstance(src, dict):
        return deepcopy(src)
    for k, v in src.items():
        if k in dst and isinstance(dst[k], dict) and isinstance(v, dict):
            deep_merge(dst[k], v)
        else:
            dst[k] = deepcopy(v)
    return dst

def _upsert_value(existing, new_value, delimiter):
    # strings
    if isinstance(existing, str) and isinstance(new_value, str):
        if existing == "":
            return new_value
        if new_value == "":
            return existing
        return existing + delimiter + new_value
    # arrays
    if isinstance(existing, list):
        if isinstance(new_value, list):
            return existing + new_value
        return existing + [new_value]
    # objects
    if isinstance(existing, dict) and isinstance(new_value, dict):
        deep_merge(existing, new_value)
        return existing
    # if existing is None -> set
    if existing is None:
        return deepcopy(new_value)
    # mixed/scalars: last write wins
    return deepcopy(new_value)

def _ensure_parent(container, path_segments):
    if not path_segments:
        return None, None
    cur = container
    for j, seg in enumerate(path_segments[:-1]):
        nxt = path_segments[j+1] if j+1 < len(path_segments) else None
        if isinstance(seg, int):
            if not isinstance(cur, list):
                raise TypeError(f"Encountered non-list while navigating index [{seg}]")
            while len(cur) <= seg:
                cur.append(None)
            if cur[seg] is None:
                cur[seg] = [] if isinstance(nxt, int) else {}
            cur = cur[seg]
        else:
            if not isinstance(cur, dict):
                raise TypeError(f"Encountered non-object while navigating field .{seg}")
            if seg not in cur or cur[seg] is None:
                cur[seg] = {} if not isinstance(nxt, int) else []
            cur = cur[seg]
    return cur, path_segments[-1]

def set_path_value(root, path_segments, value, mode, delimiter):
    if not path_segments:
        if mode == "replace":
            return deepcopy(value)
        else:
            return _upsert_value(root, value, delimiter)

    parent, last = _ensure_parent(root, path_segments)
    if isinstance(last, int):
        if not isinstance(parent, list):
            raise TypeError(f"Target parent is not a list for index [{last}]")
        while len(parent) <= last:
            parent.append(None)
        if mode == "replace":
            parent[last] = deepcopy(value)
        else:
            parent[last] = _upsert_value(parent[last], value, delimiter)
    else:
        if not isinstance(parent, dict):
            raise TypeError(f"Target parent is not an object for key '{last}'")
        if mode == "replace":
            parent[last] = deepcopy(value)
        else:
            parent[last] = _upsert_value(parent.get(last, None), value, delimiter)
    return root

# -------------------------
# ETL core
# -------------------------

def evaluate_src(expr, src_obj, ctx_obj, prelude):
    ctx_json = json.dumps(ctx_obj, ensure_ascii=False)
    # Compose: ($ctx) | <prelude> ( <expr> )
    # prelude can be empty string.
    wrapped = f"({ctx_json}) as $ctx | {prelude} ({expr})"
    prog = jq.compile(wrapped)
    return list(prog.input(src_obj).all())

def _decode_delim(s: str) -> str:
    # match CLI behavior: accept "\n", "\t", etc.
    return bytes(s, "utf-8").decode("unicode_escape")

def apply_mapping(src_obj, dst_obj, mapping, delimiter, ctx, prelude=""):
    src_expr = mapping["src"]
    dst_path = mapping["dst"]
    mode = mapping.get("mode", "upsert").lower()
    if mode not in ("upsert", "replace"):
        raise ValueError(f"Unsupported mode: {mode}")

    # NEW: per-row delimiter (overrides step/CLI default)
    if "delimiter" in mapping and mapping["delimiter"] is not None:
        eff_delim = _decode_delim(mapping["delimiter"])
    else:
        eff_delim = delimiter

    results = evaluate_src(src_expr, src_obj, ctx, prelude)
    segs = parse_jq_path(dst_path)

    if mode == "replace":
        if len(results) == 0:
            value = None
        elif len(results) == 1:
            value = results[0]
        else:
            value = results
        set_path_value(dst_obj, segs, value, mode="replace", delimiter=eff_delim)
        return

    # upsert per item (string upserts will use eff_delim)
    for val in results:
        set_path_value(dst_obj, segs, val, mode="upsert", delimiter=eff_delim)

def run_etl(mappings, src_obj, dst_obj, delimiter, ctx, prelude):
    for mapping in mappings:
        if "src" not in mapping or "dst" not in mapping:
            raise ValueError("Each mapping must include 'src' and 'dst'")
        apply_mapping(src_obj, dst_obj, mapping, delimiter, ctx, prelude)
    return dst_obj

# -------------------------
# ETL spec loading (supports array/object + inline ctx)
# -------------------------

def load_etl_file(path):
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    mappings, ctx, prelude = [], {}, ""
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict) and "ctx" in item and len(item) == 1:
                if item["ctx"]:
                    deep_merge(ctx, item["ctx"])
            elif isinstance(item, dict) and "with" in item and len(item) == 1:
                prelude = (item["with"] or "").strip()
            else:
                mappings.append(item)
    elif isinstance(raw, dict):
        mappings = raw.get("mappings", [])
        ctx = raw.get("ctx", {}) or {}
        prelude = (raw.get("with", "") or "").strip()
    else:
        raise ValueError("ETL spec must be a JSON array or object")

    return mappings, ctx, prelude

# -------------------------
# META-ETL runner
# -------------------------

def run_meta(meta_path, default_delimiter):
    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    meta_ctx = meta.get("ctx", {}) or {}
    steps = meta.get("steps")
    if not isinstance(steps, list) or not steps:
        raise ValueError(f"{meta_path}: 'steps' must be a non-empty array")

    prev_obj = None  # in-memory chain
    final_obj = None

    for idx, step in enumerate(steps, 1):
        etl_rel = step.get("etl")
        src_rel = step.get("src")
        dst_rel = step.get("dst")  # optional
        step_ctx = step.get("ctx", {}) or {}
        options = step.get("options", {}) or {}

        if not etl_rel or not src_rel:
            raise ValueError(f"Step {idx}: missing required keys 'etl' and/or 'src'")

        etl_path = os.path.join(os.path.dirname(meta_path), etl_rel)
        mappings, etl_ctx, prelude = load_etl_file(etl_path)

        # Effective context: meta < etl < step
        eff_ctx = deepcopy(meta_ctx)
        deep_merge(eff_ctx, etl_ctx)
        deep_merge(eff_ctx, step_ctx)

        # Step delimiter (fallback to default)
        step_delim = options.get("delimiter", default_delimiter)

        # Resolve SRC object
        if src_rel == "$prev":
            if prev_obj is None:
                raise ValueError(f"Step {idx}: src '$prev' used but no previous output exists")
            src_obj = prev_obj
        else:
            src_path = os.path.join(os.path.dirname(meta_path), src_rel)
            with open(src_path, "r", encoding="utf-8") as f:
                src_obj = json.load(f)

        # Resolve DST seed object
        if dst_rel == "$prev":
            if prev_obj is None:
                raise ValueError(f"Step {idx}: dst '$prev' used but no previous output exists")
            dst_obj = deepcopy(prev_obj)
        elif dst_rel:
            dst_path = os.path.join(os.path.dirname(meta_path), dst_rel)
            if os.path.exists(dst_path):
                with open(dst_path, "r", encoding="utf-8") as f:
                    dst_obj = json.load(f)
            else:
                dst_obj = {}
        else:
            # No dst provided -> start from {}
            dst_obj = {}

        # Run the ETL step (in-memory)
        out_obj = run_etl(mappings, src_obj, dst_obj, step_delim, eff_ctx, prelude)
        prev_obj = out_obj
        final_obj = out_obj

    return final_obj

# -------------------------
# CLI
# -------------------------

def main():
    ap = argparse.ArgumentParser(description="JSON-JSON ETL (single or meta chain) Declaratively extract and transform one JSON structure to another")
    mux = ap.add_mutually_exclusive_group(required=True)
    mux.add_argument("--etl", help="ETL spec JSON file (single ETL)")
    mux.add_argument("--meta", help="Meta-ETL spec JSON file (chain multiple ETLs)")

    ap.add_argument("--src", help="Source JSON file (required for --etl)")
    ap.add_argument("--dst", help="Destination/seed JSON file (created as {} if missing)")
    ap.add_argument("--out", default="-", help="Output file (default: stdout). Use '-' for stdout explicitly.")
    ap.add_argument("--stdout", action="store_true",
                    help="Force writing output to stdout (overrides --out)")
    ap.add_argument("--delimiter", default="\\n",
                    help="Default delimiter for string upserts (default: \\n)")

    args = ap.parse_args()
    delimiter = bytes(args.delimiter, "utf-8").decode("unicode_escape")

    # Small helper to emit
    def _emit(obj):
        out_txt = json.dumps(obj, ensure_ascii=False, indent=2)
        if args.stdout or args.out in (None, "-", ""):
            print(out_txt)
        else:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(out_txt)

    if args.meta:
        result = run_meta(args.meta, default_delimiter=delimiter)
        _emit(result)
        return

    # Single ETL mode
    if not (args.etl and args.src ):
        ap.error("for single ETL mode, the following are required: --etl, --src")

    mappings, etl_ctx, prelude = load_etl_file(args.etl)

    with open(args.src, "r", encoding="utf-8") as f:
        src_obj = json.load(f)

    if args.dst and os.path.exists(args.dst):
        with open(args.dst, "r", encoding="utf-8") as f:
            dst_obj = json.load(f)
    else:
        dst_obj = {}

    result = run_etl(mappings, src_obj, dst_obj, delimiter, ctx=etl_ctx, prelude=prelude)
    _emit(result)


if __name__ == "__main__":
    main()
