#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JSON→JSON ETL using jq expressions for source extraction and simple jq-like
paths for destination updates.

Spec format (JSON array):
[
  {"src": ".users[] | select(.active) | .email", "dst": ".report.activeEmails", "mode": "upsert"},
  {"src": ".version", "dst": ".metadata.version", "mode": "replace"}
]

Modes:
- upsert (default):
    * string targets: append using delimiter when already a string
    * array targets: extend with src arrays, append scalars
    * object targets: deep-merge
    * other scalars: last write wins
    * non-existent targets are created
    * multiple src results are applied one-by-one
- replace:
    * if src yields 0 results: sets null
    * if src yields 1 result: sets that value
    * if src yields >1 result: sets array of results
    * overwrites any prior value at the destination

Destination path syntax (subset of jq lvalue paths):
  .foo.bar[0].baz
  .foo["key with spaces"]
  .foo['quoted']
No filters/selects in dst — it must be a concrete path.
"""

import argparse
import json
import os
import re
import sys
from copy import deepcopy
import jq
import unittest


# -------------------------
# Path parsing & utilities
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
    # interpret common escapes like \" \\ \n \t in quoted bracket notation
    return bytes(s, "utf-8").decode("unicode_escape")

def parse_jq_path(path: str):
    """Parse a limited jq-style path into a list of segments (str keys or int indices)."""
    if not path or path[0] != '.':
        raise ValueError(f"Destination path must start with '.': {path}")
    i = 0
    segs = []
    while i < len(path):
        m = _PATH_TOKEN_RE.match(path, i)
        if not m:
            if i == 0:
                # allow bare '.' (root)
                if path == '.':
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
        else:
            raise AssertionError("Unexpected path parse state")
        i = m.end()
    return segs

def _ensure_parent(container, path_segments):
    """Ensure parent objects/arrays exist up to the last segment. Returns (parent, last_seg)."""
    if not path_segments:
        return None, None
    cur = container
    for j, seg in enumerate(path_segments[:-1]):
        nxt = path_segments[j+1] if j+1 < len(path_segments) else None
        if isinstance(seg, int):
            # ensure list
            if not isinstance(cur, list):
                # create list if absent or wrong type
                # replace with a list in its parent context is handled by caller
                raise TypeError(f"Encountered non-list while navigating index [{seg}]")
            # grow list as needed
            while len(cur) <= seg:
                cur.append(None)
            if cur[seg] is None:
                # decide next container type by next segment
                cur[seg] = [] if isinstance(nxt, int) else {}
            cur = cur[seg]
        else:
            # ensure dict
            if not isinstance(cur, dict):
                raise TypeError(f"Encountered non-object while navigating field .{seg}")
            if seg not in cur or cur[seg] is None:
                cur[seg] = {} if not isinstance(nxt, int) else []
            cur = cur[seg]
    return cur, path_segments[-1]

def _get_ref(container, path_segments, create_missing: bool):
    """Get (parent, last_seg, exists_flag). Create parents if requested."""
    if not path_segments:
        return None, None, True  # root
    # We need to create parents if asked; else just traverse
    cur = container
    # Traversal without creation to detect missing
    try:
        for j, seg in enumerate(path_segments[:-1]):
            if isinstance(seg, int):
                if not isinstance(cur, list) or seg >= len(cur):
                    if create_missing:
                        # create parents from here
                        parent, last = _ensure_parent(container, path_segments)
                        return parent, last, False
                    return None, None, False
                cur = cur[seg]
            else:
                if not isinstance(cur, dict) or seg not in cur:
                    if create_missing:
                        parent, last = _ensure_parent(container, path_segments)
                        return parent, last, False
                    return None, None, False
                cur = cur[seg]
        # Now have parent; determine if leaf exists
        last = path_segments[-1]
        exists = False
        if isinstance(last, int):
            if isinstance(cur, list) and last < len(cur) and cur[last] is not None:
                exists = True
        else:
            if isinstance(cur, dict) and last in cur:
                exists = True
        return cur, last, exists
    except TypeError:
        if create_missing:
            parent, last = _ensure_parent(container, path_segments)
            return parent, last, False
        return None, None, False

def deep_merge(dst, src):
    """Deep-merge src into dst (dicts only). Returns merged object (in place on dst)."""
    if not isinstance(dst, dict) or not isinstance(src, dict):
        return src
    for k, v in src.items():
        if k in dst and isinstance(dst[k], dict) and isinstance(v, dict):
            deep_merge(dst[k], v)
        else:
            dst[k] = deepcopy(v)
    return dst

def _append_to_array(existing, new_item):
    if not isinstance(existing, list):
        # convert scalars to 1-element list
        return [existing, new_item] if existing is not None else [new_item]
    if isinstance(new_item, list):
        existing.extend(new_item)
    else:
        existing.append(new_item)
    return existing

def _upsert_value(existing, new_value, delimiter):
    """Type-aware upsert semantics."""
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
    # if existing is None -> simply set
    if existing is None:
        return deepcopy(new_value)
    # mixed or scalar types -> last write wins
    return deepcopy(new_value)

def set_path_value(root, path_segments, value, mode, delimiter):
    """Set or upsert value at dst path."""
    if not path_segments:
        # root assignment
        if mode == "replace":
            return deepcopy(value)
        else:
            return _upsert_value(root, value, delimiter)

    # Ensure parents exist
    parent, last, exists = _get_ref(root, path_segments, create_missing=True)
    if isinstance(last, int):
        if not isinstance(parent, list):
            raise TypeError(f"Target parent is not a list for index [{last}]")
        # grow if needed
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
# ETL processing
# -------------------------

def evaluate_src(expr, src_obj):
    """Evaluate a jq expression against src_obj, return list of results."""
    prog = jq.compile(expr)
    # Using .input(src_obj).all() handles streaming outputs
    return list(prog.input(src_obj).all())

def apply_mapping(src_obj, dst_obj, mapping, delimiter):
    src_expr = mapping["src"]
    dst_path = mapping["dst"]
    mode = mapping.get("mode", "upsert").lower()
    if mode not in ("upsert", "replace"):
        raise ValueError(f"Unsupported mode: {mode}")
    results = evaluate_src(src_expr, src_obj)

    # replace: set to null (if 0 results), single value, or array of results
    if mode == "replace":
        if len(results) == 0:
            value = None
        elif len(results) == 1:
            value = results[0]
        else:
            value = results
        segs = parse_jq_path(dst_path)
        set_path_value(dst_obj, segs, value, mode="replace", delimiter=delimiter)
        return

    # upsert: apply sequentially
    segs = parse_jq_path(dst_path)
    if len(results) == 0:
        # Nothing to apply
        return
    for val in results:
        set_path_value(dst_obj, segs, val, mode="upsert", delimiter=delimiter)

def run_etl(etl_spec, src_obj, dst_obj, delimiter):
    # Preserve order; allow multiple rules for same dst
    for mapping in etl_spec:
        if "src" not in mapping or "dst" not in mapping:
            raise ValueError("Each mapping must include 'src' and 'dst'")
        apply_mapping(src_obj, dst_obj, mapping, delimiter)
    return dst_obj

# -------------------------
# CLI
# -------------------------

def main():
    ap = argparse.ArgumentParser(description="JSON→JSON ETL using jq and jq-like dst paths")
    ap.add_argument("--etl", required=False, help="ETL spec JSON file (array of mappings)")
    ap.add_argument("--src", required=False, help="Source JSON file")
    ap.add_argument("--dst", required=False, help="Destination (seed) JSON file")
    ap.add_argument("--out", default="-", help="Output file (default: stdout)")
    ap.add_argument("--delimiter", default="\\n", help="Delimiter used when appending strings (default: \\n)")
    ap.add_argument("--run-tests", action="store_true", help="Run unit tests and exit")
    args = ap.parse_args()


    if not (args.etl and args.src and args.dst):
        ap.error("the following arguments are required: --etl, --src, --dst (or use --run-tests)")

    # Interpret escaped delimiter (e.g., "\n")
    delimiter = bytes(args.delimiter, "utf-8").decode("unicode_escape")

    with open(args.etl, "r", encoding="utf-8") as f:
        etl_spec = json.load(f)
        if not isinstance(etl_spec, list):
            raise ValueError("ETL spec must be a JSON array")

    with open(args.src, "r", encoding="utf-8") as f:
        src_obj = json.load(f)

    if os.path.exists(args.dst):
        with open(args.dst, "r", encoding="utf-8") as f:
            dst_obj = json.load(f)
    else:
        # If the destination file does not exist, initialize with an empty object
        dst_obj = {}

    result = run_etl(etl_spec, src_obj, dst_obj, delimiter)

    out_txt = json.dumps(result, ensure_ascii=False, indent=2)
    if args.out == "-" or args.out is None:
        print(out_txt)
    else:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out_txt)


if __name__ == "__main__":
    main()
