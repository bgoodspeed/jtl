# JTL - JSON-to-JSON Transform Loader

**JTL** is a small, declarative JSON-JSON ETL tool powered by jq. It’s designed to help you extract data from scanner outputs (Trivy, Prowler, etc.) and map them into a fixed structure (e.g., a `finding` object with `title`, `affected_entities`, `description`, …).

You describe mappings as one‑liners in an ETL spec (`etl.json`) and run them against a source JSON (`src.json`) and an optional destination/seed JSON (`dst.json`). You can also chain multiple ETLs with a **meta-ETL** (`meta.json`), with context (`ctx`) that’s available to jq expressions.

---

## Installation

```bash
$ # clone repository, enter the directory
$ pip install .
```
> Requires Python 3.9+

---

## Usage

Run as `python jtl.py`, or as `jtl` if installed via pip above.
### Single ETL

Run a single ETL spec against a source and destination:

```bash
python jtl.py --etl test/examples/strings/etl.json \
  --src test/examples/strings/src.json \
  --dst test/examples/strings/dst.json \
  --stdout
```

Flags:
- `--delimiter DELIM` : delimiter for string upserts (default: `\n`)
- `--out FILE` : file path for output (default: stdout, `-`)
- `--stdout` : force output to stdout (overrides `--out`)

### Meta-ETL (chains)

Chain multiple ETLs with context precedence:

```bash
python jtl.py --meta test/meta-examples/inline_chain/meta.json --stdout
```

Context precedence per step:
```
meta.ctx  <  etl.ctx  <  step.ctx
```

Special tokens:
- `"$prev"` for `src` → previous step’s output as input
- `"$prev"` for `dst` → previous step’s output as destination seed
- If `dst` is omitted or doesn’t exist, JTL starts with `{}`

---

## Examples (copy/paste)

These examples are taken directly from this repository’s tests so you can reproduce them verbatim.

### 1) Basic upsert (string join with newline)

**Files**: `test/examples/strings/{src.json, etl.json, dst.json}`

Run:
```bash
python jtl.py --etl test/examples/strings/etl.json \
  --src test/examples/strings/src.json \
  --dst test/examples/strings/dst.json \
  --stdout
```

Expected output:
```json
{ "t": "start\nx\ny" }
```

---

### 2) Inline static context (`ctx`) in an ETL

**Files**: `test/examples/inline_static/{src.json, etl.json}` (no dst.json; JTL starts from `{}`)

Run:
```bash
python jtl.py --etl test/examples/inline_static/etl.json \
  --src test/examples/inline_static/src.json \
  --dst test/examples/inline_static/dst.json \
  --stdout
```

Output:
```json
{
  "finding": {
    "title": "Issue: EC2",
    "affected_entities": "arn:aws:ec2:region:acct:instance/i-aaaa\narn:aws:ec2:region:acct:instance/i-bbbb",
    "description": "Detected type: EC2 — please review."
  }
}
```

---

### 3) Custom delimiter for string upsert

**Files**: `test/examples/delimiter_strings/{src.json, etl.json, dst.json, options.json, expect.json}`

Run (using the delimiter in `options.json` is demonstrated in meta tests; for single ETL pass via CLI):
```bash
python jtl.py --etl test/examples/delimiter_strings/etl.json \
  --src test/examples/delimiter_strings/src.json \
  --dst test/examples/delimiter_strings/dst.json \
  --delimiter " | " \
  --stdout
```

Output:
```json
{ "t": "start | x | y" }
```

---

### 4) Meta-ETL: inline chain with ctx precedence

**Files**: `test/meta-examples/inline_chain/{src.json, etl_step1.json, etl_step2.json, meta.json}`

`meta.json` (context split across meta and step-level; ETLs stay tiny):
```json
{
  "version": 1,
  "ctx": { "suffix": " — please review." },
  "steps": [
    { "etl": "etl_step1.json", "src": "src.json" },
    { "etl": "etl_step2.json", "src": "src.json", "dst": "$prev", "ctx": { "preamble": "Detected type: " } }
  ]
}
```

Run:
```bash
python jtl.py --meta test/meta-examples/inline_chain/meta.json --stdout
```

Output:
```json
{
  "finding": {
    "title": "Issue: EC2",
    "affected_entities": "arn:aws:ec2:region:acct:instance/i-aaaa\narn:aws:ec2:region:acct:instance/i-bbbb",
    "description": "Detected type: EC2 — please review."
  }
}
```

---

### 5) Meta-ETL: demonstrate ctx precedence (meta.ctx < etl.ctx < step.ctx)

**Files**: `test/meta-examples/precedence_override/{src.json, etl_step1.json, etl_step2.json, meta.json}`

Run:
```bash
python jtl.py --meta test/meta-examples/precedence_override/meta.json --stdout
```

Output:
```json
{
  "finding": {
    "title": "Issue: S3",
    "affected_entities": "arn:aws:s3:::bucket-1\narn:aws:s3:::bucket-2",
    "description": "Detected: S3 - check controls"
  }
}
```

---

## Writing ETLs

An ETL is either:
- **Array form** of one‑line mappings, with an optional trailing `{ "ctx": {...} }`, or
- **Object form** with `{ "mappings": [...], "ctx": {...} }`

Each mapping:
```json
{ "src": "<jq expr>", "dst": ".jq.lvalue.path", "mode": "upsert|replace" }
```
- Default `mode` is `upsert`
- `dst` must be a concrete jq lvalue path (e.g., `.finding.title`, `.a.b[2].c`)
- `$ctx` is available inside `src` expressions

**String upsert** appends with the delimiter (defaults to `\n`).  
**Array upsert** extends arrays or appends scalars.  
**Object upsert** deep-merges objects.  
`replace` overwrites the target (with `null`, single value, or array of results if the jq expression streams).

---

## jq Snippets Cheat‑Sheet (security‑oriented)

Below are practical jq fragments you can paste into `src` expressions when building parsers for scanner outputs.

1. **Extract ARNs from an array of objects**
   ```jq
   .[] | .Arn
   ```

2. **Get the issue type once (first seen)**
   ```jq
   ([.[].Type] | first)
   ```

3. **Collect unique severities across findings**
   ```jq
   ([.[].Severity] | unique | sort | join(", "))
   ```

4. **Filter by severity (CRITICAL)**
   ```jq
   .[] | select(.Severity == "CRITICAL")
   ```

5. **Build a multi‑line description with context**
   ```jq
   $ctx.preamble + ([.[].Type] | first) + $ctx.suffix
   ```

6. **Join package names as newline‑separated list**
   ```jq
   ([.[].PackageName] | join("\n"))
   ```

7. **Fallback when a field may be null / missing**
   ```jq
   (.Remediation // "No remediation provided")
   ```

8. **Emit a compact subobject for later merging**
   ```jq
   { id: .FindingId, sev: .Severity, resource: .Resource }
   ```

9. **Compose a title with severity and rule**
   ```jq
   "Rule " + .RuleId + " (" + (.Severity // "UNKNOWN") + ")"
   ```

10. **Normalize boolean flags to Yes/No**
    ```jq
    (if (.PubliclyAccessible // false) then "Yes" else "No" end)
    ```

Tip: when you need to aggregate values (e.g., count or summarize per resource), build that in a meta step that reads from the original `src.json` while writing into `$prev`.

---

## Typical Workflow

1. Capture scanner output (e.g., `trivy.json`, `prowler.json`).  
2. Create a concise `etl.json` or a small chain `meta.json` describing how to map fields into your normalized `finding` object.  
3. Run JTL:
   ```bash
   # Single ETL
   python jtl.py --etl path/to/etl.json --src path/to/src.json --dst path/to/dst.json --stdout
   # Meta chain
   python jtl.py --meta path/to/meta.json --stdout
   ```
4. Add new ETLs as folders under `test/examples/` or `test/meta-examples/` to version and regression‑test them.

---

## License

MIT
