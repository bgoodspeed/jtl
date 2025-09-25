import unittest

from main import run_etl, parse_jq_path



class TestJTL(unittest.TestCase):
    def test_string_upsert_with_existing_and_multiple_sources(self):
        src = {"a": "src1", "b": "src2"}
        dst = {"t": "existingvalue"}
        etl = [
            {"src": ".a", "dst": ".t"},  # upsert default
            {"src": ".b", "dst": ".t"},
        ]
        out = run_etl(etl, src, dst, delimiter="\n", ctx={}, prelude="")
        self.assertEqual(out["t"], "existingvalue\nsrc1\nsrc2")

    def test_string_upsert_empty_existing(self):
        src = {"x": "val"}
        dst = {"t": ""}
        etl = [{"src": ".x", "dst": ".t"}]
        out = run_etl(etl, src, dst, delimiter="\n", ctx={}, prelude="")
        self.assertEqual(out["t"], "val")

    def test_string_replace(self):
        src = {"x": "one", "y": "two"}
        dst = {"t": "keep"}
        etl = [{"src": ".y", "dst": ".t", "mode": "replace"}]
        out = run_etl(etl, src, dst, delimiter="\n", ctx={}, prelude="")
        self.assertEqual(out["t"], "two")

    def test_replace_multiple_results_sets_array(self):
        src = {"arr": [1, 2, 3]}
        dst = {}
        etl = [{"src": ".arr[]", "dst": ".nums", "mode": "replace"}]
        out = run_etl(etl, src, dst, delimiter="\n", ctx={}, prelude="")
        self.assertEqual(out["nums"], [1, 2, 3])

    def test_array_upsert_extend_and_append(self):
        src = {"a": [1, 2], "b": 3}
        dst = {"t": []}
        etl = [
            {"src": ".a", "dst": ".t"},       # extend with [1,2]
            {"src": ".b", "dst": ".t"}        # append 3
        ]
        out = run_etl(etl, src, dst, delimiter="\n", ctx={}, prelude="")
        self.assertEqual(out["t"], [1, 2, 3])

    def test_object_deep_merge_upsert(self):
        src = {"u": {"x": 1}, "v": {"y": 2}}
        dst = {"t": {"x": 0, "z": 9}}
        etl = [
            {"src": ".u", "dst": ".t"},
            {"src": ".v", "dst": ".t"},
        ]
        out = run_etl(etl, src, dst, delimiter="\n", ctx={}, prelude="")
        self.assertEqual(out["t"], {"x": 1, "z": 9, "y": 2})

    def test_create_missing_path_and_index(self):
        src = {"val": "hello"}
        dst = {}
        etl = [{"src": ".val", "dst": ".a.b[2].c"}]
        out = run_etl(etl, src, dst, delimiter="\n", ctx={}, prelude="")
        # a: { b: [None, None, {c: "hello"}] }
        self.assertIn("a", out)
        self.assertIn("b", out["a"])
        self.assertEqual(len(out["a"]["b"]), 3)
        self.assertEqual(out["a"]["b"][2]["c"], "hello")

    def test_multiple_mappings_same_dst_array(self):
        src = {"u": [1], "v": [2, 3], "w": 4}
        dst = {"t": []}
        etl = [
            {"src": ".u", "dst": ".t"},
            {"src": ".v", "dst": ".t"},
            {"src": ".w", "dst": ".t"},
        ]
        out = run_etl(etl, src, dst, delimiter="\n", ctx={}, prelude="")
        self.assertEqual(out["t"], [1, 2, 3, 4])

    def test_delimiter_override(self):
        src = {"a": "x", "b": "y"}
        dst = {"t": "start"}
        etl = [{"src": ".a", "dst": ".t"}, {"src": ".b", "dst": ".t"}]
        out = run_etl(etl, src, dst, delimiter=" | ", ctx={}, prelude="")
        self.assertEqual(out["t"], "start | x | y")

    def test_replace_zero_results_sets_null(self):
        src = {"a": 1}
        dst = {}
        etl = [{"src": ".b", "dst": ".t", "mode": "replace"}]
        out = run_etl(etl, src, dst, delimiter="\n", ctx={}, prelude="")
        self.assertIsNone(out["t"])

    def test_path_parsing_with_quotes(self):
        segs = parse_jq_path(r'.foo["bar baz"][0]["qu\"ote"]')
        self.assertEqual(segs, ["foo", "bar baz", 0, 'qu"ote'])


    def test_context_support(self):
        src = [{"Arn": "arn:1", "Type": "EC2"}, {"Arn": "arn:2", "Type": "EC2"}]
        dst = {}
        etl = [
            {"src": ".[] | .Arn", "dst": ".finding.affected_entities"},
            {"src": "$ctx.preamble + ([.[].Type] | first) + $ctx.suffix", "dst": ".finding.description",
             "mode": "replace"},
            {"src": "\"Issue: \" + ([.[].Type] | first)", "dst": ".finding.title", "mode": "replace"},
        ]
        ctx = {"preamble": "Detected type: ", "suffix": " — please review."}

        out = run_etl(etl, src, dst, delimiter="\n", ctx=ctx, prelude="")
        self.assertEqual(out, {
            "finding": {
                "title": "Issue: EC2",
                "affected_entities": "arn:1\narn:2",
                "description": "Detected type: EC2 — please review."
            }
        })

if __name__ == '__main__':
    unittest.main()

