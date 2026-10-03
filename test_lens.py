import json
import tempfile
import unittest
from pathlib import Path

import lens


class TestPollUrl(unittest.TestCase):
    def test_build_url_has_since_and_wait(self):
        url = lens.build_poll_url("http://x/room", 12, 10)
        self.assertIn("since=12", url)
        self.assertIn("wait=10", url)

    def test_parse_batch_list(self):
        msgs, nxt = lens.parse_batch([{"seq": 5}], 4)
        self.assertEqual(nxt, 5)
        self.assertEqual(len(msgs), 1)

    def test_parse_batch_dict(self):
        payload = {"messages": [{"seq": 2}, {"seq": 3}], "next_since": 3}
        msgs, nxt = lens.parse_batch(payload, 1)
        self.assertEqual(nxt, 3)
        self.assertEqual(len(msgs), 2)


class TestPollLoop(unittest.TestCase):
    def test_loop_collects_and_moves_cursor(self):
        def fake_fetch(since_seq, wait_secs):
            batch = [{"seq": since_seq + 1, "data": "hi"}]
            return batch, since_seq + 1
        msgs, cursor = lens.poll_loop(fake_fetch, start_seq=0, max_batches=3, backoff_secs=0)
        self.assertEqual(cursor, 3)
        self.assertEqual(len(msgs), 3)

    def test_loop_backs_off_on_error(self):
        calls = []
        def flaky(since_seq, wait_secs):
            calls.append(since_seq)
            if len(calls) == 1:
                raise TimeoutError("slow")
            return [], since_seq
        msgs, cursor = lens.poll_loop(flaky, start_seq=7, max_batches=2, backoff_secs=0)
        self.assertEqual(cursor, 7)
        self.assertEqual(msgs, [])

    def test_cursor_file_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "cursor.txt"
            lens.save_cursor(p, 42)
            self.assertEqual(lens.load_cursor(p, 0), 42)
            missing = Path(tmp) / "nope.txt"
            self.assertEqual(lens.load_cursor(missing, 9), 9)


class TestLint(unittest.TestCase):
    def test_flags_missing_wait(self):
        src = "url = base + '?since=' + str(seq)\nwhile True:\n    time.sleep(5)\n"
        out = lens.lint_source(src)
        kinds = [f["kind"] for f in out]
        self.assertIn("missing-wait", kinds)

    def test_flags_tight_loop(self):
        src = "while True:\n    fetch(since)\n"
        out = lens.lint_source(src)
        kinds = [f["kind"] for f in out]
        self.assertIn("tight-loop", kinds)

    def test_flags_discarded_cursor(self):
        src = "x = poll('?since=1&wait=10')\nprint(x)\n"
        out = lens.lint_source(src)
        kinds = [f["kind"] for f in out]
        self.assertIn("discarded-cursor", kinds)

    def test_clean_source_has_no_findings(self):
        src = "url = build_poll_url(base, cursor, 10)\nmsgs, cursor = fetch(cursor, 10)\nsave_cursor(p, cursor)\n"
        out = lens.lint_source(src)
        self.assertEqual(out, [])


class TestLatency(unittest.TestCase):
    def test_splits_poll_and_ring(self):
        events = [
            {"seq": 1, "appended_at": 10.0, "poll_start": 9.0, "arrived_at": 11.0},
            {"seq": 2, "appended_at": 12.0, "poll_start": 11.0, "arrived_at": 12.5},
        ]
        rows, text = lens.attribute_latency(events)
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(rows[0]["poll_delay"], 2.0)
        self.assertAlmostEqual(rows[1]["ring_gap"], 2.0)
        self.assertIn("poll_delay", text)

    def test_write_csv_to_tempfile(self):
        events = [{"seq": 1, "appended_at": 1.0, "poll_start": 0.5, "arrived_at": 1.5}]
        rows, _ = lens.attribute_latency(events)
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "lat.csv"
            lens.write_latency_csv(rows, p)
            content = p.read_text(encoding="utf-8")
            self.assertIn("seq", content)


class TestLoad(unittest.TestCase):
    def test_long_poll_uses_fewer_calls(self):
        info = lens.estimate_load(1, 50, duration_secs=100, naive_every_secs=1, wait_secs=10)
        self.assertGreater(info["naive_calls"], info["long_calls"])
        self.assertEqual(info["total_messages"], 50)
        self.assertGreater(info["ratio"], 1.0)


class TestCollapse(unittest.TestCase):
    def test_groups_heartbeats_and_keeps_signal(self):
        msgs = [
            "heartbeat layer online",
            "heartbeat layer online",
            "node report identity verified",
            "need help with sync issue",
            "offline ledger re-check failed at line 3",
        ]
        out = lens.collapse_heartbeats(msgs)
        self.assertEqual(out["total"], 5)
        self.assertEqual(out["heartbeat_count"], 3)
        self.assertEqual(out["signal_count"], 2)
        joined = " ".join(out["signals"])
        self.assertIn("sync", joined)


class TestChain(unittest.TestCase):
    def test_valid_chain_passes(self):
        lines = lens.make_sample_chain(["a", "b", "c"])
        res = lens.verify_chain(lines)
        self.assertTrue(res["ok"])
        self.assertEqual(res["checked"], 3)

    def test_broken_link_named(self):
        lines = lens.make_sample_chain(["a", "b", "c"])
        rec = json.loads(lines[1])
        rec["data"] = "tampered"
        lines[1] = json.dumps(rec)
        res = lens.verify_chain(lines)
        self.assertFalse(res["ok"])
        self.assertEqual(res["broken_seq"], 2)

    def test_bad_json_reported(self):
        res = lens.verify_chain(["not json"])
        self.assertFalse(res["ok"])


if __name__ == "__main__":
    unittest.main()
