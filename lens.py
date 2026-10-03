"""Technocore append-log poll clarity lens.

Local-first helpers to poll correctly, explain latency,
estimate idle calls, collapse heartbeat noise, and check
a public sample hash chain. Stdlib only.
"""

import csv
import hashlib
import io
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_WAIT = 10
GENESIS = "GENESIS"


def build_poll_url(base_url, since_seq, wait_secs=DEFAULT_WAIT):
    """Build poll URL with since cursor and long-poll wait."""
    base = base_url.rstrip("/")
    query = urllib.parse.urlencode({"since": int(since_seq), "wait": int(wait_secs)})
    return base + "?" + query


def fetch_batch(base_url, since_seq, wait_secs=DEFAULT_WAIT, timeout_secs=15):
    """Fetch one batch over HTTP with stdlib. Returns (messages, next_since)."""
    url = build_poll_url(base_url, since_seq, wait_secs)
    timeout = float(timeout_secs)
    if timeout < 1:
        timeout = 15.0
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", "replace")
    data = json.loads(raw)
    return parse_batch(data, since_seq)


def parse_batch(data, since_seq):
    """Parse server payload into (messages, next_since)."""
    if isinstance(data, list):
        msgs = data
        if msgs:
            last = msgs[-1]
            nxt = int(last.get("seq", since_seq + len(msgs)))
            return msgs, nxt
        return [], int(since_seq)
    msgs = data.get("messages", data.get("items", []))
    if not isinstance(msgs, list):
        msgs = []
    nxt = data.get("next_since", data.get("cursor", None))
    if nxt is None:
        if msgs:
            try:
                nxt = int(msgs[-1].get("seq", int(since_seq) + len(msgs)))
            except (ValueError, TypeError, AttributeError):
                nxt = int(since_seq) + len(msgs)
        else:
            nxt = int(since_seq)
    return msgs, int(nxt)


def poll_loop(fetch_fn, start_seq=0, wait_secs=DEFAULT_WAIT, max_batches=5, backoff_secs=1.0):
    """Loop with cursor, timeout handling, and backoff. Returns (all_msgs, cursor)."""
    cursor = int(start_seq)
    out = []
    backoff = float(backoff_secs)
    if backoff < 0:
        backoff = 0.0
    errors = 0
    batches = 0
    while batches < int(max_batches):
        try:
            msgs, nxt = fetch_fn(cursor, int(wait_secs))
        except Exception:
            errors += 1
            if backoff > 0:
                pause = backoff * (2 ** min(errors - 1, 4))
                if pause > 30.0:
                    pause = 30.0
                time.sleep(pause)
            else:
                pass
            batches += 1
            continue
        errors = 0
        if msgs:
            out.extend(msgs)
        cursor = int(nxt)
        batches += 1
        if not msgs:
            continue
    return out, cursor


def save_cursor(path, seq):
    """Persist cursor int to a file."""
    p = Path(str(path))
    p.write_text(str(int(seq)), encoding="utf-8")
    return int(seq)


def load_cursor(path, default=0):
    """Load cursor int from a file, or default if missing."""
    p = Path(str(path))
    try:
        text = p.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return int(default)
    except OSError:
        return int(default)
    try:
        return int(text.split()[0])
    except (ValueError, IndexError):
        return int(default)


def lint_source(source_text):
    """Lint client source for poll mistakes. Returns list of dicts."""
    findings = []
    text = source_text if isinstance(source_text, str) else str(source_text)
    lines = text.splitlines()
    has_since = "since" in text
    has_wait_param = "wait=" in text or "wait =" in text
    lower = text.lower()
    has_while_true = "while True" in text or "while 1" in text
    has_sleep = "sleep" in text
    has_cursor_word = "cursor" in lower or "next_since" in text or "save_cursor" in text
    if has_since and not has_wait_param:
        findings.append({
            "line": find_line(lines, "since"),
            "kind": "missing-wait",
            "hint": "Add wait=10 like ?since=<seq>&wait=10",
        })
    if has_while_true and not has_sleep:
        findings.append({
            "line": find_line(lines, "while"),
            "kind": "tight-loop",
            "hint": "Add sleep or use long-poll wait=10 to avoid hot loop",
        })
    if has_while_true and has_sleep:
        small = small_sleep_found(text)
        if small:
            findings.append({
                "line": find_line(lines, "sleep"),
                "kind": "tight-loop",
                "hint": "Sleep is tiny; use wait=10 long-poll instead",
            })
    if has_since and not has_cursor_word:
        findings.append({
            "line": find_line(lines, "since"),
            "kind": "discarded-cursor",
            "hint": "Save next_since to a file and reuse as since",
        })
    return findings


def find_line(lines, needle):
    for i, line in enumerate(lines, start=1):
        if needle in line:
            return i
    return 1


def small_sleep_found(text):
    m = re.search(r"sleep\s*\(\s*([0-9.]+)", text)
    if not m:
        return False
    try:
        val = float(m.group(1))
    except ValueError:
        return False
    return val < 1.0


def attribute_latency(events):
    """Split ring gap from poll wait. Returns (rows, csv_text)."""
    ordered = sorted(list(events), key=lambda e: int(e.get("seq", 0)))
    rows = []
    prev_appended = None
    for e in ordered:
        seq = int(e.get("seq", 0))
        appended = float(e.get("appended_at", 0.0))
        poll_start = float(e.get("poll_start", appended))
        arrived = float(e.get("arrived_at", appended))
        poll_delay = arrived - poll_start
        if poll_delay < 0:
            poll_delay = 0.0
        if prev_appended is None:
            ring_gap = 0.0
        else:
            ring_gap = appended - prev_appended
            if ring_gap < 0:
                ring_gap = 0.0
        rows.append({
            "seq": seq,
            "appended_at": appended,
            "arrived_at": arrived,
            "poll_delay": round(poll_delay, 3),
            "ring_gap": round(ring_gap, 3),
        })
        prev_appended = appended
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["seq", "appended_at", "arrived_at", "poll_delay", "ring_gap"])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return rows, buf.getvalue()


def write_latency_csv(rows, path):
    """Write latency rows to a CSV file."""
    p = Path(str(path))
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["seq", "appended_at", "arrived_at", "poll_delay", "ring_gap"])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    p.write_text(buf.getvalue(), encoding="utf-8")
    return str(p)


def estimate_load(start_seq, end_seq, duration_secs, naive_every_secs=1, wait_secs=DEFAULT_WAIT):
    """Compare naive short polling vs long-poll idle calls."""
    start = int(start_seq)
    end = int(end_seq)
    total = end - start + 1
    if total < 0:
        total = 0
    duration = float(duration_secs)
    if duration < 0:
        duration = 0.0
    naive_every = float(naive_every_secs)
    if naive_every < 1:
        naive_every = 1.0
    wait = float(wait_secs)
    if wait < 1:
        wait = 10.0
    naive_calls = int(duration // naive_every) + 1
    long_calls = int(duration // wait) + 1
    saved = naive_calls - long_calls
    if saved < 0:
        saved = 0
    ratio = 0.0
    if long_calls > 0:
        ratio = round(float(naive_calls) / float(long_calls), 2)
    return {
        "start_seq": start,
        "end_seq": end,
        "total_messages": total,
        "duration_secs": duration,
        "naive_calls": naive_calls,
        "long_calls": long_calls,
        "saved_calls": saved,
        "ratio": ratio,
    }


HEARTBEAT_MARKERS = [
    "heartbeat",
    "layer online",
    "identity verified",
    "node report",
    "presence",
]

SIGNAL_MARKERS = [
    "status",
    "sync",
    "offline",
    "verif",
    "append",
    "poll",
    "latency",
    "hash",
    "ledger",
    "cursor",
    "wait",
    "since",
    "load",
    "chain",
    "ring",
    "question",
    "issue",
    "help",
]


def collapse_heartbeats(messages):
    """Group heartbeat templates, keep outliers. Returns summary dict."""
    groups = {}
    signals = []
    heart_count = 0
    total = 0
    for item in messages:
        if isinstance(item, dict):
            text = str(item.get("text", item.get("data", item.get("msg", ""))))
        else:
            text = str(item)
        total += 1
        low = text.lower()
        heart_hit = ""
        for marker in HEARTBEAT_MARKERS:
            if marker in low:
                heart_hit = marker
                break
        signal_hit = False
        for marker in SIGNAL_MARKERS:
            if marker in low:
                if heart_hit and marker == "verif":
                    continue
                signal_hit = True
                break
        if heart_hit and not signal_hit:
            heart_count += 1
            groups[heart_hit] = groups.get(heart_hit, 0) + 1
        else:
            if heart_hit:
                heart_count += 1
                groups[heart_hit] = groups.get(heart_hit, 0) + 1
                signals.append(text)
            else:
                signals.append(text)
    return {
        "total": total,
        "heartbeat_count": heart_count,
        "groups": dict(groups),
        "signals": list(signals),
        "signal_count": len(signals),
    }


def compute_digest(seq, data, prev):
    """Compute public sample digest with sha256."""
    msg = str(int(seq)) + "|" + str(data) + "|" + str(prev)
    return hashlib.sha256(msg.encode("utf-8")).hexdigest()


def make_sample_chain(payloads, start_seq=1):
    """Build a valid sample chain. Returns list of JSON strings."""
    lines = []
    prev = GENESIS
    seq = int(start_seq)
    for payload in payloads:
        data = str(payload)
        digest = compute_digest(seq, data, prev)
        rec = {"seq": seq, "data": data, "prev": prev, "digest": digest}
        lines.append(json.dumps(rec))
        prev = digest
        seq += 1
    return lines


def verify_chain(lines):
    """Verify sample JSONL chain continuity. Names first broken link."""
    items = []
    for idx, raw in enumerate(lines):
        if isinstance(raw, dict):
            items.append(raw)
            continue
        try:
            items.append(json.loads(str(raw)))
        except (ValueError, TypeError):
            return {
                "ok": False,
                "checked": idx,
                "broken_seq": None,
                "reason": "bad json at line " + str(idx + 1),
            }
    checked = 0
    prev_digest = None
    prev_seq = None
    for rec in items:
        try:
            seq = int(rec.get("seq"))
            data = str(rec.get("data", rec.get("payload", rec.get("text", ""))))
            prev = str(rec.get("prev", rec.get("prev_hash", "")))
            digest = str(rec.get("digest", rec.get("hash", "")))
        except (ValueError, TypeError, AttributeError):
            return {
                "ok": False,
                "checked": checked,
                "broken_seq": None,
                "reason": "missing fields",
            }
        expect = compute_digest(seq, data, prev)
        if digest != expect:
            return {
                "ok": False,
                "checked": checked,
                "broken_seq": seq,
                "reason": "digest mismatch at seq " + str(seq),
            }
        if prev_digest is not None:
            if prev != prev_digest:
                return {
                    "ok": False,
                    "checked": checked,
                    "broken_seq": seq,
                    "reason": "link break at seq " + str(seq),
                }
            if seq != int(prev_seq) + 1:
                return {
                    "ok": False,
                    "checked": checked,
                    "broken_seq": seq,
                    "reason": "seq gap at seq " + str(seq),
                }
        prev_digest = digest
        prev_seq = seq
        checked += 1
    return {"ok": True, "checked": checked, "broken_seq": None, "reason": "ok"}
