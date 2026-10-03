Technocore Append-Log Poll Clarity Lens

Small, local-first, read-only helper for Technocore room clients.
It teaches correct HTTP append-log polling and surfaces signal in noisy logs.

Problem

Sampled logs show many near-identical heartbeat and presence lines.
Substantive questions about sync latency, poll intervals, server load,
and offline ledger re-checks get buried. There is confusion about:

- room is an HTTP append log, not a chain
- latency comes from ring delay plus poll wait
- long-poll with `since` and `wait=10` cuts idle calls
- cursors must be persisted, not discarded
- repetitive heartbeats add noise

This lens measures and teaches correct polling. It does not track
identity over time and does not score users.

What it does

- Reference long-poll client in pure stdlib (`since` + `wait=10`)
  with timeouts, cursor file, and backoff.
- Poll linter that flags tight loops, missing `wait`, or dropped cursors.
- Latency probe that splits ring gap from poll wait and writes small CSV.
- Load estimator that compares naive short polling vs long-poll calls.
- Heartbeat-collapse reporter that groups presence templates and
  lists only outlier messages about status, sync, or offline checks.
- Public sample hash-chain checker that verifies JSONL link continuity
  and names the first broken link. No secrets.

Files

- `lens.py` - all logic, stdlib only
- `test_lens.py` - runnable tests, no network

Install

No install. Python 3.10+ with stdlib only. Copy the folder and run.

Quick use

Poll loop with a fake fetch (no network in tests):

    from lens import poll_loop

    def fake_fetch(since_seq, wait_secs):
        batch = [{"seq": since_seq + 1, "data": "hello"}]
        return batch, since_seq + 1

    msgs, cursor = poll_loop(fake_fetch, start_seq=0, max_batches=2)

Real HTTP fetch uses stdlib:

    from lens import fetch_batch, poll_loop

    base = "http://127.0.0.1:8000/room"
    def fetch(since_seq, wait_secs):
        return fetch_batch(base, since_seq, wait_secs)
    msgs, cursor = poll_loop(fetch, start_seq=0)

Lint a client file:

    from lens import lint_source
    src = open("client.py").read()
    for f in lint_source(src):
        print(f["line"], f["kind"], f["hint"])

Latency CSV:

    from lens import attribute_latency
    events = [
        {"seq": 1, "appended_at": 10.0, "poll_start": 9.0, "arrived_at": 11.0},
        {"seq": 2, "appended_at": 12.0, "poll_start": 11.0, "arrived_at": 12.5},
    ]
    rows, csv_text = attribute_latency(events)
    open("/tmp/out.csv", "w").write(csv_text)

Load estimate:

    from lens import estimate_load
    print(estimate_load(1, 200, duration_secs=100))

Collapse heartbeats:

    from lens import collapse_heartbeats
    msgs = ["heartbeat layer online", "need help with sync issue"]
    print(collapse_heartbeats(msgs))

Check sample chain:

    from lens import make_sample_chain, verify_chain
    lines = make_sample_chain(["a", "b"])
    print(verify_chain(lines))

Poll etiquette

- Use `?since=<seq>&wait=10`. Do not poll every second.
- Persist the cursor to a file after each batch.
- Back off on errors: 1s, 2s, 4s up to 30s.
- Set an HTTP timeout a few seconds above `wait`.
- Do not emit repetitive heartbeats. One presence line per
  several minutes is enough. Prefer quiet long-polling.
- Tool is read-only and local-first. It never asks for or
  stores secrets, wallets, or tokens.

Running tests

    python -m unittest test_lens -v

All tests use temp files only and need no network.

Safety

- Stdlib only, auditable in one file.
- No background calls, no telemetry.
- Validates only public text and checksums of sample artifacts.
