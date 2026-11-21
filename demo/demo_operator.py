"""Operator console for the delay-broadcast mode.

Runs alongside the relay and demonstrates the full retraction workflow:

1. wait for packets to enter the delay queue;
2. list pending queue ids (待播编号) and the wrapping RTP sequence number of
   each original datagram;
3. pull a contiguous interval of *RTP sequence numbers* out of the queue
   (translated to queue ids from the live /pending view) and show the
   all-or-nothing result;
4. repeat a second, deliberately invalid request to show the three rejection
   reasons (already sent / already retracted / not enqueued);
5. poll /status until the hold drains and print the final counters, proving
   retracted packets were never handed to UDP and are not counted as skips.

The script deliberately uses the public HTTP API only -- it is the same
surface a human operator would use with curl.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request


def _request(base: str, method: str, path: str, payload: dict | None = None):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        base.rstrip("/") + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Delay-mode operator console demo")
    p.add_argument("--api-url", default="http://127.0.0.1:8000")
    p.add_argument("--retract-rtp", default=None,
                   help="comma-separated RTP seqs to retract; default: pick the "
                        "middle --retract-count entries of the live pending queue")
    p.add_argument("--retract-count", type=int, default=3,
                   help="how many contiguous pending packets to retract (default 3)")
    p.add_argument("--wait-packed", type=float, default=1.5,
                   help="seconds to wait for the queue to fill before acting")
    p.add_argument("--drain-timeout", type=float, default=12.0)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    wanted_rtp = (
        [int(x) for x in args.retract_rtp.split(",") if x.strip()]
        if args.retract_rtp else None)

    print(f"[operator] waiting {args.wait_packed:.1f}s for the delay queue to fill ...")
    time.sleep(args.wait_packed)

    status, body = _request(args.api_url, "GET", "/status")
    if status != 200 or not body.get("delay", {}).get("enabled"):
        print(f"[operator] ERROR: relay is not running in delay mode: {body}",
              file=sys.stderr)
        return 2
    print(f"[operator] /status: delay={json.dumps(body['delay'])}")

    # Step 1: inspect the pending queue-id -> RTP-seq mapping.
    _, body = _request(args.api_url, "GET", "/pending")
    pending = body["pending"]
    print(f"[operator] pending packets ({len(pending)}), oldest first:")
    for item in pending:
        print(f"           queue_id={item['queue_id']:<4d} rtp_seq={item['rtp_seq']:<6d} "
              f"scheduled_in={item['scheduled_in_ms']:.0f}ms")
    if not pending:
        print("[operator] ERROR: queue already empty, slow operator or delay too short",
              file=sys.stderr)
        return 2

    rtp_to_qid = {item["rtp_seq"]: item["queue_id"] for item in pending}

    # Step 2: choose the retraction interval. Explicit RTP seqs win; otherwise
    # take a middle slice of the live queue -- far enough from the head that
    # scheduling slack cannot send it first, far enough from the tail that it
    # is guaranteed to be enqueued.
    if wanted_rtp is not None:
        missing = [s for s in wanted_rtp if s not in rtp_to_qid]
        if missing:
            print(f"[operator] ERROR: RTP seqs {missing} are not currently pending",
                  file=sys.stderr)
            return 2
        qids = sorted(rtp_to_qid[s] for s in wanted_rtp)
    else:
        count = min(args.retract_count, len(pending))
        start_idx = max(0, (len(pending) - count) // 2)
        chosen = pending[start_idx:start_idx + count]
        wanted_rtp = [item["rtp_seq"] for item in chosen]
        qids = [item["queue_id"] for item in chosen]

    contiguous = qids == list(range(qids[0], qids[-1] + 1))
    if not contiguous:
        print(f"[operator] ERROR: RTP seqs {wanted_rtp} do not map to a contiguous "
              f"queue-id interval {qids}", file=sys.stderr)
        return 2

    # Step 3: the valid retraction.
    print(f"[operator] retracting RTP seqs {wanted_rtp} -> queue ids [{qids[0]}..{qids[-1]}]")
    code, body = _request(args.api_url, "POST", "/retract",
                          {"start": qids[0], "end": qids[-1]})
    print(f"[operator] POST /retract -> HTTP {code}: {json.dumps(body)}")
    if code != 200:
        print("[operator] ERROR: expected the first retraction to succeed", file=sys.stderr)
        return 2

    # Step 4: the same interval again -> already_retracted rejection, proving
    # the whole-interval validation refuses a single bad id atomically.
    code, body = _request(args.api_url, "POST", "/retract",
                          {"start": qids[0], "end": qids[-1]})
    print(f"[operator] repeating the same request -> HTTP {code}: {json.dumps(body)}")

    # Step 5: an interval reaching beyond the highest allocated id -> not_enqueued.
    _, status_body = _request(args.api_url, "GET", "/status")
    next_id = status_body["delay"]["next_queue_id"]
    code, body = _request(args.api_url, "POST", "/retract",
                          {"start": next_id, "end": next_id + 2})
    print(f"[operator] requesting never-enqueued ids [{next_id}..{next_id + 2}] "
          f"-> HTTP {code}: {json.dumps(body)}")

    # Step 6: wait for the hold to drain and show the final accounting.
    print("[operator] waiting for the delay queue to drain (timers run even if "
          "the upstream is silent) ...")
    deadline = time.monotonic() + args.drain_timeout
    while time.monotonic() < deadline:
        _, body = _request(args.api_url, "GET", "/status")
        if body["delay"]["pending_count"] == 0:
            break
        time.sleep(0.2)

    code, body = _request(args.api_url, "GET", "/status")
    stats = body["stats"]
    print("[operator] final /status:")
    print(json.dumps(body, indent=2))
    print("[operator] summary:")
    print(f"  received (both paths, incl. dupes) = {stats['received']}")
    print(f"  duplicates suppressed at merge      = {stats['duplicates']}")
    print(f"  forwarded (handed to UDP)           = {stats['forwarded']}")
    print(f"  retracted (never sent)              = {stats['retracted']}")
    print(f"  retraction rejections               = {stats['retraction_rejections']}")
    print(f"  skipped (upstream gaps only)        = {stats['skipped']}")
    # Every *unique* merged packet is either forwarded or retracted;
    # duplicates never enter the delay queue and are counted separately.
    unique = stats["received"] - stats["duplicates"] - stats["invalid"]
    assert stats["forwarded"] + stats["retracted"] == unique, (
        "every merged packet must be either forwarded or retracted")
    assert stats["skipped"] == 0, "retractions must never be counted as skips"
    print("[operator] OK: retracted packets never reached UDP; skips unaffected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
