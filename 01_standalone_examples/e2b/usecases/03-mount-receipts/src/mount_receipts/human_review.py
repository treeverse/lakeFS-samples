"""Ask a human about rows the (agent-written) Phase-3 validator flagged ``ambiguous``.

Host-side only — this module is never uploaded into the E2B sandbox (see
``e2b_session.SANDBOX_MODULES``). It runs while the sandbox is paused
(``e2b_session.pause``/``resume``), so a human can answer "whenever they get to it" with
zero compute billed in between.

Three ways to get an answer, in priority order:
  1. Slack, if ``SLACK_BOT_TOKEN`` is configured — post one message per ambiguous row and
     poll the thread for a reply.
  2. An interactive CLI prompt, if stdin is a TTY (no Slack, but a human is at the terminal).
  3. Auto-decide ``HUMAN_REVIEW_DEFAULT`` (default "reject") — keeps non-interactive runs
     (``scripts/run_demo.py``, CI, docs quickstart) from blocking forever, matching this
     demo's "always completes" philosophy (see codegen.py's self-repair -> reference-fallback).

The validator decides "ambiguous" for whatever reason it sees fit — RULES_SPEC rule 7 is
deliberately open-ended, not a fixed list — so every pending row is asked the same way:
approve, reject, or name a currency. Naming a currency is always offered because it's the
one kind of correction this demo can act on mechanically (the validator itself has no
network access and can't look up an exchange rate); if the row wasn't actually a currency
problem, a human just won't use that option.
"""
from __future__ import annotations

import sys
import time

from mount_receipts.validation import FX_TO_USD

# Currencies a human can name to resolve a row: USD (no conversion needed) plus everything
# the fixed demo rate table can convert.
RECOGNIZED_CURRENCIES = ("USD",) + tuple(FX_TO_USD.keys())


def _format_message(item: dict) -> str:
    reasons = "; ".join(item.get("reasons") or []) or "flagged ambiguous"
    others = ", ".join(c for c in RECOGNIZED_CURRENCIES if c != "USD")
    return (
        f"{item['source_file']}: the agent's validator flagged this ambiguous — {reasons}. "
        f"Approve, reject, or — if this is a currency issue — name the actual currency "
        f"({others}, or USD)."
    )


def _parse_decision(text: str) -> str | None:
    """Extract a decision from free-text reply. "reject" wins if present; a named currency
    is checked next; "approve" is the fallback match so it doesn't shadow a currency code
    mentioned alongside it."""
    lowered = (text or "").strip().lower()
    if "reject" in lowered:
        return "reject"
    for code in RECOGNIZED_CURRENCIES:
        if code.lower() in lowered:
            return code
    if "approve" in lowered:
        return "approve"
    return None


def _ask_via_slack(message: str, cfg) -> dict:
    from slack_sdk import WebClient

    client = WebClient(token=cfg.slack_bot_token)
    posted = client.chat_postMessage(channel=cfg.slack_human_channel, text=message)
    channel_id, ts = posted["channel"], posted["ts"]

    deadline = time.monotonic() + cfg.human_review_timeout_s
    while time.monotonic() < deadline:
        time.sleep(cfg.human_review_poll_s)
        replies = client.conversations_replies(channel=channel_id, ts=ts)
        for m in replies.get("messages", [])[1:]:  # [0] is our own posted message
            decision = _parse_decision(m.get("text", ""))
            if decision:
                return {"decision": decision, "note": m.get("text", "")}

    client.chat_postMessage(
        channel=channel_id, thread_ts=ts,
        text=f"No reply within {cfg.human_review_timeout_s}s — defaulting to '{cfg.human_review_default}'.",
    )
    return {"decision": cfg.human_review_default, "note": "timeout — no human reply"}


def _ask_via_cli(message: str, cfg) -> dict:
    print(f"\n  [human review] {message}")
    raw = input(f"  decision (default {cfg.human_review_default}): ").strip()
    decision = _parse_decision(raw)
    if decision is None:
        return {"decision": cfg.human_review_default, "note": f"unrecognised input {raw!r} — used default"}
    return {"decision": decision, "note": raw}


def ask_human(item: dict, cfg) -> dict:
    """Get a decision for one pending item. Returns ``{"decision", "note"}``."""
    message = _format_message(item)

    if cfg.slack_bot_token:
        try:
            return _ask_via_slack(message, cfg)
        except Exception as exc:  # slack_sdk import error, network error, bad token, ...
            print(f"  Slack ask_human failed ({exc}) — falling back to '{cfg.human_review_default}'.")
            return {"decision": cfg.human_review_default, "note": f"slack error: {exc}"}

    if sys.stdin.isatty():
        return _ask_via_cli(message, cfg)

    print(
        f"  [human review] {item['source_file']}: no Slack configured and not running "
        f"interactively — auto-deciding '{cfg.human_review_default}'."
    )
    return {"decision": cfg.human_review_default, "note": "no reviewer available"}


def collect_decisions(cfg, pending: list[dict]) -> dict[str, dict]:
    """Ask a human about every pending item. Returns ``{source_file: {"decision", "note"}}``."""
    return {item["source_file"]: ask_human(item, cfg) for item in pending}
