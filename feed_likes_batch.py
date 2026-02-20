#!/usr/bin/env python3
"""
Batch process feed URIs from a CSV and write likes + user DIDs.

Input CSV (in ./data): must include columns:
  - feed_at_uri
  - feed_display_name

Output CSV (in ./data):
  - Feed At URI
  - Feed Display Name
  - User like count
  - Users
"""
from __future__ import annotations

import os
import argparse
import logging
from typing import List, Optional, Tuple

import pandas as pd
from atproto import Client


def _build_feed_uri(
    feed_uri: Optional[str],
    feed_at_uri: Optional[str],
    feed_did: Optional[str],
    feed_rkey: Optional[str],
) -> str:
    if feed_uri:
        if not feed_uri.startswith("at://"):
            raise ValueError("feed-uri must start with 'at://'")
        return feed_uri
    if feed_at_uri:
        if not feed_at_uri.startswith("at://"):
            raise ValueError("feed-at-uri must start with 'at://'")
        return feed_at_uri
    if not feed_did or not feed_rkey:
        raise ValueError("Provide --feed-uri/--feed-at-uri OR both --feed-did and --feed-rkey")
    return f"at://{feed_did}/app.bsky.feed.generator/{feed_rkey}"


def _get_feed_subject(client: Client, feed_uri: str) -> Tuple[str, str]:
    resp = client.app.bsky.feed.get_feed_generator({'feed': feed_uri})
    view = resp.view
    return view.uri, view.cid


def get_feed_liker_dids(
    client: Client,
    feed_uri: Optional[str] = None,
    feed_at_uri: Optional[str] = None,
    feed_did: Optional[str] = None,
    feed_rkey: Optional[str] = None,
    limit: int = 100,
) -> Tuple[List[str], int]:
    feed_uri = _build_feed_uri(feed_uri, feed_at_uri, feed_did, feed_rkey)
    logging.info("Resolving feed: %s", feed_uri)
    subject_uri, subject_cid = _get_feed_subject(client, feed_uri)
    logging.info("Resolved feed subject uri=%s cid=%s", subject_uri, subject_cid)

    liker_dids: List[str] = []
    cursor: Optional[str] = None

    page = 1
    while True:
        params = {
            'uri': subject_uri,
            'cid': subject_cid,
            'limit': limit,
        }
        if cursor:
            params['cursor'] = cursor

        logging.info("Fetching likes page %d (limit=%d)%s", page, limit, " with cursor" if cursor else "")
        resp = client.app.bsky.feed.get_likes(params)
        for like in resp.likes:
            liker_dids.append(like.actor.did)

        logging.info("Page %d: +%d likes (total=%d)", page, len(resp.likes), len(liker_dids))
        cursor = resp.cursor
        if not cursor:
            logging.info("No more pages. Done.")
            break
        page += 1

    return liker_dids, len(liker_dids)


def _load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def _require_columns(df: pd.DataFrame, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def main() -> None:
    _load_dotenv()

    parser = argparse.ArgumentParser(description="Batch fetch feed likes from a CSV")
    parser.add_argument("--input", default="data/feeds.csv", help="Input CSV path (default: data/feeds.csv)")
    parser.add_argument("--output", default="data/Feed-Users Likes.csv", help="Output CSV path (default: data/Feed-Users Likes.csv)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.info("Program started")

    handle = os.getenv("BLUESKY_HANDLE")
    app_password = os.getenv("BLUESKY_APP_PASSWORD")
    if not handle or not app_password:
        raise RuntimeError("Missing BLUESKY_HANDLE or BLUESKY_APP_PASSWORD in .env")

    logging.info("Reading input CSV: %s", args.input)
    df = pd.read_csv(args.input)
    _require_columns(df, ["feed_at_uri", "feed_display_name"])

    if os.path.exists(args.output):
        os.remove(args.output)

    client = Client()
    client.login(handle, app_password)

    wrote_header = False
    for _, row in df.iterrows():
        feed_at_uri = str(row["feed_at_uri"]).strip()
        feed_display_name = str(row["feed_display_name"]).strip()

        if not feed_at_uri:
            continue

        logging.info("Fetching likers for feed: %s", feed_at_uri)
        dids, total = get_feed_liker_dids(client, feed_at_uri=feed_at_uri)
        row_out = {
            "Feed At URI": feed_at_uri,
            "Feed Display Name": feed_display_name,
            "User like count": total,
            "Users": ";".join(dids),
        }
        out_df = pd.DataFrame([row_out], columns=["Feed At URI", "Feed Display Name", "User like count", "Users"])
        if not wrote_header:
            logging.info("Writing output CSV: %s", args.output)
        out_df.to_csv(args.output, mode="a", header=not wrote_header, index=False)
        wrote_header = True


if __name__ == "__main__":
    main()
