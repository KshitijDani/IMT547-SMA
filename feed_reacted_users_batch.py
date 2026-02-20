#!/usr/bin/env python3
"""
Batch process feed URIs from a CSV and collect unique users who reacted
(liked, reposted, or replied) to posts in the last N days.

Input CSV (in ./data): must include columns:
  - feed_at_uri
  - feed_display_name

Output CSV (in ./data):
  - Feed At URI
  - Feed Display Name
  - Reacted user count
  - Reacted users
"""
from __future__ import annotations

import os
import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Set

import pandas as pd
from atproto import Client

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


def _parse_iso_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _paginate_likes(client: Client, uri: str, cid: Optional[str], limit: int = 100) -> Iterable[str]:
    cursor = None
    while True:
        params = {'uri': uri, 'limit': limit}
        if cid:
            params['cid'] = cid
        if cursor:
            params['cursor'] = cursor
        resp = client.app.bsky.feed.get_likes(params)
        for like in resp.likes:
            yield like.actor.did
        cursor = resp.cursor
        if not cursor:
            break


def _paginate_reposts(client: Client, uri: str, limit: int = 100) -> Iterable[str]:
    cursor = None
    while True:
        params = {'uri': uri, 'limit': limit}
        if cursor:
            params['cursor'] = cursor
        resp = client.app.bsky.feed.get_reposted_by(params)
        for rep in resp.reposted_by:
            yield rep.did
        cursor = resp.cursor
        if not cursor:
            break


def _collect_reply_dids(thread) -> Set[str]:
    dids: Set[str] = set()
    replies = getattr(thread, 'replies', None)
    if not replies:
        return dids
    for item in replies:
        post = getattr(item, 'post', None)
        if post and getattr(post, 'author', None):
            dids.add(post.author.did)
        dids.update(_collect_reply_dids(item))
    return dids


def _get_reply_dids(client: Client, uri: str, depth: int = 6) -> Set[str]:
    params = {'uri': uri, 'depth': depth}
    thread = client.app.bsky.feed.get_post_thread(params).thread
    return _collect_reply_dids(thread)


def _get_feed_posts_last_days(client: Client, feed_at_uri: str, days: int = 7, limit: int = 50) -> List[tuple]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    posts: List[tuple] = []
    cursor = None

    while True:
        params = {'feed': feed_at_uri, 'limit': limit}
        if cursor:
            params['cursor'] = cursor
        resp = client.app.bsky.feed.get_feed(params)

        if not resp.feed:
            break

        stop = False
        for item in resp.feed:
            post = item.post
            created_at = _parse_iso_datetime(getattr(post.record, 'created_at', '') or '')
            if not created_at:
                created_at = _parse_iso_datetime(getattr(post, 'indexed_at', '') or '')
            if created_at and created_at < cutoff:
                stop = True
                break
            posts.append((post.uri, post.cid))

        if stop:
            break

        cursor = resp.cursor
        if not cursor:
            break

    return posts


def main() -> None:
    _load_dotenv()

    parser = argparse.ArgumentParser(description="Batch fetch reacted users from feeds in a CSV")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", default="data/Feed-Reacted Users.csv", help="Output CSV path (default: data/Feed-Reacted Users.csv)")
    parser.add_argument("--days", type=int, default=7, help="Look back N days for posts (default 7)")
    parser.add_argument("--reply-depth", type=int, default=6, help="Reply thread depth to scan (default 6)")
    parser.add_argument(
        "--include-reposts",
        action="store_true",
        help="Include users who reposted posts (disabled by default for speed)",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    # Silence verbose HTTP client logs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    handle = os.getenv("BLUESKY_HANDLE")
    app_password = os.getenv("BLUESKY_APP_PASSWORD")
    if not handle or not app_password:
        raise RuntimeError("Missing BLUESKY_HANDLE or BLUESKY_APP_PASSWORD in .env")

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

        logging.info("Selected feed: %s (%s)", feed_display_name, feed_at_uri)
        reacted_users: Set[str] = set()
        posts = _get_feed_posts_last_days(client, feed_at_uri, days=args.days)
        for uri, cid in posts:
            for did in _paginate_likes(client, uri, cid):
                reacted_users.add(did)
            logging.info("Extracted liked users for post: %s", uri)
            if args.include_reposts:
                for did in _paginate_reposts(client, uri):
                    reacted_users.add(did)
                logging.info("Extracted reposted users for post: %s", uri)
            for did in _get_reply_dids(client, uri, depth=args.reply_depth):
                reacted_users.add(did)
            logging.info("Extracted replied users for post: %s", uri)

        row_out = {
            "Feed At URI": feed_at_uri,
            "Feed Display Name": feed_display_name,
            "Reacted user count": len(reacted_users),
            "Reacted users": ";".join(sorted(reacted_users)),
        }
        out_df = pd.DataFrame([row_out], columns=["Feed At URI", "Feed Display Name", "Reacted user count", "Reacted users"])
        out_df.to_csv(args.output, mode="a", header=not wrote_header, index=False)
        logging.info("Added feed data to CSV: %s (%s)", feed_display_name, feed_at_uri)
        wrote_header = True


if __name__ == "__main__":
    main()
