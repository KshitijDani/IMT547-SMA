#!/usr/bin/env python3
"""
Fetch Bluesky account details and last N post texts for a list of accounts.
"""
from __future__ import annotations

import argparse
import logging
import os
from typing import Dict, Iterable, List

import pandas as pd

from atproto import Client


def get_user_data(
    client: Client,
    accounts: Iterable[str],
    post_limit: int = 15,
) -> List[Dict[str, object]]:
    """
    Fetch account details for each handle/DID in accounts.

    Returns a list of dicts with:
      - account_name
      - account_description
      - account_handle
      - last_posts (list of text from last N posts)
    """
    results: List[Dict[str, object]] = []

    for account in accounts:
        actor = str(account).strip()
        if not actor:
            continue

        profile = client.app.bsky.actor.get_profile({'actor': actor})

        feed = client.app.bsky.feed.get_author_feed({'actor': actor, 'limit': post_limit})
        posts = []
        for item in feed.feed:
            post = item.post
            text = getattr(post.record, 'text', '')
            posts.append(text)

        results.append(
            {
                'account_name': getattr(profile, 'display_name', None),
                'account_description': getattr(profile, 'description', None),
                'account_handle': getattr(profile, 'handle', None),
                'last_posts': posts,
            }
        )

    return results


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


def run_cli() -> None:
    _load_dotenv()
    parser = argparse.ArgumentParser(description="Fetch Bluesky account details from a CSV of creator_did values")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--limit", type=int, default=15, help="Number of recent posts to fetch (default 15)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    df = pd.read_csv(args.input)
    if "creator_did" not in df.columns:
        raise ValueError("Input CSV must contain a 'creator_did' column")
    if "feed_display_name" not in df.columns:
        raise ValueError("Input CSV must contain a 'feed_display_name' column")

    creator_to_feed = {}
    for _, row in df.iterrows():
        creator = str(row["creator_did"]).strip()
        feed_name = str(row["feed_display_name"]).strip()
        if creator and creator not in creator_to_feed:
            creator_to_feed[creator] = feed_name

    account_ids = sorted(creator_to_feed.keys())
    logging.info("Unique creator_did count: %d", len(account_ids))

    client = Client()
    # Expect caller to have authenticated client elsewhere or use app password envs.
    # For CLI usage, login is required via env vars.
    # Uses BLUESKY_HANDLE and BLUESKY_APP_PASSWORD from environment if set.
    import os

    handle = os.getenv("BLUESKY_HANDLE")
    app_password = os.getenv("BLUESKY_APP_PASSWORD")
    if not handle or not app_password:
        raise RuntimeError("Missing BLUESKY_HANDLE or BLUESKY_APP_PASSWORD in environment")
    client.login(handle, app_password)

    # Reset output file if it exists
    import os
    if os.path.exists(args.output):
        os.remove(args.output)

    wrote_header = False
    for creator in account_ids:
        logging.info("Extracting user data for creator: %s", creator)
        result = get_user_data(client, [creator], post_limit=args.limit)[0]
        row_out = {
            "Feed Name": creator_to_feed.get(creator),
            "Creator DID": creator,
            "Account Name": result.get("account_name"),
            "Account Description": result.get("account_description"),
            "Account Handle": result.get("account_handle"),
            "Last Posts": "|".join(result.get("last_posts", [])),
        }
        out_df = pd.DataFrame([row_out], columns=[
            "Feed Name",
            "Creator DID",
            "Account Name",
            "Account Description",
            "Account Handle",
            "Last Posts",
        ])
        out_df.to_csv(args.output, mode="a", header=not wrote_header, index=False)
        wrote_header = True


if __name__ == "__main__":
    run_cli()
