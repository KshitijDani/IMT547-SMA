import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests


BASE_URL = "https://bsky.social/xrpc/"
SESSION_ENDPOINT = "com.atproto.server.createSession"
FEED_ENDPOINT = "app.bsky.feed.getFeed"
INPUT_CSV = "LeftLeaningFeeds-2.csv"
OUTPUT_CSV = "raw_posts_2weeks.csv"


def authenticate():
    """Create an AT Protocol session and return the access JWT."""
    handle = os.getenv("BLUESKY_HANDLE")
    app_password = os.getenv("BLUESKY_APP_PASSWORD")

    if not handle or not app_password:
        raise EnvironmentError(
            "Missing required environment variables: BLUESKY_HANDLE and/or BLUESKY_APP_PASSWORD"
        )

    url = f"{BASE_URL}{SESSION_ENDPOINT}"
    payload = {"identifier": handle, "password": app_password}

    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()

    data = response.json()
    access_jwt = data.get("accessJwt")
    if not access_jwt:
        raise RuntimeError("Authentication succeeded but accessJwt was missing from response")

    return access_jwt


def load_feeds():
    """Load feed URIs and display names from input CSV."""
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    required_cols = {"feed_at_uri", "feed_display_name"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in {INPUT_CSV}: {', '.join(sorted(missing_cols))}")

    feeds_df = df[["feed_at_uri", "feed_display_name"]].dropna(subset=["feed_at_uri"])
    return feeds_df.to_dict(orient="records")


def parse_created_at(value):
    """Parse Bluesky timestamp into timezone-aware UTC datetime."""
    if not value:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def fetch_feed_posts(feed_at_uri, feed_display_name, access_jwt, cutoff_utc):
    """Fetch posts for a single feed, keeping only items within cutoff window."""
    headers = {"Authorization": f"Bearer {access_jwt}"}
    url = f"{BASE_URL}{FEED_ENDPOINT}"

    all_rows = []
    cursor = None
    reached_older_posts = False

    while True:
        params = {"feed": feed_at_uri, "limit": 100}
        if cursor:
            params["cursor"] = cursor

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            print(f"  API error for {feed_display_name} ({feed_at_uri}): {exc}")
            break

        items = data.get("feed", [])
        for item in items:
            post = item.get("post", {})
            author = post.get("author", {})
            created_at_str = post.get("record", {}).get("createdAt")
            created_at_dt = parse_created_at(created_at_str)

            # Feed results are newest -> oldest; once we hit a post older than cutoff,
            # we can stop pagination early because remaining posts will also be older.
            if created_at_dt and created_at_dt < cutoff_utc:
                reached_older_posts = True
                break

            if created_at_dt and created_at_dt >= cutoff_utc:
                all_rows.append(
                    {
                        "post_uri": post.get("uri"),
                        "post_cid": post.get("cid"),
                        "text": post.get("record", {}).get("text"),
                        "created_at": created_at_dt.isoformat(),
                        "author_did": author.get("did"),
                        "author_handle": author.get("handle"),
                        "reply_count": post.get("replyCount"),
                        "repost_count": post.get("repostCount"),
                        "like_count": post.get("likeCount"),
                        "feed_at_uri": feed_at_uri,
                        "feed_display_name": feed_display_name,
                    }
                )

        if reached_older_posts:
            break

        cursor = data.get("cursor")
        if not cursor:
            break

        time.sleep(0.5)

    return all_rows


def main():
    access_jwt = authenticate()
    feeds = load_feeds()

    now_utc = datetime.now(timezone.utc)
    cutoff_utc = now_utc - timedelta(days=14)

    all_posts = []
    for feed in feeds:
        feed_at_uri = feed.get("feed_at_uri")
        feed_display_name = feed.get("feed_display_name", "")

        print(f"Processing feed: {feed_display_name} ({feed_at_uri})")
        rows = fetch_feed_posts(feed_at_uri, feed_display_name, access_jwt, cutoff_utc)
        print(f"  Collected {len(rows)} posts")
        all_posts.extend(rows)

    posts_df = pd.DataFrame(all_posts)

    if not posts_df.empty:
        posts_df = posts_df.drop_duplicates(subset=["post_uri"])

    output_path = OUTPUT_CSV
    if os.path.exists(output_path):
        output_path = "raw_posts_2weeks_v2.csv"

    posts_df.to_csv(output_path, index=False)
    print(f"Saved {len(posts_df)} unique posts to {output_path}")


if __name__ == "__main__":
    main()
