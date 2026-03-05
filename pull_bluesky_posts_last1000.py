import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests


BASE_URL = "https://bsky.social/xrpc/"
SESSION_ENDPOINT = "com.atproto.server.createSession"
FEED_ENDPOINT = "app.bsky.feed.getFeed"
INPUT_CSV = "Clean Left Feeds Final - Sheet1.csv"
OUTPUT_CSV = "raw_posts_last1000_per_feed.csv"
MAX_PAGES_PER_FEED = 10000
MAX_POSTS_PER_FEED = 1000


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
    df = df.rename(columns={"feed_name": "feed_display_name"})
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


def fetch_feed_posts(feed_at_uri, feed_display_name, access_jwt):
    """Fetch up to MAX_POSTS_PER_FEED recent posts for a single feed."""
    headers = {"Authorization": f"Bearer {access_jwt}"}
    url = f"{BASE_URL}{FEED_ENDPOINT}"

    all_rows = []
    seen_post_uris = set()
    seen_cursors = set()
    cursor = None
    page_count = 0
    posts_collected = 0

    while posts_collected < MAX_POSTS_PER_FEED:
        if page_count >= MAX_PAGES_PER_FEED:
            print(f"  Reached MAX_PAGES_PER_FEED ({MAX_PAGES_PER_FEED}) for {feed_display_name}")
            break

        if cursor and cursor in seen_cursors:
            print(f"  Detected repeating cursor for {feed_display_name}; stopping pagination.")
            break

        if cursor:
            seen_cursors.add(cursor)

        params = {"feed": feed_at_uri, "limit": 100}
        if cursor:
            params["cursor"] = cursor

        retry_delay = 1
        data = None
        for attempt in range(5):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                if response.status_code in {429, 500}:
                    raise requests.HTTPError(
                        f"HTTP {response.status_code} for {feed_display_name}", response=response
                    )
                response.raise_for_status()
                data = response.json()
                break
            except (requests.RequestException, requests.HTTPError) as exc:
                if attempt == 4:
                    print(f"  API error for {feed_display_name} ({feed_at_uri}) after retries: {exc}")
                    return all_rows
                time.sleep(retry_delay)
                retry_delay *= 2

        if data is None:
            break

        items = data.get("feed", [])
        if not items:
            break

        page_count += 1
        for item in items:
            post = item.get("post", {})
            author = post.get("author", {})
            post_uri = post.get("uri")
            if not post_uri or post_uri in seen_post_uris:
                continue

            created_at_str = post.get("record", {}).get("createdAt")
            created_at_dt = parse_created_at(created_at_str)
            created_at_value = created_at_dt.isoformat() if created_at_dt else created_at_str

            seen_post_uris.add(post_uri)
            all_rows.append(
                {
                    "post_uri": post_uri,
                    "post_cid": post.get("cid"),
                    "text": post.get("record", {}).get("text"),
                    "created_at": created_at_value,
                    "author_did": author.get("did"),
                    "author_handle": author.get("handle"),
                    "reply_count": post.get("replyCount"),
                    "repost_count": post.get("repostCount"),
                    "like_count": post.get("likeCount"),
                    "feed_at_uri": feed_at_uri,
                    "feed_display_name": feed_display_name,
                }
            )
            posts_collected += 1

            if posts_collected >= MAX_POSTS_PER_FEED:
                break

        print(f"Feed: {feed_display_name}")
        print(f"Posts collected: {posts_collected}")

        if posts_collected >= MAX_POSTS_PER_FEED:
            break

        cursor = data.get("cursor")
        if not cursor:
            break

        time.sleep(0.5)

    return all_rows


def main():
    access_jwt = authenticate()
    feeds = load_feeds()

    all_posts = []
    posts_per_feed = {}
    for feed in feeds:
        feed_at_uri = feed.get("feed_at_uri")
        feed_display_name = feed.get("feed_display_name")

        print(f"Processing feed: {feed_display_name}")
        rows = fetch_feed_posts(feed_at_uri, feed_display_name, access_jwt)
        print(f"Collected {len(rows)} posts")
        posts_per_feed[feed_display_name or feed_at_uri] = len(rows)
        all_posts.extend(rows)

    posts_df = pd.DataFrame(all_posts)

    if not posts_df.empty:
        posts_df = posts_df.drop_duplicates(subset=["post_uri"])

    output_path = OUTPUT_CSV
    if os.path.exists(output_path):
        base, ext = os.path.splitext(OUTPUT_CSV)
        version = 2
        while True:
            candidate = f"{base}_v{version}{ext}"
            if not os.path.exists(candidate):
                output_path = candidate
                break
            version += 1

    posts_df.to_csv(output_path, index=False)
    number_of_feeds = len(feeds)
    print("Total posts collected:", len(all_posts))
    print("Number of feeds processed:", number_of_feeds)
    print("Posts collected per feed:")
    for feed_name, post_count in posts_per_feed.items():
        print(f"  {feed_name}: {post_count}")
    print(f"Saved {len(posts_df)} unique posts to {output_path}")


if __name__ == "__main__":
    main()
