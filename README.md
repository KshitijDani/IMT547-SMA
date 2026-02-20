# Bluesky Feed Extraction Scripts

## Setup

```bash
cd /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file with your Bluesky credentials:

```env
BLUESKY_HANDLE=your-handle.bsky.social
BLUESKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx
```

## Program 1: Feed Likes Batch

This script reads a CSV of feeds and outputs the list of user DIDs who liked each feed, plus the total like count per feed.

Input CSV must include columns:
- `feed_at_uri`
- `feed_display_name`

Run example:

```bash
python /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA/feed_likes_batch.py \
  --input /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA/data/feeds.csv \
  --output /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA/data/Feed-Users Likes.csv
```

## Program 2: Feed Reacted Users Batch

This script reads a CSV of feeds and collects unique users who reacted (liked, replied, and optionally reposted) to posts in each feed over the last N days. It outputs a per-feed list of reacted users and the total count.

Input CSV must include columns:
- `feed_at_uri`
- `feed_display_name`

Run example:

```bash
python /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA/feed_reacted_users_batch.py \
  --input /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA/data/LeftLeaningFeeds.csv \
  --output /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA/data/Feeds-Interacted-Users.csv
```

Include reposted users (optional):

```bash
python /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA/feed_reacted_users_batch.py \
  --input /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA/data/LeftLeaningFeeds.csv \
  --output /Users/kshitijdani/Desktop/Ksh_Personal_Projects/UW/IMT547-SMA/data/Feeds-Interacted-Users.csv \
  --include-reposts
```
