from playwright.sync_api import sync_playwright
import pandas as pd

LEFT_SEARCH_KEYWORDS = [
    "Leftist","Leftists","Left wing","Progressive","Progressives","Liberal","Liberals",
    "Democrat","Democrats","Dems",
    "Democratic socialist","Democratic Socialists","Social democrat","Social democracy",
    "The resistance","Lib Dem", "Trump", "Anti Capital", "Republican Cri", "MAGAC",
    "Lefty","Lefties","Center left","Center-left politics", "Democracy", "Democratic", "Congress",
    "Liberal politics","Progressive politics","US liberal","US progressive","Democratic Party feed"
]

def collect_query_feed_links(page, query: str, scrolls: int = 8) -> set[str]:
    # locate the search input on /feeds
    candidates = [
        "input[type='search']",
        "input[placeholder*='Search' i]",
        "input[aria-label*='Search' i]",
    ]
    search = None
    for sel in candidates:
        loc = page.locator(sel)
        if loc.count() and loc.first.is_visible():
            search = loc.first
            break
    if search is None:
        raise RuntimeError("Could not find search input on /feeds")

    # run search
    search.click()
    search.fill(query)
    page.keyboard.press("Enter")
    page.wait_for_timeout(1800)

    feed_urls = set()

    def extract_visible():
        urls = set()
        links = page.locator("a[href*='/profile/'][href*='/feed/']")
        for i in range(links.count()):
            href = links.nth(i).get_attribute("href")
            if not href:
                continue
            full = "https://bsky.app" + href
            # Drop Trending feeds block that can appear on the right
            if "trending.bsky.app" in full:
                continue
            urls.add(full)
        return urls

    # collect initial viewport
    feed_urls |= extract_visible()

    # scroll and stop once the Trending block is visible (prevents grabbing trending links)
    for _ in range(scrolls):
        if page.locator("text=Trending").count() and page.locator("text=Trending").first.is_visible():
            break
        page.mouse.wheel(0, 1800)
        page.wait_for_timeout(850)
        feed_urls |= extract_visible()

    return feed_urls

def scrape_feeds_from_keywords(keywords, scrolls=8, headless=False):
    all_urls = set()
    per_query_counts = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto("https://bsky.app/feeds", wait_until="domcontentloaded")
        page.wait_for_timeout(1200)

        for kw in keywords:
            urls = collect_query_feed_links(page, kw, scrolls=scrolls)
            per_query_counts[kw] = len(urls)
            all_urls |= urls
            print(f"{kw:22s}  +{len(urls):3d}   total_unique={len(all_urls)}")

        browser.close()

    return sorted(all_urls), per_query_counts

feed_urls, per_query = scrape_feeds_from_keywords(LEFT_SEARCH_KEYWORDS, scrolls=10, headless=False)

print("\nFINAL unique feed URLs:", len(feed_urls))
print("\nTop 10 queries by yield:")
for k, v in sorted(per_query.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"{v:3d}  {k}")

print("\nSample URLs:")
for u in feed_urls[:25]:
    print(u)

df = pd.DataFrame(feed_urls,columns=['FeedURL'])
df.to_csv('scraped_feeds.csv',index=False)