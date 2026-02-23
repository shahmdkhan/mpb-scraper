import json
from scrapling.fetchers import StealthySession


headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'content-language': 'nl_NL',
        'priority': 'u=1, i',
        'referer': 'https://www.mpb.com/nl-nl/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    }

COOKIE_FILE = "input/mpb_cookies.json"

with StealthySession(
        headless=True,
        geoip=True,
        humanize=True,
        solve_cloudflare=True,
        load_dom=True
) as session:

    response = session.fetch(url='https://www.mpb.com/nl-nl', headers=headers)

    # Extract cookies
    cookies_dict = {cookie["name"]: cookie["value"] for cookie in session.context.cookies()}

    # Save to file
    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(cookies_dict, f, indent=4)

    print("Cookies saved successfully.")