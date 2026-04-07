"""
MAAC Swim Club - SwimCloud Best Times Scraper
Pulls each swimmer's personal best per event (SCY + LCM).

Pipeline position:
    [maac_scraper.py] → swim_history.py → build_tables.py

Input (swimmer list, in priority order):
    ../data/swimmers.csv        — preferred, already has swimmer IDs
    ../data/maac_best_times.csv — fallback if swimmers.csv missing
    SwimCloud roster page       — last resort, requires valid session cookie

Output:
    ../data/maac_best_times.csv
    ../data/maac_best_times.xlsx

Requirements:
    pip install requests beautifulsoup4 pandas openpyxl python-dotenv

Setup:
    Create scraper/.env with your SwimCloud session cookie:
        SWIMCLOUD_SESSION=your_session_cookie_here

    To get your session cookie:
        1. Log into swimcloud.com in Chrome
        2. F12 → Application → Cookies → www.swimcloud.com
        3. Copy the value of 'sessionid'

Usage:
    cd scraper
    python maac_scraper.py
"""

import sys
import os
import time
import pandas as pd
sys.stdout.reconfigure(encoding="utf-8")
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from curl_cffi import requests

# ── Paths ─────────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()
SESSION_COOKIE = os.getenv("SWIMCLOUD_SESSION")
CF_CLEARANCE   = os.getenv("SWIMCLOUD_CF_CLEARANCE")

if not SESSION_COOKIE:
    raise SystemExit(
        "\n❌ SWIMCLOUD_SESSION not found in .env file.\n"
        "   Create scraper/.env and add:\n"
        "   SWIMCLOUD_SESSION=your_session_cookie_here\n"
        "   SWIMCLOUD_CF_CLEARANCE=your_cf_clearance_here\n"
        "   (Log into swimcloud.com → F12 → Application → Cookies → copy both values)"
    )

TEAM_ID  = "10005638"
BASE_URL = "https://www.swimcloud.com"

ROSTER_URLS = {
    "men":   f"{BASE_URL}/team/{TEAM_ID}/roster/?page=1&gender=M&season_id=29&agegroup=UNOV&sort=name",
    "women": f"{BASE_URL}/team/{TEAM_ID}/roster/?page=1&gender=F&season_id=29&agegroup=UNOV&sort=name",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer":         "https://www.swimcloud.com/",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

STROKE_MAP = {"1": "Free", "2": "Back", "3": "Breast", "4": "Fly", "5": "IM"}
COURSE_MAP = {"Y": "SCY", "L": "LCM"}


# ── Session ───────────────────────────────────────────────────────────────────

def build_session():
    s = requests.Session(impersonate="chrome124")
    s.headers.update(HEADERS)
    s.cookies.set("sessionid", SESSION_COOKIE, domain="swimcloud.com")
    s.cookies.set("sessionid", SESSION_COOKIE, domain="www.swimcloud.com")
    if CF_CLEARANCE:
        s.cookies.set("cf_clearance", CF_CLEARANCE, domain="swimcloud.com")
        s.cookies.set("cf_clearance", CF_CLEARANCE, domain="www.swimcloud.com")
    return s

SESSION = build_session()


def get(url, retries=3):
    """GET with retry. Exits immediately on 403 — retrying won't help."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=15)

            if resp.status_code == 200:
                return resp

            elif resp.status_code == 403:
                return resp   # return so caller can decide how to handle

            elif resp.status_code == 429:
                wait = 20 * (attempt + 1)
                print(f"\n  ⚠ Rate limited — waiting {wait}s...", end="", flush=True)
                time.sleep(wait)

            else:
                wait = 3 * (attempt + 1)
                print(f"\n  ⚠ HTTP {resp.status_code} — retrying in {wait}s...", end="", flush=True)
                time.sleep(wait)

        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"\n  ⚠ Error: {e} — retrying in {wait}s...", end="", flush=True)
            time.sleep(wait)

    return None


# ── Swimmer list ──────────────────────────────────────────────────────────────

def load_swimmers_from_csv():
    """Load swimmer list from existing local CSV files."""
    for fname in ["swimmers.csv", "maac_best_times.csv"]:
        fpath = os.path.join(DATA_DIR, fname)
        if os.path.exists(fpath):
            df = pd.read_csv(fpath)
            swimmers = (
                df[["name", "swimmer_id", "gender"]]
                .drop_duplicates()
                .to_dict("records")
            )
            print(f"   Loaded {len(swimmers)} swimmers from data/{fname}")
            return swimmers
    return None


def scrape_roster(gender):
    """
    Attempt to scrape roster from SwimCloud.
    Returns list of swimmer dicts, or None if blocked.
    """
    resp = get(ROSTER_URLS[gender])
    if resp is None:
        return None
    if resp.status_code == 403:
        return None   # blocked — caller will use CSV fallback

    soup     = BeautifulSoup(resp.text, "html.parser")
    swimmers = []
    for row in soup.select("table tbody tr"):
        link = row.find("a", href=lambda h: h and "/swimmer/" in h)
        if not link:
            continue
        name       = link.get_text(strip=True)
        swimmer_id = link["href"].strip("/").split("/")[-1]
        swimmers.append({"name": name, "swimmer_id": swimmer_id, "gender": gender})
    return swimmers


def get_swimmer_list():
    """
    Get swimmer list using best available source:
    1. Try SwimCloud roster (needs valid session cookie)
    2. Fall back to local CSV if roster is blocked
    """
    print(f"\n📋 Getting swimmer list...")

    # Try roster scrape first
    print(f"   Trying SwimCloud roster...", end="", flush=True)
    men   = scrape_roster("men")
    women = scrape_roster("women")

    if men is not None and women is not None:
        total = len(men) + len(women)
        if total > 0:
            print(f" ✅ {len(men)} men, {len(women)} women")
            return men + women
        print(f" empty response")
    else:
        print(f" blocked (403)")

    # Fall back to local CSV
    print(f"   Falling back to local CSV...")
    swimmers = load_swimmers_from_csv()
    if swimmers:
        print(f"   ⚠  Using existing swimmer list — new swimmers won't be included.")
        print(f"      To add new swimmers, update data/swimmers.csv manually.")
        return swimmers

    raise SystemExit(
        "❌ Could not get swimmer list from SwimCloud or local CSV.\n"
        "   Make sure data/swimmers.csv exists, or provide a valid session cookie."
    )


# ── Best times ────────────────────────────────────────────────────────────────

def get_best_times(swimmer_id, name, gender):
    """Fetch personal bests for one swimmer via SwimCloud's profile API."""
    url  = f"{BASE_URL}/api/swimmers/{swimmer_id}/profile_fastest_times/"
    resp = get(url)

    if resp is None or resp.status_code != 200:
        return []

    try:
        data = resp.json()
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    records = []
    for entry in data:
        course = COURSE_MAP.get(str(entry.get("eventcourse", "")).upper())
        if not course:
            continue

        stroke = STROKE_MAP.get(str(entry.get("eventstroke", "")))
        if not stroke:
            continue

        best_time = str(entry.get("eventtime", "")).strip()
        if not best_time or best_time == "None":
            continue

        records.append({
            "name":       name,
            "gender":     gender,
            "swimmer_id": swimmer_id,
            "distance":   entry.get("eventdistance", ""),
            "stroke":     stroke,
            "course":     course,
            "best_time":  best_time,
            "meet":       str(entry.get("name", "")).strip(),
            "date":       str(entry.get("dateofswim", "")).strip(),
        })

    return records


# ── Export ────────────────────────────────────────────────────────────────────

def export(records):
    df = pd.DataFrame(records)
    df = df[["name", "gender", "swimmer_id", "distance", "stroke",
             "course", "best_time", "meet", "date"]]
    df = df.sort_values(
        ["gender", "name", "course", "distance", "stroke"]
    ).reset_index(drop=True)

    # CSV
    csv_path = os.path.join(DATA_DIR, "maac_best_times.csv")
    df.to_csv(csv_path, index=False)
    print(f"   ✅ data/maac_best_times.csv — {len(df):,} records")

    # Excel — times forced as text to preserve MM:SS.xx format
    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Best Times"
        ws.append(list(df.columns))
        for _, row in df.iterrows():
            ws.append(list(row))

        time_col = list(df.columns).index("best_time") + 1
        for row in ws.iter_rows(min_row=2, min_col=time_col, max_col=time_col):
            for cell in row:
                cell.value         = str(cell.value)
                cell.number_format = "@"

        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = min(
                max(len(str(c.value or "")) for c in col) + 2, 40
            )

        xlsx_path = os.path.join(DATA_DIR, "maac_best_times.xlsx")
        wb.save(xlsx_path)
        print(f"   ✅ data/maac_best_times.xlsx — times stored as text")

    except ImportError:
        print("   ⚠ openpyxl not installed — skipping Excel export")
        print("     Run: pip install openpyxl")

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  MAAC SwimCloud Best Times Scraper")
    print("=" * 55)

    all_swimmers = get_swimmer_list()
    total        = len(all_swimmers)
    all_records  = []

    print(f"\n⏱  Fetching best times for {total} swimmers...\n")

    for i, swimmer in enumerate(all_swimmers, 1):
        name   = swimmer["name"]
        sid    = str(swimmer["swimmer_id"])
        gender = swimmer["gender"]
        print(f"  [{i:>3}/{total}] {name:<30} ", end="", flush=True)

        records = get_best_times(sid, name, gender)
        all_records.extend(records)
        print(f"{len(records)} times" if records else "— no times")

        time.sleep(1.5)   # polite delay — avoid triggering rate limits

    print(f"\n{'─' * 55}")
    if all_records:
        print(f"\n💾 Saving output...")
        df = export(all_records)
        print(f"\n✅ Done — {len(df):,} records across {df['name'].nunique()} swimmers")
    else:
        print("⚠ No records collected — check your session cookie.")


if __name__ == "__main__":
    main()
