"""
MAAC Swim Club - Full Swim History Scraper
Pulls every recorded swim per event per swimmer, including split times.

Pipeline position:
    maac_scraper.py → [swim_history.py] → build_tables.py

Input:
    ../data/maac_best_times.csv   (from maac_scraper.py)

Output:
    ../data/maac_swim_history.xlsx
    ../data/maac_swim_history.csv

Requirements:
    pip install requests pandas openpyxl python-dotenv

Setup:
    Create scraper/.env with your SwimCloud session cookie:
        SWIMCLOUD_SESSION=your_session_cookie_here

Usage:
    cd scraper
    python swim_history.py

Note:
    This script makes ~1 API call per event per swimmer.
    With 133 swimmers averaging ~15 events each, expect 30-60 minutes.
    Do not run this unnecessarily — only needed when refreshing full history.
"""

import os
import time
import pandas as pd
from dotenv import load_dotenv
from curl_cffi import requests

# ── Paths ─────────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()
SESSION_COOKIE = os.getenv("SWIMCLOUD_SESSION")

if not SESSION_COOKIE:
    raise SystemExit(
        "\n❌ SWIMCLOUD_SESSION not found in .env file.\n"
        "   Create scraper/.env and add:\n"
        "   SWIMCLOUD_SESSION=your_session_cookie_here\n"
        "   (Log into swimcloud.com → F12 → Application → Cookies → copy sessionid)"
    )

BASE_URL = "https://www.swimcloud.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer":          "https://www.swimcloud.com/",
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
}

STROKE_TO_CODE = {"Free": "1", "Back": "2", "Breast": "3", "Fly": "4", "IM": "5"}
COURSE_TO_CODE = {"SCY": "Y", "LCM": "L"}


# ── Session ───────────────────────────────────────────────────────────────────

def build_session():
    s = requests.Session(impersonate="chrome124")
    s.headers.update(HEADERS)
    s.cookies.set("sessionid", SESSION_COOKIE, domain="swimcloud.com")
    s.cookies.set("sessionid", SESSION_COOKIE, domain="www.swimcloud.com")
    cf = os.getenv("SWIMCLOUD_CF_CLEARANCE")
    if cf:
        s.cookies.set("cf_clearance", cf, domain="swimcloud.com")
        s.cookies.set("cf_clearance", cf, domain="www.swimcloud.com")
    return s

SESSION = build_session()


def get(url, retries=3):
    """GET with retry. Exits immediately on 403."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=15)

            if resp.status_code == 200:
                return resp

            elif resp.status_code == 403:
                raise SystemExit(
                    "\n❌ 403 Forbidden — session cookie has expired.\n"
                    "   Get a fresh one:\n"
                    "     1. Log into swimcloud.com in Chrome\n"
                    "     2. F12 → Application → Cookies → www.swimcloud.com\n"
                    "     3. Copy 'sessionid' value into scraper/.env"
                )

            elif resp.status_code == 429:
                wait = 20 * (attempt + 1)
                print(f"\n  ⚠ Rate limited — waiting {wait}s...", end="", flush=True)
                time.sleep(wait)

            else:
                wait = 3 * (attempt + 1)
                print(f"\n  ⚠ HTTP {resp.status_code} — retrying in {wait}s...", end="", flush=True)
                time.sleep(wait)

        except SystemExit:
            raise
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"\n  ⚠ Error: {e} — retrying in {wait}s...", end="", flush=True)
            time.sleep(wait)

    return None


# ── Event history ─────────────────────────────────────────────────────────────

def get_event_history(swimmer_id, name, gender, distance, stroke, course):
    """Fetch all recorded swims for one swimmer/event combination."""
    stroke_code = STROKE_TO_CODE.get(stroke)
    course_code = COURSE_TO_CODE.get(course)
    if not stroke_code or not course_code:
        return []

    event_param = f"1|{distance}|{course_code}|{stroke_code}"
    url = (
        f"{BASE_URL}/api/swimmers/{swimmer_id}/times_by_event/"
        f"?event={requests.utils.quote(event_param)}"
    )

    resp = get(url)
    if resp is None:
        return []

    try:
        data = resp.json()
    except Exception:
        return []

    # Response may be a list or wrapped in a key
    times_list = []
    if isinstance(data, list):
        times_list = data
    elif isinstance(data, dict):
        for key in ["times", "results", "data", "swims"]:
            if key in data and isinstance(data[key], list):
                times_list = data[key]
                break

    records = []
    for entry in times_list:
        if not isinstance(entry, dict):
            continue

        swim_time = str(entry.get("eventtime", "")).strip()
        if not swim_time or swim_time == "None":
            continue

        split_data  = entry.get("split", {})
        splits_list = split_data.get("normalized_splittimes", []) if split_data else []
        splits      = ", ".join(str(s) for s in splits_list) if splits_list else ""

        records.append({
            "name":       name,
            "gender":     gender,
            "swimmer_id": swimmer_id,
            "distance":   distance,
            "stroke":     stroke,
            "course":     course,
            "time":       swim_time,
            "meet":       str(entry.get("name", "") or entry.get("meet_name", "")).strip(),
            "date":       str(entry.get("dateofswim", "")).strip(),
            "place":      str(entry.get("place", "")).strip(),
            "heat":       entry.get("heat", ""),
            "lane":       entry.get("lane", ""),
            "splits":     splits,
        })

    return records


# ── Export ────────────────────────────────────────────────────────────────────

def export(all_records):
    df = pd.DataFrame(all_records)
    df = df[[
        "name", "gender", "swimmer_id", "distance", "stroke", "course",
        "time", "meet", "date", "place", "heat", "lane", "splits"
    ]]
    df = df.sort_values(
        ["gender", "name", "course", "distance", "stroke", "date"]
    ).reset_index(drop=True)

    # CSV
    csv_path = os.path.join(DATA_DIR, "maac_swim_history.csv")
    df.to_csv(csv_path, index=False)
    print(f"   ✅ data/maac_swim_history.csv — {len(df):,} records")

    # Excel — times forced as text
    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Swim History"
        ws.append(list(df.columns))
        for _, row in df.iterrows():
            ws.append(list(row))

        time_col = list(df.columns).index("time") + 1
        for row in ws.iter_rows(min_row=2, min_col=time_col, max_col=time_col):
            for cell in row:
                cell.value         = str(cell.value)
                cell.number_format = "@"

        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = min(
                max(len(str(c.value or "")) for c in col) + 2, 40
            )

        xlsx_path = os.path.join(DATA_DIR, "maac_swim_history.xlsx")
        wb.save(xlsx_path)
        print(f"   ✅ data/maac_swim_history.xlsx — times stored as text")

    except ImportError:
        print("   ⚠ openpyxl not installed — run: pip install openpyxl")

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  MAAC SwimCloud Full Swim History Scraper")
    print("=" * 55)
    print("  ⚠  This script takes 30-60 minutes to complete.")
    print("  ⚠  Only run when a full history refresh is needed.")
    print("=" * 55)

    # Load best times to know which events each swimmer has
    best_times_path = os.path.join(DATA_DIR, "maac_best_times.csv")
    if not os.path.exists(best_times_path):
        raise SystemExit(
            f"❌ data/maac_best_times.csv not found.\n"
            f"   Run maac_scraper.py first."
        )

    best_times = pd.read_csv(best_times_path)
    swimmers   = (
        best_times[["name", "swimmer_id", "gender"]]
        .drop_duplicates()
        .to_dict("records")
    )

    print(f"\n📋 {len(swimmers)} swimmers | "
          f"{len(best_times)} events total\n")

    all_records  = []
    total_events = 0

    for i, swimmer in enumerate(swimmers, 1):
        name   = swimmer["name"]
        sid    = str(swimmer["swimmer_id"])
        gender = swimmer["gender"]

        swimmer_events = best_times[
            best_times["swimmer_id"] == int(sid)
        ][["distance", "stroke", "course"]].drop_duplicates()

        event_count   = len(swimmer_events)
        total_events += event_count

        print(f"\n  [{i:>3}/{len(swimmers)}] {name} ({gender}) — {event_count} events")

        for _, event_row in swimmer_events.iterrows():
            distance = event_row["distance"]
            stroke   = event_row["stroke"]
            course   = event_row["course"]

            print(f"    {distance} {stroke} {course}...", end="", flush=True)

            records = get_event_history(sid, name, gender, distance, stroke, course)

            if records:
                print(f" {len(records)} swims")
                all_records.extend(records)
            else:
                print(f" no data")

            time.sleep(1.2)

    print(f"\n{'─' * 55}")
    print(f"  Events scraped: {total_events}")

    if all_records:
        print(f"\n💾 Saving output...")
        df = export(all_records)
        print(f"\n✅ Done — {len(df):,} swim records across {df['name'].nunique()} swimmers")
    else:
        print("⚠ No records collected — check your session cookie.")


if __name__ == "__main__":
    main()
