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
    This script makes ~1 API call per unique (distance, course) per swimmer.
    With 132 swimmers averaging ~8 distance/course combos each, expect 15-30 minutes.
    Do not run this unnecessarily — only needed when refreshing full history.

API note:
    SwimCloud's times_by_event endpoint ignores the stroke parameter and returns
    ALL swims at a given distance/course for a swimmer. Stroke is assigned by
    matching each returned swim time to the swimmer's closest best time per stroke.
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


# ── Time helpers ──────────────────────────────────────────────────────────────

def time_to_seconds(t):
    """Convert 'M:SS.ss' or 'SS.ss' string to float seconds. Returns None on failure."""
    try:
        s = str(t).strip()
        if ":" in s:
            m, sec = s.split(":", 1)
            return float(m) * 60 + float(sec)
        return float(s)
    except Exception:
        return None


def assign_stroke(swim_time_str, stroke_bests):
    """
    Assign a stroke to a swim by finding which stroke's best time is closest.

    stroke_bests: dict of {stroke: best_time_str}
    Returns the stroke name, or None if no match can be made.

    Because the SwimCloud times_by_event API ignores the stroke parameter and
    returns all swims at a given distance/course, we use this heuristic to
    recover the correct stroke label.
    """
    swim_secs = time_to_seconds(swim_time_str)
    if swim_secs is None:
        return None

    best_stroke = None
    min_diff    = float("inf")
    for stroke, best_t in stroke_bests.items():
        best_secs = time_to_seconds(best_t)
        if best_secs is None:
            continue
        diff = abs(swim_secs - best_secs)
        if diff < min_diff:
            min_diff    = diff
            best_stroke = stroke

    return best_stroke


# ── Event history ─────────────────────────────────────────────────────────────

def get_swims_for_distance_course(swimmer_id, distance, course):
    """
    Fetch all recorded swims for a swimmer at a given distance/course.

    Note: the stroke code in the URL is ignored by the API; any valid code
    returns the same full set of swims at that distance/course.
    """
    course_code = COURSE_TO_CODE.get(course)
    if not course_code:
        return []

    # Stroke code in URL doesn't matter — use Free as the placeholder
    event_param = f"1|{distance}|{course_code}|1"
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

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ["times", "results", "data", "swims"]:
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


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
    print("  ⚠  This script takes 15-30 minutes to complete.")
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

    # Build best-time lookup: {swimmer_id: {(distance, course): {stroke: best_time}}}
    best_lookup = {}
    for _, row in best_times.iterrows():
        sid  = int(row["swimmer_id"])
        key  = (row["distance"], row["course"])
        best_lookup.setdefault(sid, {}).setdefault(key, {})[row["stroke"]] = row["best_time"]

    print(f"\n📋 {len(swimmers)} swimmers | "
          f"{len(best_times)} events total\n")

    all_records   = []
    total_api_calls = 0

    for i, swimmer in enumerate(swimmers, 1):
        name   = swimmer["name"]
        sid    = int(swimmer["swimmer_id"])
        gender = swimmer["gender"]

        # Get unique (distance, course) combos for this swimmer
        dist_course_combos = (
            best_times[best_times["swimmer_id"] == sid][["distance", "course"]]
            .drop_duplicates()
            .to_dict("records")
        )

        stroke_bests_map = best_lookup.get(sid, {})

        print(f"\n  [{i:>3}/{len(swimmers)}] {name} ({gender}) "
              f"— {len(dist_course_combos)} distance/course combos")

        for combo in dist_course_combos:
            distance = combo["distance"]
            course   = combo["course"]
            key      = (distance, course)

            stroke_bests = stroke_bests_map.get(key, {})
            stroke_list  = list(stroke_bests.keys())

            label = f"{distance} {'/'.join(stroke_list)} {course}"
            print(f"    {label}...", end="", flush=True)

            entries = get_swims_for_distance_course(str(sid), distance, course)
            total_api_calls += 1

            # Deduplicate entries by swim ID
            seen_ids = set()
            unique_entries = []
            for e in entries:
                if not isinstance(e, dict):
                    continue
                eid = e.get("id")
                if eid and eid in seen_ids:
                    continue
                if eid:
                    seen_ids.add(eid)
                unique_entries.append(e)

            count = 0
            for entry in unique_entries:
                swim_time = str(entry.get("eventtime", "")).strip()
                if not swim_time or swim_time == "None":
                    continue

                # Assign stroke via time-matching against best times
                if len(stroke_bests) == 1:
                    stroke = stroke_list[0]
                else:
                    stroke = assign_stroke(swim_time, stroke_bests)
                    if stroke is None:
                        continue  # can't assign — skip

                split_data  = entry.get("split", {})
                splits_list = split_data.get("normalized_splittimes", []) if split_data else []
                splits      = ", ".join(str(s) for s in splits_list) if splits_list else ""

                all_records.append({
                    "name":       name,
                    "gender":     gender,
                    "swimmer_id": sid,
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
                count += 1

            print(f" {count} swims" if count else " no data")

            time.sleep(1.2)

    print(f"\n{'─' * 55}")
    print(f"  API calls made: {total_api_calls}")

    if all_records:
        print(f"\n💾 Saving output...")
        df = export(all_records)
        print(f"\n✅ Done — {len(df):,} swim records across {df['name'].nunique()} swimmers")

        # Stroke distribution summary
        print(f"\nStroke distribution:")
        for stroke, cnt in df["stroke"].value_counts().items():
            print(f"  {stroke}: {cnt:,}")
    else:
        print("⚠ No records collected — check your session cookie.")


if __name__ == "__main__":
    main()
