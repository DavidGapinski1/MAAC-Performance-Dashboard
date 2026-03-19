"""
MAAC Swim Club - Full Swim History Scraper
Uses times_by_event endpoint to get every recorded swim per event per swimmer.

Pipeline:
  1. Load maac_best_times.csv (already scraped)
  2. For each swimmer, loop through their recorded events
  3. Hit /api/swimmers/{id}/times_by_event/?event={orgcode}|{distance}|{course}|{stroke}
  4. Output every individual swim with splits

Requirements:
    pip install requests pandas openpyxl python-dotenv

Setup:
    Create a .env file in the same folder with:
    SWIMCLOUD_SESSION=your_session_cookie_here

Usage:
    py -3.9 swim_history.py

Output:
    maac_swim_history.csv
    maac_swim_history.xlsx
"""

import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
SESSION_COOKIE = os.getenv("SWIMCLOUD_SESSION")

if not SESSION_COOKIE:
    raise ValueError("SWIMCLOUD_SESSION not found in .env file")

BASE_URL = "https://www.swimcloud.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.swimcloud.com/",
    "Accept": "application/json, text/plain, */*",
    "X-Requested-With": "XMLHttpRequest",
}

# Reverse maps to get API codes from readable values
STROKE_TO_CODE = {
    "Free": "1",
    "Back": "2",
    "Breast": "3",
    "Fly": "4",
    "IM": "5",
}

COURSE_TO_CODE = {
    "SCY": "Y",
    "LCM": "L",
}

STROKE_MAP = {
    "1": "Free",
    "2": "Back",
    "3": "Breast",
    "4": "Fly",
    "5": "IM",
}

COURSE_MAP = {
    "Y": "SCY",
    "L": "LCM",
}


# ── Session ───────────────────────────────────────────────────────────────────

def new_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    s.cookies.set("sessionid", SESSION_COOKIE, domain="www.swimcloud.com")
    try:
        s.get(BASE_URL, timeout=10)
    except Exception:
        pass
    return s

session = new_session()
request_count = 0

def api_get(url, retries=3):
    global session, request_count

    if request_count > 0 and request_count % 20 == 0:
        print(f"\n  ♻ Refreshing session after {request_count} requests...")
        session = new_session()
        time.sleep(3)

    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=15)
            request_count += 1

            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"\n  ⚠ Rate limited (429) — waiting {wait}s...", end="")
                time.sleep(wait)
            elif resp.status_code == 403:
                print(f"\n  ⚠ 403 Forbidden — refreshing session...", end="")
                session = new_session()
                time.sleep(5)
            else:
                print(f"\n  ⚠ HTTP {resp.status_code}", end="")
                time.sleep(2)
        except Exception as e:
            print(f"\n  ⚠ Request error: {e}", end="")
            time.sleep(3)

    return None


# ── Fetch all swims for one event ─────────────────────────────────────────────

def get_event_history(swimmer_id, name, gender, distance, stroke, course):
    stroke_code = STROKE_TO_CODE.get(stroke)
    course_code = COURSE_TO_CODE.get(course)

    if not stroke_code or not course_code:
        return []

    # Event param format: orgcode|distance|course|stroke
    event_param = f"1|{distance}|{course_code}|{stroke_code}"
    url = f"{BASE_URL}/api/swimmers/{swimmer_id}/times_by_event/?event={requests.utils.quote(event_param)}"

    resp = api_get(url)
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
        meet      = str(entry.get("name", "") or entry.get("meet_name", "")).strip()
        date      = str(entry.get("dateofswim", "")).strip()
        place     = str(entry.get("place", "")).strip()
        heat      = entry.get("heat", "")
        lane      = entry.get("lane", "")

        split_data  = entry.get("split", {})
        splits_list = split_data.get("normalized_splittimes", []) if split_data else []
        splits      = ", ".join(str(s) for s in splits_list) if splits_list else ""

        if not swim_time or swim_time == "None":
            continue

        records.append({
            "name":       name,
            "gender":     gender,
            "swimmer_id": swimmer_id,
            "distance":   distance,
            "stroke":     stroke,
            "course":     course,
            "time":       swim_time,
            "meet":       meet,
            "date":       date,
            "place":      place,
            "heat":       heat,
            "lane":       lane,
            "splits":     splits,
        })

    return records


# ── Export ────────────────────────────────────────────────────────────────────

def export(all_records):
    df = pd.DataFrame(all_records)
    df = df[["name", "gender", "swimmer_id", "distance", "stroke", "course",
             "time", "meet", "date", "place", "heat", "lane", "splits"]]
    df = df.sort_values(["gender", "name", "course", "distance", "stroke", "date"]).reset_index(drop=True)

    df.to_csv("maac_swim_history.csv", index=False)
    print(f"✅ CSV saved: maac_swim_history.csv")

    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Swim History"
        ws.append(list(df.columns))
        for _, row in df.iterrows():
            ws.append(list(row))

        time_col = list(df.columns).index("time") + 1
        for col in ws.iter_cols(min_col=time_col, max_col=time_col, min_row=2):
            for c in col:
                c.value = str(c.value)
                c.number_format = "@"

        for col in ws.columns:
            width = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(width + 2, 40)

        wb.save("maac_swim_history.xlsx")
        print(f"✅ Excel saved: maac_swim_history.xlsx (times as text)")
    except ImportError:
        print("⚠ openpyxl not found — run: pip install openpyxl")

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load best times CSV to know which events each swimmer has
    try:
        best_times = pd.read_csv("maac_best_times.csv")
    except FileNotFoundError:
        raise FileNotFoundError("maac_best_times.csv not found. Run maac_scraper.py first.")

    # Get unique swimmers
    swimmers = best_times[["name", "swimmer_id", "gender"]].drop_duplicates().to_dict("records")
    print(f"Starting full swim history scraper...")
    print(f"Found {len(swimmers)} swimmers in best times dataset")

    all_records = []
    total_events = 0

    for i, swimmer in enumerate(swimmers):
        name       = swimmer["name"]
        sid        = str(swimmer["swimmer_id"])
        gender     = swimmer["gender"]

        # Get this swimmer's events from best times
        swimmer_events = best_times[best_times["swimmer_id"] == int(sid)][["distance", "stroke", "course"]].drop_duplicates()
        event_count = len(swimmer_events)
        total_events += event_count

        print(f"\n  [{i+1}/{len(swimmers)}] {name} ({gender}) — {event_count} events")

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

            time.sleep(1)

    print(f"\nTotal events scraped: {total_events}")

    if all_records:
        df = export(all_records)
        print(f"Total swim records: {len(df)}")
        print(df.head(20).to_string(index=False))
    else:
        print("\n⚠ No records collected.")


if __name__ == "__main__":
    main()
