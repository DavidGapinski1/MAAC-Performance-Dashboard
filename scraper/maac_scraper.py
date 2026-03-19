"""
MAAC Swim Club - SwimCloud Best Times Scraper v10
Splits event into separate distance and stroke columns.

Columns: name, gender, swimmer_id, distance, stroke, course, best_time, meet, date

Requirements:
    pip install requests beautifulsoup4 pandas openpyxl

Usage:
    py -3.9 maac_scraper_v10.py

Output:
    maac_best_times.csv
    maac_best_times.xlsx
"""

import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

TEAM_ID = "10005638"
BASE_URL = "https://www.swimcloud.com"

ROSTER_URLS = {
    "men":   f"{BASE_URL}/team/{TEAM_ID}/roster/?page=1&gender=M&season_id=29&agegroup=UNOV&sort=name",
    "women": f"{BASE_URL}/team/{TEAM_ID}/roster/?page=1&gender=F&season_id=29&agegroup=UNOV&sort=name",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.swimcloud.com/",
    "Accept": "application/json, text/plain, */*",
    "X-Requested-With": "XMLHttpRequest",
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
    "S": "SCM",
}


# ── Session ───────────────────────────────────────────────────────────────────

def new_session():
    s = requests.Session()
    s.headers.update(HEADERS)
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


# ── Roster ────────────────────────────────────────────────────────────────────

def get_roster(gender="men"):
    resp = session.get(ROSTER_URLS[gender], timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")
    swimmers = []
    for row in soup.select("table tbody tr"):
        link = row.find("a", href=lambda h: h and "/swimmer/" in h)
        if not link:
            continue
        name = link.get_text(strip=True)
        swimmer_id = link["href"].strip("/").split("/")[-1]
        swimmers.append({"name": name, "swimmer_id": swimmer_id, "gender": gender})
    return swimmers


# ── Times ─────────────────────────────────────────────────────────────────────

def get_best_times(swimmer_id, name, gender):
    url = f"{BASE_URL}/api/swimmers/{swimmer_id}/profile_fastest_times/"
    resp = api_get(url)

    if resp is None:
        return []

    try:
        data = resp.json()
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    records = []
    for entry in data:
        course_code = str(entry.get("eventcourse", "")).upper()
        course = COURSE_MAP.get(course_code)
        if not course or course == "SCM":
            continue

        distance    = entry.get("eventdistance", "")
        stroke_code = str(entry.get("eventstroke", ""))
        stroke      = STROKE_MAP.get(stroke_code, stroke_code)
        best_time   = str(entry.get("eventtime", "")).strip()
        meet        = str(entry.get("name", "")).strip()
        date        = str(entry.get("dateofswim", "")).strip()

        if not best_time or best_time == "None":
            continue

        records.append({
            "name":       name,
            "gender":     gender,
            "swimmer_id": swimmer_id,
            "distance":   distance,
            "stroke":     stroke,
            "course":     course,
            "best_time":  best_time,
            "meet":       meet,
            "date":       date,
        })

    return records


# ── Export ────────────────────────────────────────────────────────────────────

def export(all_records):
    df = pd.DataFrame(all_records)
    df = df[["name", "gender", "swimmer_id", "distance", "stroke", "course", "best_time", "meet", "date"]]
    df = df.sort_values(["gender", "name", "course", "distance", "stroke"]).reset_index(drop=True)

    df.to_csv("maac_best_times.csv", index=False)
    print(f"✅ CSV saved: maac_best_times.csv")

    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Best Times"
        ws.append(list(df.columns))
        for _, row in df.iterrows():
            ws.append(list(row))

        # Force best_time column as text
        time_col = list(df.columns).index("best_time") + 1
        for col in ws.iter_cols(min_col=time_col, max_col=time_col, min_row=2):
            for c in col:
                c.value = str(c.value)
                c.number_format = "@"

        for col in ws.columns:
            width = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(width + 2, 40)

        wb.save("maac_best_times.xlsx")
        print(f"✅ Excel saved: maac_best_times.xlsx (times as text)")
    except ImportError:
        print("⚠ openpyxl not found — run: pip install openpyxl")

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Starting MAAC SwimCloud scraper v10...")

    print("\n📋 Fetching men's roster...")
    men = get_roster("men")
    print(f"   Found {len(men)} men")

    print("📋 Fetching women's roster...")
    women = get_roster("women")
    print(f"   Found {len(women)} women")

    all_swimmers = men + women
    all_records  = []

    print(f"\n⏱ Fetching best times for {len(all_swimmers)} swimmers...")

    for i, swimmer in enumerate(all_swimmers):
        name   = swimmer["name"]
        sid    = swimmer["swimmer_id"]
        gender = swimmer["gender"]
        print(f"  [{i+1}/{len(all_swimmers)}] {name} ({gender})", end="", flush=True)

        records = get_best_times(sid, name, gender)

        if records:
            print(f" → {len(records)} times")
            all_records.extend(records)
        else:
            print(f" → No times found")

        time.sleep(1.5)

    if all_records:
        df = export(all_records)
        print(f"\nTotal records: {len(df)}")
        print(df.head(20).to_string(index=False))
    else:
        print("\n⚠ No records collected.")


if __name__ == "__main__":
    main()
