"""
MAAC Performance Dashboard - Build Normalized Tables
Converts maac_swim_history.xlsx into 3 normalized CSV tables for Tableau,
and merges swimmer birthdates from a Commit roster export if available.

Pipeline position:
    swim_history.py → [build_tables.py] → Tableau

Input:
    ../data/maac_swim_history.xlsx   (from swim_history.py)
    ../data/commit_roster.csv        (optional — Commit export for birthdates)

Output (all written to ../data/):
    swimmers.csv  — swimmer_id, name, gender, birthdate
    swims.csv     — swim_id, swimmer_id, gender, distance, stroke, course,
                    time, meet, date, place, heat, lane
    splits.csv    — swim_id, split_number, split_time

Tableau join keys:
    swims.swim_id                             → splits.swim_id
    swims.swimmer_id                          → swimmers.swimmer_id
    swims.gender + distance + stroke + course → cut_times.gender + distance + stroke + course

Birthdate merge:
    Drop a fresh Commit roster export at ../data/commit_roster.csv to keep
    birthdates current. The file is gitignored — never pushed to the repo.
    If the file is missing, swimmers.csv is still written without birthdates
    and a warning is shown.

Requirements:
    pip install pandas openpyxl

Usage:
    cd scraper
    python build_tables.py
"""

import os
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(SCRIPT_DIR, "..", "data")
HISTORY_FILE = os.path.join(DATA_DIR, "maac_swim_history.xlsx")
COMMIT_FILE  = os.path.join(DATA_DIR, "commit_roster.csv")

os.makedirs(DATA_DIR, exist_ok=True)


# ── Load swim history ─────────────────────────────────────────────────────────

print("=" * 55)
print("  MAAC Build Tables")
print("=" * 55)

if not os.path.exists(HISTORY_FILE):
    raise SystemExit(
        f"❌ {HISTORY_FILE} not found.\n"
        f"   Run swim_history.py first."
    )

print(f"\n📂 Loading swim history...")
df = pd.read_excel(HISTORY_FILE)
print(f"   {len(df):,} swim records across {df['swimmer_id'].nunique()} swimmers")


# ── Table 1: swimmers ─────────────────────────────────────────────────────────

print(f"\n🏊 Building swimmers.csv...")

swimmers = (
    df[["swimmer_id", "name", "gender"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Merge birthdates from Commit roster export if available
if os.path.exists(COMMIT_FILE):
    print(f"   Found commit_roster.csv — merging birthdates...")
    try:
        commit = pd.read_csv(COMMIT_FILE, skiprows=2)

        # Build name lookup using both legal and preferred names
        lookup = {}
        for _, row in commit.iterrows():
            last  = str(row.get("Last Name",  "")).strip()
            first = str(row.get("First Name", "")).strip()
            pref  = str(row.get("Preferred Name", "")).strip()
            bd    = row.get("Birthdate")

            if pd.isna(bd):
                continue

            legal = f"{first} {last}".strip()
            if legal:
                lookup[legal.lower()] = bd

            if pref and pref != first:
                pref_full = f"{pref} {last}".strip()
                lookup[pref_full.lower()] = bd

        # Apply lookup
        swimmers["birthdate"] = swimmers["name"].apply(
            lambda n: lookup.get(str(n).strip().lower())
        )

        matched = swimmers["birthdate"].notna().sum()
        missing = swimmers["birthdate"].isna().sum()
        print(f"   ✅ Birthdates matched: {matched}/{len(swimmers)}")
        if missing > 0:
            unmatched = swimmers[swimmers["birthdate"].isna()]["name"].tolist()
            print(f"   ⚠  No birthdate for {missing} swimmers "
                  f"(likely left team or not in Commit):")
            for name in unmatched:
                print(f"      — {name}")

    except Exception as e:
        print(f"   ⚠  Could not parse commit_roster.csv: {e}")
        print(f"      Continuing without birthdates.")
        swimmers["birthdate"] = None

else:
    print(f"   ⚠  commit_roster.csv not found at {COMMIT_FILE}")
    print(f"      Drop a Commit roster export there to include birthdates.")
    print(f"      Continuing without birthdates.")
    swimmers["birthdate"] = None

out = os.path.join(DATA_DIR, "swimmers.csv")
swimmers.to_csv(out, index=False)
print(f"   ✅ swimmers.csv — {len(swimmers)} rows")


# ── Table 2: swims ────────────────────────────────────────────────────────────

print(f"\n🏁 Building swims.csv...")

swims = df[[
    "swimmer_id", "gender", "distance", "stroke", "course",
    "time", "meet", "date", "place", "heat", "lane"
]].copy()

swims.insert(0, "swim_id", range(1, len(swims) + 1))

out = os.path.join(DATA_DIR, "swims.csv")
swims.to_csv(out, index=False)
print(f"   ✅ swims.csv — {len(swims):,} rows")


# ── Table 3: splits ───────────────────────────────────────────────────────────

print(f"\n⏱  Building splits.csv...")

split_records = []
for idx, row in df.iterrows():
    swim_id    = idx + 1
    splits_raw = str(row.get("splits", ""))
    split_list = [
        s.strip() for s in splits_raw.split(",")
        if s.strip() and s.strip().lower() != "nan"
    ]
    for split_num, split_time in enumerate(split_list, 1):
        split_records.append({
            "swim_id":      swim_id,
            "split_number": split_num,
            "split_time":   split_time,
        })

splits = pd.DataFrame(split_records)
out = os.path.join(DATA_DIR, "splits.csv")
splits.to_csv(out, index=False)
print(f"   ✅ splits.csv — {len(splits):,} rows")


# ── Summary ───────────────────────────────────────────────────────────────────

print(f"\n{'─' * 55}")
print(f"✅ All tables written to {os.path.abspath(DATA_DIR)}/")
print(f"\nTableau relationships:")
print(f"  swims.swimmer_id                          → swimmers.swimmer_id")
print(f"  swims.swim_id                             → splits.swim_id")
print(f"  swims.gender+distance+stroke+course       → cut_times (same fields)")
