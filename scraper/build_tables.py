"""
MAAC Performance Dashboard - Build Normalized Tables
Converts maac_swim_history.xlsx into 3 normalized CSV tables for Tableau.

Input:
    maac_swim_history.xlsx  (output from swim_history.py)

Output:
    swimmers.csv  — one row per swimmer
    swims.csv     — one row per swim
    splits.csv    — one row per split, linked by swim_id

Schema:
    swimmers: swimmer_id, name, gender
    swims:    swim_id, swimmer_id, distance, stroke, course, time, meet, date, place, heat, lane
    splits:   swim_id, split_number, split_time

Join keys:
    swims.swim_id      → splits.swim_id
    swims.swimmer_id   → swimmers.swimmer_id

Requirements:
    pip install pandas openpyxl

Usage:
    py -3.9 build_tables.py
"""

import pandas as pd

# ── Load ──────────────────────────────────────────────────────────────────────

try:
    df = pd.read_excel("maac_swim_history.xlsx")
except FileNotFoundError:
    raise FileNotFoundError("maac_swim_history.xlsx not found. Run swim_history.py first.")

print(f"Loaded {len(df)} swim records")

# ── Table 1: swimmers ─────────────────────────────────────────────────────────

swimmers = (
    df[["swimmer_id", "name", "gender"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
swimmers.to_csv("swimmers.csv", index=False)
print(f"✅ swimmers.csv — {len(swimmers)} rows")

# ── Table 2: swims ────────────────────────────────────────────────────────────

swims = df[["swimmer_id", "distance", "stroke", "course", "time",
            "meet", "date", "place", "heat", "lane"]].copy()
swims.insert(0, "swim_id", range(1, len(swims) + 1))
swims.to_csv("swims.csv", index=False)
print(f"✅ swims.csv — {len(swims)} rows")

# ── Table 3: splits ───────────────────────────────────────────────────────────

split_records = []

for idx, row in df.iterrows():
    swim_id    = idx + 1
    splits_raw = str(row["splits"])
    split_list = [s.strip() for s in splits_raw.split(",")
                  if s.strip() and s.strip() != "nan"]
    for split_num, split_time in enumerate(split_list, 1):
        split_records.append({
            "swim_id":      swim_id,
            "split_number": split_num,
            "split_time":   split_time,
        })

splits = pd.DataFrame(split_records)
splits.to_csv("splits.csv", index=False)
print(f"✅ splits.csv — {len(splits)} rows")

print(f"\nDone. Connect all 3 files in Tableau using:")
print(f"  swims.swim_id    → splits.swim_id")
print(f"  swims.swimmer_id → swimmers.swimmer_id")
