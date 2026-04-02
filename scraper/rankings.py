"""
MAAC Performance Dashboard - Rankings & Time Standards
Generates team rankings by event with time standard tiers.

Input:
    maac_best_times.csv   (from maac_scraper.py)
    maac_roster.csv       (optional: name, age, gender — for age-group standards)

Output:
    maac_rankings.csv     — one row per swimmer per event, with rank, gap, and all standard tiers

Standards tracked (in ascending difficulty):
    GA State (age-group: 10U / 11-12 / 13-14 / Senior)  — SCY + LCM
    TYR Futures (18-under)                                — SCY + LCM
    NCSA Spring                                           — SCY + LCM
    Speedo Winter Juniors                                 — SCY + LCM
    Speedo Junior Nationals                               — SCY + LCM
    TYR Pro Swim                                          — SCY + LCM
    2028 Olympic Trials A / B                             — LCM only

Rankings are split by gender and by course (SCY / LCM).
Gap to #1 is reported as +seconds (how far behind the leader).

Requirements:
    pip install pandas

Usage:
    py -3.9 rankings.py
"""

import os
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "..", "data")

# ── Time helpers ──────────────────────────────────────────────────────────────

def time_to_seconds(t):
    """Convert 'M:SS.ss' or 'SS.ss' string to float seconds. Returns None on failure."""
    if pd.isna(t) or str(t).strip() in ("", "None", "--", "x"):
        return None
    t = str(t).strip()
    try:
        if ":" in t:
            parts = t.split(":")
            return int(parts[0]) * 60 + float(parts[1])
        return float(t)
    except Exception:
        return None

def seconds_to_time(s):
    """Convert float seconds back to 'M:SS.xx' or 'SS.xx' string."""
    if s is None:
        return ""
    if s >= 60:
        m = int(s // 60)
        sec = s - m * 60
        return f"{m}:{sec:05.2f}"
    return f"{s:.2f}"

# ── Age group helper ──────────────────────────────────────────────────────────

def age_group(age):
    """Return GA State age group bucket."""
    if age is None:
        return "senior"
    try:
        age = int(age)
    except Exception:
        return "senior"
    if age <= 10:
        return "10u"
    elif age <= 12:
        return "11-12"
    elif age <= 14:
        return "13-14"
    else:
        return "senior"

# ── Time Standards ────────────────────────────────────────────────────────────
# Format: (distance, stroke, course) → seconds
# Women and men stored separately.
# All times transcribed from official 2025-26 standards.
# "--" or None means no standard exists for that event.

def build_standards():
    """
    Returns dict:
        standards[gender][meet][event_key] = cutoff_seconds
    where event_key = (distance, stroke, course)
    and gender = 'women' or 'men'
    Faster than this time = standard achieved.

    For GA State, returns nested by age group:
        standards['women']['ga_state']['10u'][(distance, stroke, course)] = seconds
    """

    raw = {
        # ── GA STATE SCY ─────────────────────────────────────────────────────
        # Girls / Boys — age groups: 10u, 11-12, 13-14
        # Senior handled by GA Senior State below
        "ga_state_scy": {
            "women": {
                "10u": [
                    (50,   "Free",   "SCY", "34.59"),
                    (100,  "Free",   "SCY", "1:16.19"),
                    (200,  "Free",   "SCY", "2:46.79"),
                    (400,  "Free",   "SCY", "7:27.29"),
                    (800,  "Free",   "SCY", None),
                    (1500, "Free",   "SCY", None),
                    (50,   "Back",   "SCY", "40.29"),
                    (100,  "Back",   "SCY", "1:29.59"),
                    (200,  "Back",   "SCY", None),
                    (50,   "Breast", "SCY", "45.89"),
                    (100,  "Breast", "SCY", "1:43.19"),
                    (200,  "Breast", "SCY", None),
                    (50,   "Fly",    "SCY", "39.19"),
                    (100,  "Fly",    "SCY", "1:32.29"),
                    (200,  "Fly",    "SCY", None),
                    (100,  "IM",     "SCY", None),
                    (200,  "IM",     "SCY", "3:13.79"),
                    (400,  "IM",     "SCY", None),
                ],
                "11-12": [
                    (50,   "Free",   "SCY", "28.29"),
                    (100,  "Free",   "SCY", "1:02.09"),
                    (200,  "Free",   "SCY", "2:15.59"),
                    (400,  "Free",   "SCY", "6:07.09"),
                    (800,  "Free",   "SCY", "12:57.99"),
                    (1500, "Free",   "SCY", "21:45.99"),
                    (50,   "Back",   "SCY", "32.69"),
                    (100,  "Back",   "SCY", "1:11.29"),
                    (200,  "Back",   "SCY", "2:36.89"),
                    (50,   "Breast", "SCY", "36.99"),
                    (100,  "Breast", "SCY", "1:21.59"),
                    (200,  "Breast", "SCY", "2:59.39"),
                    (50,   "Fly",    "SCY", "31.59"),
                    (100,  "Fly",    "SCY", "1:12.19"),
                    (200,  "Fly",    "SCY", "2:44.59"),
                    (100,  "IM",     "SCY", None),
                    (200,  "IM",     "SCY", "2:36.89"),
                    (400,  "IM",     "SCY", "5:41.89"),
                ],
                "13-14": [
                    (50,   "Free",   "SCY", "25.79"),
                    (100,  "Free",   "SCY", "56.69"),
                    (200,  "Free",   "SCY", "2:03.39"),
                    (400,  "Free",   "SCY", "5:32.29"),
                    (800,  "Free",   "SCY", "11:34.89"),
                    (1500, "Free",   "SCY", "19:23.09"),
                    (50,   "Back",   "SCY", "30.26"),
                    (100,  "Back",   "SCY", "1:04.19"),
                    (200,  "Back",   "SCY", "2:22.89"),
                    (50,   "Breast", "SCY", "34.49"),
                    (100,  "Breast", "SCY", "1:13.69"),
                    (200,  "Breast", "SCY", "2:42.89"),
                    (50,   "Fly",    "SCY", "28.90"),
                    (100,  "Fly",    "SCY", "1:04.19"),
                    (200,  "Fly",    "SCY", "2:25.19"),
                    (100,  "IM",     "SCY", None),
                    (200,  "IM",     "SCY", "2:20.69"),
                    (400,  "IM",     "SCY", "4:58.09"),
                ],
            },
            "men": {
                "10u": [
                    (50,   "Free",   "SCY", "34.89"),
                    (100,  "Free",   "SCY", "1:16.49"),
                    (200,  "Free",   "SCY", "2:47.59"),
                    (400,  "Free",   "SCY", "7:28.29"),
                    (800,  "Free",   "SCY", None),
                    (1500, "Free",   "SCY", None),
                    (50,   "Back",   "SCY", "40.99"),
                    (100,  "Back",   "SCY", "1:30.29"),
                    (200,  "Back",   "SCY", None),
                    (50,   "Breast", "SCY", "45.69"),
                    (100,  "Breast", "SCY", "1:42.49"),
                    (200,  "Breast", "SCY", None),
                    (50,   "Fly",    "SCY", "38.29"),
                    (100,  "Fly",    "SCY", "1:30.29"),
                    (200,  "Fly",    "SCY", None),
                    (100,  "IM",     "SCY", None),
                    (200,  "IM",     "SCY", "3:11.89"),
                    (400,  "IM",     "SCY", None),
                ],
                "11-12": [
                    (50,   "Free",   "SCY", "27.79"),
                    (100,  "Free",   "SCY", "1:00.79"),
                    (200,  "Free",   "SCY", "2:13.29"),
                    (400,  "Free",   "SCY", "6:06.09"),
                    (800,  "Free",   "SCY", "12:47.19"),
                    (1500, "Free",   "SCY", "21:46.29"),
                    (50,   "Back",   "SCY", "32.29"),
                    (100,  "Back",   "SCY", "1:10.29"),
                    (200,  "Back",   "SCY", "2:32.69"),
                    (50,   "Breast", "SCY", "35.89"),
                    (100,  "Breast", "SCY", "1:20.19"),
                    (200,  "Breast", "SCY", "2:58.99"),
                    (50,   "Fly",    "SCY", "30.69"),
                    (100,  "Fly",    "SCY", "1:09.99"),
                    (200,  "Fly",    "SCY", "2:39.79"),
                    (100,  "IM",     "SCY", None),
                    (200,  "IM",     "SCY", "2:32.59"),
                    (400,  "IM",     "SCY", "5:34.69"),
                ],
                "13-14": [
                    (50,   "Free",   "SCY", "24.09"),
                    (100,  "Free",   "SCY", "52.79"),
                    (200,  "Free",   "SCY", "1:56.59"),
                    (400,  "Free",   "SCY", "5:19.69"),
                    (800,  "Free",   "SCY", "11:06.09"),
                    (1500, "Free",   "SCY", "18:47.09"),
                    (50,   "Back",   "SCY", "28.06"),
                    (100,  "Back",   "SCY", "1:00.29"),
                    (200,  "Back",   "SCY", "2:12.39"),
                    (50,   "Breast", "SCY", "31.70"),
                    (100,  "Breast", "SCY", "1:08.69"),
                    (200,  "Breast", "SCY", "2:34.59"),
                    (50,   "Fly",    "SCY", "26.92"),
                    (100,  "Fly",    "SCY", "59.39"),
                    (200,  "Fly",    "SCY", "2:14.49"),
                    (100,  "IM",     "SCY", None),
                    (200,  "IM",     "SCY", "2:11.29"),
                    (400,  "IM",     "SCY", "4:47.19"),
                ],
            },
        },

        # ── GA STATE LCM ─────────────────────────────────────────────────────
        "ga_state_lcm": {
            "women": {
                "10u": [
                    (50,   "Free",   "LCM", "39.29"),
                    (100,  "Free",   "LCM", "1:26.89"),
                    (200,  "Free",   "LCM", "3:10.49"),
                    (400,  "Free",   "LCM", "6:42.39"),
                    (50,   "Back",   "LCM", "46.69"),
                    (100,  "Back",   "LCM", "1:43.99"),
                    (50,   "Breast", "LCM", "52.89"),
                    (100,  "Breast", "LCM", "1:59.29"),
                    (50,   "Fly",    "LCM", "44.29"),
                    (100,  "Fly",    "LCM", "1:45.79"),
                    (200,  "IM",     "LCM", "3:41.89"),
                ],
                "11-12": [
                    (50,   "Free",   "LCM", "32.19"),
                    (100,  "Free",   "LCM", "1:10.79"),
                    (200,  "Free",   "LCM", "2:34.29"),
                    (400,  "Free",   "LCM", "5:30.19"),
                    (800,  "Free",   "LCM", "11:40.39"),
                    (1500, "Free",   "LCM", "22:42.19"),
                    (50,   "Back",   "LCM", "37.69"),
                    (100,  "Back",   "LCM", "1:22.79"),
                    (200,  "Back",   "LCM", "3:02.69"),
                    (50,   "Breast", "LCM", "42.19"),
                    (100,  "Breast", "LCM", "1:33.79"),
                    (200,  "Breast", "LCM", "3:26.99"),
                    (50,   "Fly",    "LCM", "35.49"),
                    (100,  "Fly",    "LCM", "1:21.89"),
                    (200,  "Fly",    "LCM", "3:07.69"),
                    (200,  "IM",     "LCM", "3:00.39"),
                    (400,  "IM",     "LCM", "6:32.09"),
                ],
                "13-14": [
                    (50,   "Free",   "LCM", "29.49"),
                    (100,  "Free",   "LCM", "1:04.69"),
                    (200,  "Free",   "LCM", "2:20.79"),
                    (400,  "Free",   "LCM", "4:59.29"),
                    (800,  "Free",   "LCM", "10:23.59"),
                    (1500, "Free",   "LCM", "19:58.09"),
                    (50,   "Back",   "LCM", "34.18"),
                    (100,  "Back",   "LCM", "1:14.89"),
                    (200,  "Back",   "LCM", "2:44.79"),
                    (50,   "Breast", "LCM", "39.29"),
                    (100,  "Breast", "LCM", "1:25.59"),
                    (200,  "Breast", "LCM", "3:07.59"),
                    (50,   "Fly",    "LCM", "32.79"),
                    (100,  "Fly",    "LCM", "1:12.69"),
                    (200,  "Fly",    "LCM", "2:45.19"),
                    (200,  "IM",     "LCM", "2:40.59"),
                    (400,  "IM",     "LCM", "5:47.99"),
                ],
            },
            "men": {
                "10u": [
                    (50,   "Free",   "LCM", "40.19"),
                    (100,  "Free",   "LCM", "1:27.59"),
                    (200,  "Free",   "LCM", "3:11.19"),
                    (400,  "Free",   "LCM", "6:47.99"),
                    (50,   "Back",   "LCM", "46.99"),
                    (100,  "Back",   "LCM", "1:44.69"),
                    (50,   "Breast", "LCM", "52.89"),
                    (100,  "Breast", "LCM", "1:59.69"),
                    (50,   "Fly",    "LCM", "43.59"),
                    (100,  "Fly",    "LCM", "1:43.59"),
                    (200,  "IM",     "LCM", "3:38.59"),
                ],
                "11-12": [
                    (50,   "Free",   "LCM", "31.89"),
                    (100,  "Free",   "LCM", "1:09.79"),
                    (200,  "Free",   "LCM", "2:32.89"),
                    (400,  "Free",   "LCM", "5:30.89"),
                    (800,  "Free",   "LCM", "11:35.59"),
                    (1500, "Free",   "LCM", "22:38.59"),
                    (50,   "Back",   "LCM", "37.09"),
                    (100,  "Back",   "LCM", "1:21.69"),
                    (200,  "Back",   "LCM", "2:59.69"),
                    (50,   "Breast", "LCM", "40.89"),
                    (100,  "Breast", "LCM", "1:32.59"),
                    (200,  "Breast", "LCM", "3:28.09"),
                    (50,   "Fly",    "LCM", "34.59"),
                    (100,  "Fly",    "LCM", "1:19.79"),
                    (200,  "Fly",    "LCM", "3:03.09"),
                    (200,  "IM",     "LCM", "2:56.59"),
                    (400,  "IM",     "LCM", "6:28.59"),
                ],
                "13-14": [
                    (50,   "Free",   "LCM", "27.59"),
                    (100,  "Free",   "LCM", "1:01.09"),
                    (200,  "Free",   "LCM", "2:14.09"),
                    (400,  "Free",   "LCM", "4:51.19"),
                    (800,  "Free",   "LCM", "10:01.89"),
                    (1500, "Free",   "LCM", "19:24.99"),
                    (50,   "Back",   "LCM", "31.74"),
                    (100,  "Back",   "LCM", "1:10.79"),
                    (200,  "Back",   "LCM", "2:34.69"),
                    (50,   "Breast", "LCM", "36.19"),
                    (100,  "Breast", "LCM", "1:19.89"),
                    (200,  "Breast", "LCM", "2:58.79"),
                    (50,   "Fly",    "LCM", "30.59"),
                    (100,  "Fly",    "LCM", "1:07.69"),
                    (200,  "Fly",    "LCM", "2:34.19"),
                    (200,  "IM",     "LCM", "2:32.39"),
                    (400,  "IM",     "LCM", "5:32.29"),
                ],
            },
        },

        # ── GA SENIOR STATE ───────────────────────────────────────────────────
        # SCY and LCM (15+)
        "ga_senior_state": {
            "women": [
                (50,   "Free",   "SCY", "25.89"),
                (100,  "Free",   "SCY", "56.09"),
                (200,  "Free",   "SCY", "2:00.09"),
                (400,  "Free",   "SCY", "5:24.99"),
                (800,  "Free",   "SCY", "11:21.99"),
                (1500, "Free",   "SCY", "18:57.99"),
                (50,   "Back",   "SCY", "28.99"),
                (100,  "Back",   "SCY", "1:02.69"),
                (200,  "Back",   "SCY", "2:13.99"),
                (50,   "Breast", "SCY", "33.09"),
                (100,  "Breast", "SCY", "1:13.89"),
                (200,  "Breast", "SCY", "2:37.99"),
                (50,   "Fly",    "SCY", "27.29"),
                (100,  "Fly",    "SCY", "1:02.09"),
                (200,  "Fly",    "SCY", "2:22.59"),
                (200,  "IM",     "SCY", "2:16.79"),
                (400,  "IM",     "SCY", "4:53.69"),
                (50,   "Free",   "LCM", "29.19"),
                (100,  "Free",   "LCM", "1:03.39"),
                (200,  "Free",   "LCM", "2:15.99"),
                (400,  "Free",   "LCM", "4:52.89"),
                (800,  "Free",   "LCM", "10:08.89"),
                (1500, "Free",   "LCM", "19:29.59"),
                (50,   "Back",   "LCM", "32.79"),
                (100,  "Back",   "LCM", "1:11.99"),
                (200,  "Back",   "LCM", "2:34.49"),
                (50,   "Breast", "LCM", "37.69"),
                (100,  "Breast", "LCM", "1:25.29"),
                (200,  "Breast", "LCM", "3:00.99"),
                (50,   "Fly",    "LCM", "30.99"),
                (100,  "Fly",    "LCM", "1:09.19"),
                (200,  "Fly",    "LCM", "2:38.59"),
                (200,  "IM",     "LCM", "2:36.09"),
                (400,  "IM",     "LCM", "5:35.09"),
            ],
            "men": [
                (50,   "Free",   "SCY", "23.19"),
                (100,  "Free",   "SCY", "50.69"),
                (200,  "Free",   "SCY", "1:49.49"),
                (400,  "Free",   "SCY", "5:03.79"),
                (800,  "Free",   "SCY", "10:29.49"),
                (1500, "Free",   "SCY", "17:31.99"),
                (50,   "Back",   "SCY", "25.89"),
                (100,  "Back",   "SCY", "57.69"),
                (200,  "Back",   "SCY", "2:04.49"),
                (50,   "Breast", "SCY", "28.49"),
                (100,  "Breast", "SCY", "1:03.99"),
                (200,  "Breast", "SCY", "2:24.19"),
                (50,   "Fly",    "SCY", "24.49"),
                (100,  "Fly",    "SCY", "55.39"),
                (200,  "Fly",    "SCY", "2:05.19"),
                (200,  "IM",     "SCY", "2:05.39"),
                (400,  "IM",     "SCY", "4:30.29"),
                (50,   "Free",   "LCM", "26.19"),
                (100,  "Free",   "LCM", "56.39"),
                (200,  "Free",   "LCM", "2:04.69"),
                (400,  "Free",   "LCM", "4:34.29"),
                (800,  "Free",   "LCM", "9:30.29"),
                (1500, "Free",   "LCM", "18:09.39"),
                (50,   "Back",   "LCM", "29.29"),
                (100,  "Back",   "LCM", "1:05.79"),
                (200,  "Back",   "LCM", "2:23.99"),
                (50,   "Breast", "LCM", "32.59"),
                (100,  "Breast", "LCM", "1:14.59"),
                (200,  "Breast", "LCM", "2:45.79"),
                (50,   "Fly",    "LCM", "27.79"),
                (100,  "Fly",    "LCM", "1:02.39"),
                (200,  "Fly",    "LCM", "2:23.09"),
                (200,  "IM",     "LCM", "2:22.19"),
                (400,  "IM",     "LCM", "5:07.29"),
            ],
        },

        # ── TYR FUTURES (18-under) ────────────────────────────────────────────
        "futures": {
            "women": [
                (50,   "Free",   "SCY", "23.89"),
                (100,  "Free",   "SCY", "51.89"),
                (200,  "Free",   "SCY", "1:52.29"),
                (500,  "Free",   "SCY", "5:02.59"),
                (1000, "Free",   "SCY", "10:20.49"),
                (1650, "Free",   "SCY", "17:14.39"),
                (50,   "Back",   "SCY", "26.29"),
                (100,  "Back",   "SCY", "57.09"),
                (200,  "Back",   "SCY", "2:04.19"),
                (50,   "Breast", "SCY", "29.79"),
                (100,  "Breast", "SCY", "1:05.49"),
                (200,  "Breast", "SCY", "2:22.69"),
                (50,   "Fly",    "SCY", "25.69"),
                (100,  "Fly",    "SCY", "56.59"),
                (200,  "Fly",    "SCY", "2:05.39"),
                (200,  "IM",     "SCY", "2:06.39"),
                (400,  "IM",     "SCY", "4:30.69"),
                (50,   "Free",   "LCM", "27.39"),
                (100,  "Free",   "LCM", "59.29"),
                (200,  "Free",   "LCM", "2:07.79"),
                (400,  "Free",   "LCM", "4:28.79"),
                (800,  "Free",   "LCM", "9:13.79"),
                (1500, "Free",   "LCM", "17:40.19"),
                (50,   "Back",   "LCM", "30.89"),
                (100,  "Back",   "LCM", "1:06.79"),
                (200,  "Back",   "LCM", "2:23.99"),
                (50,   "Breast", "LCM", "34.79"),
                (100,  "Breast", "LCM", "1:15.99"),
                (200,  "Breast", "LCM", "2:43.39"),
                (50,   "Fly",    "LCM", "29.49"),
                (100,  "Fly",    "LCM", "1:04.69"),
                (200,  "Fly",    "LCM", "2:21.89"),
                (200,  "IM",     "LCM", "2:26.19"),
                (400,  "IM",     "LCM", "5:07.29"),
            ],
            "men": [
                (50,   "Free",   "SCY", "21.29"),
                (100,  "Free",   "SCY", "46.39"),
                (200,  "Free",   "SCY", "1:41.59"),
                (500,  "Free",   "SCY", "4:37.09"),
                (1000, "Free",   "SCY", "9:34.29"),
                (1650, "Free",   "SCY", "16:05.49"),
                (50,   "Back",   "SCY", "23.69"),
                (100,  "Back",   "SCY", "51.49"),
                (200,  "Back",   "SCY", "1:52.79"),
                (50,   "Breast", "SCY", "26.29"),
                (100,  "Breast", "SCY", "57.99"),
                (200,  "Breast", "SCY", "2:07.99"),
                (50,   "Fly",    "SCY", "22.89"),
                (100,  "Fly",    "SCY", "50.59"),
                (200,  "Fly",    "SCY", "1:53.69"),
                (200,  "IM",     "SCY", "1:53.89"),
                (400,  "IM",     "SCY", "4:06.99"),
                (50,   "Free",   "LCM", "24.59"),
                (100,  "Free",   "LCM", "53.59"),
                (200,  "Free",   "LCM", "1:57.79"),
                (400,  "Free",   "LCM", "4:09.99"),
                (800,  "Free",   "LCM", "8:40.69"),
                (1500, "Free",   "LCM", "16:38.99"),
                (50,   "Back",   "LCM", "27.89"),
                (100,  "Back",   "LCM", "1:00.59"),
                (200,  "Back",   "LCM", "2:11.89"),
                (50,   "Breast", "LCM", "30.89"),
                (100,  "Breast", "LCM", "1:08.19"),
                (200,  "Breast", "LCM", "2:29.09"),
                (50,   "Fly",    "LCM", "26.29"),
                (100,  "Fly",    "LCM", "57.99"),
                (200,  "Fly",    "LCM", "2:10.19"),
                (200,  "IM",     "LCM", "2:12.79"),
                (400,  "IM",     "LCM", "4:42.39"),
            ],
        },

        # ── NCSA SPRING ───────────────────────────────────────────────────────
        "ncsa": {
            "women": [
                (50,   "Free",   "SCY", "24.19"),
                (100,  "Free",   "SCY", "52.39"),
                (200,  "Free",   "SCY", "1:52.99"),
                (500,  "Free",   "SCY", "4:59.99"),
                (1000, "Free",   "SCY", "10:15.99"),
                (1650, "Free",   "SCY", "17:12.89"),
                (100,  "Back",   "SCY", "57.99"),
                (200,  "Back",   "SCY", "2:05.99"),
                (100,  "Breast", "SCY", "1:06.39"),
                (200,  "Breast", "SCY", "2:23.29"),
                (100,  "Fly",    "SCY", "57.99"),
                (200,  "Fly",    "SCY", "2:06.39"),
                (200,  "IM",     "SCY", "2:07.99"),
                (400,  "IM",     "SCY", "4:29.99"),
                (50,   "Free",   "LCM", "27.59"),
                (100,  "Free",   "LCM", "59.89"),
                (200,  "Free",   "LCM", "2:08.09"),
                (400,  "Free",   "LCM", "4:27.89"),
                (800,  "Free",   "LCM", "9:08.99"),
                (1500, "Free",   "LCM", "17:34.59"),
                (100,  "Back",   "LCM", "1:07.19"),
                (200,  "Back",   "LCM", "2:24.29"),
                (100,  "Breast", "LCM", "1:16.29"),
                (200,  "Breast", "LCM", "2:41.89"),
                (100,  "Fly",    "LCM", "1:05.29"),
                (200,  "Fly",    "LCM", "2:23.19"),
                (200,  "IM",     "LCM", "2:26.59"),
                (400,  "IM",     "LCM", "5:05.99"),
            ],
            "men": [
                (50,   "Free",   "SCY", "21.69"),
                (100,  "Free",   "SCY", "47.09"),
                (200,  "Free",   "SCY", "1:43.09"),
                (500,  "Free",   "SCY", "4:39.59"),
                (1000, "Free",   "SCY", "9:36.89"),
                (1650, "Free",   "SCY", "16:08.59"),
                (100,  "Back",   "SCY", "52.89"),
                (200,  "Back",   "SCY", "1:54.79"),
                (100,  "Breast", "SCY", "59.49"),
                (200,  "Breast", "SCY", "2:09.79"),
                (100,  "Fly",    "SCY", "51.69"),
                (200,  "Fly",    "SCY", "1:54.89"),
                (200,  "IM",     "SCY", "1:56.29"),
                (400,  "IM",     "SCY", "4:08.09"),
                (50,   "Free",   "LCM", "24.79"),
                (100,  "Free",   "LCM", "53.99"),
                (200,  "Free",   "LCM", "1:58.09"),
                (400,  "Free",   "LCM", "4:09.79"),
                (800,  "Free",   "LCM", "8:40.79"),
                (1500, "Free",   "LCM", "16:41.69"),
                (100,  "Back",   "LCM", "1:01.39"),
                (200,  "Back",   "LCM", "2:12.39"),
                (100,  "Breast", "LCM", "1:08.29"),
                (200,  "Breast", "LCM", "2:29.79"),
                (100,  "Fly",    "LCM", "58.89"),
                (200,  "Fly",    "LCM", "2:10.59"),
                (200,  "IM",     "LCM", "2:13.59"),
                (400,  "IM",     "LCM", "4:43.89"),
            ],
        },

        # ── SPEEDO WINTER JUNIORS ─────────────────────────────────────────────
        "winter_juniors": {
            "women": [
                (50,   "Free",   "SCY", "23.29"),
                (100,  "Free",   "SCY", "50.39"),
                (200,  "Free",   "SCY", "1:49.09"),
                (500,  "Free",   "SCY", "4:53.59"),
                (1000, "Free",   "SCY", "10:11.49"),
                (1650, "Free",   "SCY", "17:02.19"),
                (50,   "Back",   "SCY", "25.09"),
                (100,  "Back",   "SCY", "55.09"),
                (200,  "Back",   "SCY", "1:59.39"),
                (50,   "Breast", "SCY", None),
                (100,  "Breast", "SCY", "1:03.09"),
                (200,  "Breast", "SCY", "2:17.19"),
                (50,   "Fly",    "SCY", None),
                (100,  "Fly",    "SCY", "54.69"),
                (200,  "Fly",    "SCY", "2:01.69"),
                (200,  "IM",     "SCY", "2:02.19"),
                (400,  "IM",     "SCY", "4:21.69"),
                (50,   "Free",   "LCM", "26.89"),
                (100,  "Free",   "LCM", "58.19"),
                (200,  "Free",   "LCM", "2:04.99"),
                (400,  "Free",   "LCM", "4:26.69"),
                (800,  "Free",   "LCM", "9:06.79"),
                (1500, "Free",   "LCM", "17:26.79"),
                (100,  "Back",   "LCM", "1:04.79"),
                (200,  "Back",   "LCM", "2:19.59"),
                (100,  "Breast", "LCM", "1:13.79"),
                (200,  "Breast", "LCM", "2:38.59"),
                (100,  "Fly",    "LCM", "1:02.69"),
                (200,  "Fly",    "LCM", "2:18.39"),
                (200,  "IM",     "LCM", "2:22.09"),
                (400,  "IM",     "LCM", "5:00.29"),
            ],
            "men": [
                (50,   "Free",   "SCY", "20.49"),
                (100,  "Free",   "SCY", "44.89"),
                (200,  "Free",   "SCY", "1:38.39"),
                (500,  "Free",   "SCY", "4:28.79"),
                (1000, "Free",   "SCY", "9:22.69"),
                (1650, "Free",   "SCY", "15:40.39"),
                (100,  "Back",   "SCY", "49.19"),
                (200,  "Back",   "SCY", "1:47.79"),
                (100,  "Breast", "SCY", "55.49"),
                (200,  "Breast", "SCY", "2:01.59"),
                (100,  "Fly",    "SCY", "48.69"),
                (200,  "Fly",    "SCY", "1:49.09"),
                (200,  "IM",     "SCY", "1:49.59"),
                (400,  "IM",     "SCY", "3:56.49"),
                (50,   "Free",   "LCM", "24.09"),
                (100,  "Free",   "LCM", "52.59"),
                (200,  "Free",   "LCM", "1:55.29"),
                (400,  "Free",   "LCM", "4:04.99"),
                (800,  "Free",   "LCM", "8:28.19"),
                (1500, "Free",   "LCM", "16:14.79"),
                (100,  "Back",   "LCM", "58.79"),
                (200,  "Back",   "LCM", "2:08.29"),
                (100,  "Breast", "LCM", "1:05.89"),
                (200,  "Breast", "LCM", "2:24.09"),
                (100,  "Fly",    "LCM", "56.49"),
                (200,  "Fly",    "LCM", "2:06.39"),
                (200,  "IM",     "LCM", "2:09.29"),
                (400,  "IM",     "LCM", "4:35.89"),
            ],
        },

        # ── SPEEDO JUNIOR NATIONALS ───────────────────────────────────────────
        "junior_nationals": {
            "women": [
                (50,   "Free",   "SCY", "22.99"),
                (100,  "Free",   "SCY", "49.99"),
                (200,  "Free",   "SCY", "1:48.19"),
                (500,  "Free",   "SCY", "4:49.99"),
                (1000, "Free",   "SCY", "10:04.69"),
                (1650, "Free",   "SCY", "16:50.99"),
                (50,   "Back",   "SCY", "25.19"),
                (100,  "Back",   "SCY", "54.39"),
                (200,  "Back",   "SCY", "1:58.19"),
                (50,   "Breast", "SCY", "28.79"),
                (100,  "Breast", "SCY", "1:02.39"),
                (200,  "Breast", "SCY", "2:15.39"),
                (50,   "Fly",    "SCY", "24.69"),
                (100,  "Fly",    "SCY", "54.09"),
                (200,  "Fly",    "SCY", "2:00.49"),
                (200,  "IM",     "SCY", "2:01.09"),
                (400,  "IM",     "SCY", "4:18.79"),
                (50,   "Free",   "LCM", "26.59"),
                (100,  "Free",   "LCM", "57.69"),
                (200,  "Free",   "LCM", "2:04.99"),
                (400,  "Free",   "LCM", "4:23.59"),
                (800,  "Free",   "LCM", "9:06.79"),
                (1500, "Free",   "LCM", "17:26.79"),
                (50,   "Back",   "LCM", "29.79"),
                (100,  "Back",   "LCM", "1:04.29"),
                (200,  "Back",   "LCM", "2:19.29"),
                (50,   "Breast", "LCM", "33.69"),
                (100,  "Breast", "LCM", "1:13.29"),
                (200,  "Breast", "LCM", "2:38.59"),
                (50,   "Fly",    "LCM", "28.39"),
                (100,  "Fly",    "LCM", "1:02.49"),
                (200,  "Fly",    "LCM", "2:18.39"),
                (200,  "IM",     "LCM", "2:21.29"),
                (400,  "IM",     "LCM", "5:00.29"),
            ],
            "men": [
                (50,   "Free",   "SCY", "20.39"),
                (100,  "Free",   "SCY", "44.39"),
                (200,  "Free",   "SCY", "1:37.59"),
                (500,  "Free",   "SCY", "4:25.59"),
                (1000, "Free",   "SCY", "9:13.19"),
                (1650, "Free",   "SCY", "15:31.39"),
                (50,   "Back",   "SCY", "22.49"),
                (100,  "Back",   "SCY", "48.59"),
                (200,  "Back",   "SCY", "1:46.49"),
                (50,   "Breast", "SCY", "25.29"),
                (100,  "Breast", "SCY", "54.99"),
                (200,  "Breast", "SCY", "2:00.39"),
                (50,   "Fly",    "SCY", "21.99"),
                (100,  "Fly",    "SCY", "48.19"),
                (200,  "Fly",    "SCY", "1:47.89"),
                (200,  "IM",     "SCY", "1:48.49"),
                (400,  "IM",     "SCY", "3:52.69"),
                (50,   "Free",   "LCM", "23.79"),
                (100,  "Free",   "LCM", "51.99"),
                (200,  "Free",   "LCM", "1:54.09"),
                (400,  "Free",   "LCM", "4:02.19"),
                (800,  "Free",   "LCM", "8:23.09"),
                (1500, "Free",   "LCM", "16:05.09"),
                (50,   "Back",   "LCM", "26.69"),
                (100,  "Back",   "LCM", "58.19"),
                (200,  "Back",   "LCM", "2:06.99"),
                (50,   "Breast", "LCM", "29.59"),
                (100,  "Breast", "LCM", "1:05.09"),
                (200,  "Breast", "LCM", "2:22.39"),
                (50,   "Fly",    "LCM", "25.39"),
                (100,  "Fly",    "LCM", "55.89"),
                (200,  "Fly",    "LCM", "2:05.09"),
                (200,  "IM",     "LCM", "2:07.99"),
                (400,  "IM",     "LCM", "4:33.09"),
            ],
        },

        # ── TYR PRO SWIM ──────────────────────────────────────────────────────
        "pro_swim": {
            "women": [
                (50,   "Free",   "SCY", "22.79"),
                (100,  "Free",   "SCY", "49.49"),
                (200,  "Free",   "SCY", "1:47.09"),
                (500,  "Free",   "SCY", "4:47.39"),
                (1000, "Free",   "SCY", "9:59.19"),
                (1650, "Free",   "SCY", "16:34.19"),
                (50,   "Back",   "SCY", "24.69"),
                (100,  "Back",   "SCY", "53.59"),
                (200,  "Back",   "SCY", "1:56.59"),
                (50,   "Breast", "SCY", "28.29"),
                (100,  "Breast", "SCY", "1:01.49"),
                (200,  "Breast", "SCY", "2:13.39"),
                (50,   "Fly",    "SCY", "24.39"),
                (100,  "Fly",    "SCY", "53.39"),
                (200,  "Fly",    "SCY", "1:58.69"),
                (200,  "IM",     "SCY", "1:59.59"),
                (400,  "IM",     "SCY", "4:16.29"),
                (50,   "Free",   "LCM", "26.49"),
                (100,  "Free",   "LCM", "57.39"),
                (200,  "Free",   "LCM", "2:04.39"),
                (400,  "Free",   "LCM", "4:22.89"),
                (800,  "Free",   "LCM", "9:02.19"),
                (1500, "Free",   "LCM", "17:18.29"),
                (50,   "Back",   "LCM", "29.49"),
                (100,  "Back",   "LCM", "1:03.99"),
                (200,  "Back",   "LCM", "2:18.39"),
                (50,   "Breast", "LCM", "33.59"),
                (100,  "Breast", "LCM", "1:12.59"),
                (200,  "Breast", "LCM", "2:37.09"),
                (50,   "Fly",    "LCM", "28.49"),
                (100,  "Fly",    "LCM", "1:02.09"),
                (200,  "Fly",    "LCM", "2:17.89"),
                (200,  "IM",     "LCM", "2:20.49"),
                (400,  "IM",     "LCM", "4:59.09"),
            ],
            "men": [
                (50,   "Free",   "SCY", "19.89"),
                (100,  "Free",   "SCY", "43.59"),
                (200,  "Free",   "SCY", "1:35.99"),
                (500,  "Free",   "SCY", "4:22.49"),
                (1000, "Free",   "SCY", "9:10.29"),
                (1650, "Free",   "SCY", "15:17.09"),
                (50,   "Back",   "SCY", "21.79"),
                (100,  "Back",   "SCY", "47.49"),
                (200,  "Back",   "SCY", "1:44.39"),
                (50,   "Breast", "SCY", "24.69"),
                (100,  "Breast", "SCY", "53.89"),
                (200,  "Breast", "SCY", "1:57.59"),
                (50,   "Fly",    "SCY", "21.49"),
                (100,  "Fly",    "SCY", "47.19"),
                (200,  "Fly",    "SCY", "1:45.89"),
                (200,  "IM",     "SCY", "1:46.49"),
                (400,  "IM",     "SCY", "3:49.99"),
                (50,   "Free",   "LCM", "23.59"),
                (100,  "Free",   "LCM", "51.49"),
                (200,  "Free",   "LCM", "1:53.19"),
                (400,  "Free",   "LCM", "4:01.49"),
                (800,  "Free",   "LCM", "8:20.39"),
                (1500, "Free",   "LCM", "16:00.99"),
                (50,   "Back",   "LCM", "26.39"),
                (100,  "Back",   "LCM", "57.49"),
                (200,  "Back",   "LCM", "2:05.89"),
                (50,   "Breast", "LCM", "29.49"),
                (100,  "Breast", "LCM", "1:04.39"),
                (200,  "Breast", "LCM", "2:20.79"),
                (50,   "Fly",    "LCM", "25.19"),
                (100,  "Fly",    "LCM", "55.29"),
                (200,  "Fly",    "LCM", "2:04.29"),
                (200,  "IM",     "LCM", "2:06.79"),
                (400,  "IM",     "LCM", "4:31.59"),
            ],
        },

        # ── 2028 OLYMPIC TRIALS (LCM only) ────────────────────────────────────
        "olympic_trials": {
            "women": {
                "A": [
                    (50,   "Free",   "LCM", "24.56"),
                    (100,  "Free",   "LCM", "53.60"),
                    (200,  "Free",   "LCM", "1:56.43"),
                    (400,  "Free",   "LCM", "4:06.27"),
                    (800,  "Free",   "LCM", "8:26.71"),
                    (1500, "Free",   "LCM", "16:08.65"),
                    (100,  "Back",   "LCM", "59.49"),
                    (200,  "Back",   "LCM", "2:08.95"),
                    (100,  "Breast", "LCM", "1:06.10"),
                    (200,  "Breast", "LCM", "2:23.49"),
                    (100,  "Fly",    "LCM", "57.38"),
                    (200,  "Fly",    "LCM", "2:08.15"),
                    (200,  "IM",     "LCM", "2:09.90"),
                    (400,  "IM",     "LCM", "4:37.33"),
                ],
                "B": [
                    (50,   "Free",   "LCM", "24.81"),
                    (100,  "Free",   "LCM", "54.14"),
                    (200,  "Free",   "LCM", "1:57.59"),
                    (400,  "Free",   "LCM", "4:08.73"),
                    (800,  "Free",   "LCM", "8:31.78"),
                    (1500, "Free",   "LCM", "16:18.34"),
                    (100,  "Back",   "LCM", "1:00.08"),
                    (200,  "Back",   "LCM", "2:10.24"),
                    (100,  "Breast", "LCM", "1:06.76"),
                    (200,  "Breast", "LCM", "2:24.92"),
                    (100,  "Fly",    "LCM", "57.95"),
                    (200,  "Fly",    "LCM", "2:09.43"),
                    (200,  "IM",     "LCM", "2:11.20"),
                    (400,  "IM",     "LCM", "4:40.10"),
                ],
            },
            "men": {
                "A": [
                    (50,   "Free",   "LCM", "21.69"),
                    (100,  "Free",   "LCM", "47.86"),
                    (200,  "Free",   "LCM", "1:45.83"),
                    (400,  "Free",   "LCM", "3:45.46"),
                    (800,  "Free",   "LCM", "7:47.04"),
                    (1500, "Free",   "LCM", "14:51.62"),
                    (100,  "Back",   "LCM", "53.00"),
                    (200,  "Back",   "LCM", "1:56.05"),
                    (100,  "Breast", "LCM", "59.27"),
                    (200,  "Breast", "LCM", "2:09.35"),
                    (100,  "Fly",    "LCM", "51.06"),
                    (200,  "Fly",    "LCM", "1:54.69"),
                    (200,  "IM",     "LCM", "1:57.54"),
                    (400,  "IM",     "LCM", "4:11.52"),
                ],
                "B": [
                    (50,   "Free",   "LCM", "21.91"),
                    (100,  "Free",   "LCM", "48.34"),
                    (200,  "Free",   "LCM", "1:46.89"),
                    (400,  "Free",   "LCM", "3:47.71"),
                    (800,  "Free",   "LCM", "7:51.71"),
                    (1500, "Free",   "LCM", "15:00.54"),
                    (100,  "Back",   "LCM", "53.53"),
                    (200,  "Back",   "LCM", "1:57.21"),
                    (100,  "Breast", "LCM", "59.86"),
                    (200,  "Breast", "LCM", "2:10.64"),
                    (100,  "Fly",    "LCM", "51.57"),
                    (200,  "Fly",    "LCM", "1:55.84"),
                    (200,  "IM",     "LCM", "1:58.72"),
                    (400,  "IM",     "LCM", "4:14.04"),
                ],
            },
        },
    }

    return raw


# ── Standard lookup ───────────────────────────────────────────────────────────

def get_standard_tier(swimmer_time_s, gender, distance, stroke, course, age, standards_raw):
    """
    Returns a dict of {meet_name: achieved (bool or label)} for a given swim.
    Checks all meets in order of difficulty.
    """
    results = {}
    g = gender.lower()  # 'women' or 'men'

    def check(cut_str):
        if cut_str is None:
            return False
        cut = time_to_seconds(cut_str)
        if cut is None:
            return False
        return swimmer_time_s <= cut

    def lookup_flat(entries):
        for d, s, c, t in entries:
            if d == distance and s == stroke and c == course:
                return t
        return None

    # GA State (age-group)
    ag = age_group(age)
    if ag == "senior":
        # Use GA Senior State
        entries = standards_raw["ga_senior_state"].get(g, [])
        results["GA Senior State"] = check(lookup_flat(entries))
    else:
        # Use GA State age group for SCY/LCM
        key = "ga_state_scy" if course == "SCY" else "ga_state_lcm"
        ag_entries = standards_raw[key].get(g, {}).get(ag, [])
        results[f"GA State ({ag})"] = check(lookup_flat(ag_entries))

    # Flat meets (no age group)
    flat_meets = [
        ("futures",          "TYR Futures"),
        ("ncsa",             "NCSA Spring"),
        ("winter_juniors",   "Winter Juniors"),
        ("junior_nationals", "Junior Nationals"),
        ("pro_swim",         "Pro Swim"),
    ]
    for key, label in flat_meets:
        entries = standards_raw[key].get(g, [])
        results[label] = check(lookup_flat(entries))

    # Olympic Trials (LCM only)
    if course == "LCM":
        for tier in ["B", "A"]:
            entries = standards_raw["olympic_trials"].get(g, {}).get(tier, [])
            results[f"Olympic Trials {tier}"] = check(lookup_flat(entries))
    else:
        results["Olympic Trials B"] = False
        results["Olympic Trials A"] = False

    return results


def highest_standard(tier_dict):
    """Return the name of the highest standard achieved, or '' if none."""
    order = [
        "GA State (10u)", "GA State (11-12)", "GA State (13-14)",
        "GA Senior State",
        "TYR Futures",
        "NCSA Spring",
        "Winter Juniors",
        "Junior Nationals",
        "Pro Swim",
        "Olympic Trials B",
        "Olympic Trials A",
    ]
    best = ""
    for std in order:
        if tier_dict.get(std):
            best = std
    return best


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load best times
    best_times_path = os.path.join(DATA_DIR, "maac_best_times.csv")
    try:
        df = pd.read_csv(best_times_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"{best_times_path} not found. Run maac_scraper.py first.")

    print(f"Loaded {len(df)} best time records for {df['name'].nunique()} swimmers")

    # Load roster (optional — for age-group standard assignment)
    roster_ages = {}
    try:
        roster = pd.read_csv(os.path.join(DATA_DIR, "maac_roster.csv"))   # columns: name, age, gender
        for _, row in roster.iterrows():
            roster_ages[str(row["name"]).strip()] = row.get("age", None)
        print(f"Loaded roster ages for {len(roster_ages)} swimmers")
    except FileNotFoundError:
        print("⚠ maac_roster.csv not found — age-group standards will use 'senior' for all swimmers")
        print("  Create maac_roster.csv with columns: name, age, gender")

    standards_raw = build_standards()

    # Normalize distance column (handle 500/1000/1650 SCY vs 400/800/1500 LCM)
    df["distance"] = pd.to_numeric(df["distance"], errors="coerce").astype("Int64")

    # Convert times to seconds for ranking
    df["time_seconds"] = df["best_time"].apply(time_to_seconds)
    df = df.dropna(subset=["time_seconds"])

    # Assign age from roster
    df["age"] = df["name"].apply(lambda n: roster_ages.get(str(n).strip(), None))

    records = []

    for (gender, course, distance, stroke), group in df.groupby(["gender", "course", "distance", "stroke"]):
        group = group.sort_values("time_seconds").reset_index(drop=True)
        best_time_s = group["time_seconds"].iloc[0]

        for rank, row in group.iterrows():
            t_s = row["time_seconds"]
            gap = t_s - best_time_s
            age = row["age"]

            tier_dict = get_standard_tier(t_s, gender, distance, stroke, course, age, standards_raw)
            top_std = highest_standard(tier_dict)

            rec = {
                "rank":             rank + 1,
                "name":             row["name"],
                "gender":           gender,
                "age":              age,
                "distance":         distance,
                "stroke":           stroke,
                "course":           course,
                "best_time":        row["best_time"],
                "time_seconds":     round(t_s, 2),
                "gap_to_1st":       f"+{gap:.2f}s" if gap > 0 else "—",
                "gap_seconds":      round(gap, 2),
                "highest_standard": top_std,
                "meet":             row.get("meet", ""),
                "date":             row.get("date", ""),
            }

            # Individual standard columns
            for std_name, achieved in tier_dict.items():
                rec[std_name] = "✓" if achieved else ""

            records.append(rec)

    rankings = pd.DataFrame(records)

    # Sort: gender → course → distance → stroke → rank
    rankings = rankings.sort_values(
        ["gender", "course", "distance", "stroke", "rank"]
    ).reset_index(drop=True)

    out_path = os.path.join(DATA_DIR, "maac_rankings.csv")
    rankings.to_csv(out_path, index=False)

    # Summary
    total = len(rankings)
    standards_cols = [c for c in rankings.columns if c not in [
        "rank", "name", "gender", "age", "distance", "stroke", "course",
        "best_time", "time_seconds", "gap_to_1st", "gap_seconds",
        "highest_standard", "meet", "date"
    ]]

    print(f"\n✅ maac_rankings.csv — {total} entries across {rankings.groupby(['gender','course','distance','stroke']).ngroups} events")
    print(f"\nStandard columns: {standards_cols}")

    # Print a quick preview per gender/course
    for (gender, course), grp in rankings.groupby(["gender", "course"]):
        print(f"\n── {gender.upper()} {course} ──")
        # Show top 3 per event
        sample = grp.groupby(["distance", "stroke"]).head(3)[
            ["rank", "name", "distance", "stroke", "best_time", "gap_to_1st", "highest_standard"]
        ]
        print(sample.to_string(index=False))


if __name__ == "__main__":
    main()
