# MAAC Performance Dashboard

A data pipeline and performance dashboard for MAAC Swim Club built with Python and Tableau.

## Overview

MAAC Swim Club is a competitive youth swim club based in Atlanta, GA. This project collects, structures, and visualizes performance data for all swimmers on the 2025-26 roster across both short course (SCY) and long course (LCM) events.

The goal is to move coaching decisions from feel and memory to data — tracking individual progression, identifying each swimmer's strongest events, surfacing team-wide trends heading into championship season, and benchmarking against national time standards.

## Project Structure

```
MAAC-Performance-Dashboard/
├── data/                    # Local data files (excluded from version control)
├── scraper/
│   ├── maac_scraper.py      # Step 1 — Best times: 2,500+ records across 132 swimmers
│   ├── swim_history.py      # Step 2 — Full swim history: 35,000+ swims with splits
│   ├── build_tables.py      # Step 3 — Normalized CSV tables for Tableau
│   └── rankings.py          # Step 4 — Team rankings + time standard tiers
└── dashboard/               # Tableau dashboard (in progress)
```

## Data Pipeline

Performance data is sourced from SwimCloud via their internal API. Run scripts from the `scraper/` directory in order.

### Step 1 — Best Times (`maac_scraper.py`)

Collects each swimmer's personal best per event (SCY + LCM). Tries to pull a live roster from SwimCloud; falls back to the local `swimmers.csv` if the roster page is blocked.

```bash
cd scraper
python maac_scraper.py
```

Output: `data/maac_best_times.csv`, `data/maac_best_times.xlsx`

---

### Step 2 — Full Swim History (`swim_history.py`)

Uses the best times dataset to pull every recorded swim per event per swimmer, including split times. Makes ~1 API call per event per swimmer — expect 30–60 minutes for a full team run.

```bash
cd scraper
python swim_history.py
```

Output: `data/maac_swim_history.csv`, `data/maac_swim_history.xlsx`

---

### Step 3 — Build Normalized Tables (`build_tables.py`)

Converts the flat swim history file into three normalized CSV tables ready for Tableau. Optionally merges swimmer birthdates from a Commit roster export if `data/commit_roster.csv` is present.

```bash
cd scraper
python build_tables.py
```

Output: `data/swimmers.csv`, `data/swims.csv`, `data/splits.csv`

---

### Step 4 — Rankings (`rankings.py`)

Generates team rankings by event with time standard tier labels. Rankings are split by gender and course (SCY / LCM). Gap to #1 is reported in seconds.

Standards tracked (in ascending difficulty):
- GA State — age-group (10U / 11-12 / 13-14) and Senior, SCY + LCM
- TYR Futures (18-under) — SCY + LCM
- NCSA Spring — SCY + LCM
- Speedo Winter Juniors — SCY + LCM
- Speedo Junior Nationals — SCY + LCM
- TYR Pro Swim — SCY + LCM
- 2028 Olympic Trials A / B — LCM only

```bash
cd scraper
python rankings.py
```

Output: `data/maac_rankings.csv`

---

## Data Schema

### swimmers.csv
| Field | Description |
|---|---|
| swimmer_id | SwimCloud swimmer ID |
| name | Swimmer full name |
| gender | men / women |
| birthdate | Date of birth (populated if commit_roster.csv is present) |

### swims.csv
| Field | Description |
|---|---|
| swim_id | Unique swim identifier |
| swimmer_id | Foreign key → swimmers.csv |
| gender | men / women |
| distance | Event distance (50, 100, 200, etc.) |
| stroke | Free, Back, Breast, Fly, IM |
| course | SCY or LCM |
| time | Time swum |
| meet | Meet name |
| date | Date of swim |
| place | Finish place |
| heat | Heat number |
| lane | Lane number |

### splits.csv
| Field | Description |
|---|---|
| swim_id | Foreign key → swims.csv |
| split_number | Split order (1, 2, 3...) |
| split_time | Cumulative split time |

### maac_rankings.csv
| Field | Description |
|---|---|
| rank | Team rank within event |
| name | Swimmer name |
| gender / course / distance / stroke | Event identifiers |
| best_time | Personal best |
| gap_to_1st | Seconds behind the team leader |
| highest_standard | Highest time standard achieved |
| GA Senior State, TYR Futures, ... | One column per standard (✓ or blank) |

### Tableau Relationships
```
swims.swimmer_id                          → swimmers.swimmer_id
swims.swim_id                             → splits.swim_id
swims.gender + distance + stroke + course → cut_times (same fields)
```

## Setup

**Requirements**
```bash
pip install curl-cffi beautifulsoup4 pandas openpyxl python-dotenv
```

> `curl-cffi` replaces `requests` to bypass Cloudflare's bot protection on SwimCloud.
> It impersonates Chrome's TLS fingerprint so API calls go through correctly.

**Environment**

Create `scraper/.env`:
```
SWIMCLOUD_SESSION=your_session_cookie_here
SWIMCLOUD_CF_CLEARANCE=your_cf_clearance_cookie_here
```

To get both cookies:
1. Log into swimcloud.com in Chrome
2. F12 → Application → Cookies → www.swimcloud.com
3. Copy the values of `sessionid` and `cf_clearance`

**Optional — Birthdates**

Drop a Commit roster export at `data/commit_roster.csv` before running `build_tables.py` to populate the `birthdate` column in `swimmers.csv`. The file is gitignored and never pushed to the repo.

## Status

| Component | Status |
|---|---|
| Best times scraper | Complete |
| Full swim history scraper | Complete |
| Normalized table builder | Complete |
| Rankings + time standards | Complete |
| Tableau dashboard | In progress |

## Author

David Gapinski — Economics & Computer Science, Georgia Tech
