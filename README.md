# MAAC Performance Dashboard

A data pipeline and performance dashboard for MAAC Swim Club built with Python and Tableau.

## Overview

MAAC Swim Club is a competitive youth swim club based in Atlanta, GA. This project collects, structures, and visualizes performance data for all 133 swimmers on the 2025-26 roster across both short course (SCY) and long course (LCM) events.

The goal is to move coaching decisions from feel and memory to data — tracking individual progression, identifying each swimmer's strongest events, and surfacing team-wide trends heading into championship season.

## Project Structure

```
MAAC-Performance-Dashboard/
├── data/                    # Local data files (excluded from version control)
├── scraper/
│   ├── maac_scraper.py      # Best times pipeline — 2,500+ records across 133 swimmers
│   ├── swim_history.py      # Full swim history — 30,000+ swims with splits
│   ├── build_tables.py      # Converts swim history into normalized CSV tables
│   └── rankings.py          # Team rankings by event (coming soon)
└── dashboard/               # Tableau dashboard (coming soon)
```

## Data Pipeline

Performance data is sourced from SwimCloud via their internal API. The pipeline runs in three steps:

### Step 1 — Best Times
Collects each swimmer's personal best per event across SCY and LCM.

```bash
py -3.9 maac_scraper.py
```

Output: `maac_best_times.csv`

### Step 2 — Full Swim History
Uses the best times dataset to pull every recorded swim per event per swimmer, including split times.

Requires a SwimCloud session cookie stored in a local `.env` file:
```
SWIMCLOUD_SESSION=your_session_cookie_here
```

```bash
py -3.9 swim_history.py
```

Output: `maac_swim_history.xlsx`

### Step 3 — Build Normalized Tables
Converts the flat swim history file into three normalized CSV tables ready for Tableau.

```bash
py -3.9 build_tables.py
```

Output: `swimmers.csv`, `swims.csv`, `splits.csv`

## Data Schema

### swimmers.csv
| Field | Description |
|---|---|
| swimmer_id | SwimCloud swimmer ID |
| name | Swimmer full name |
| gender | men / women |

### swims.csv
| Field | Description |
|---|---|
| swim_id | Unique swim identifier |
| swimmer_id | Foreign key → swimmers.csv |
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

### Tableau Relationships
```
swims.swim_id      → splits.swim_id
swims.swimmer_id   → swimmers.swimmer_id
```

## Setup

**Requirements**
```bash
pip install requests beautifulsoup4 pandas openpyxl python-dotenv
```

**Environment**

Create a `.env` file in the scraper folder:
```
SWIMCLOUD_SESSION=your_session_cookie_here
```

To get your session cookie: log into SwimCloud in Chrome → F12 → Application → Cookies → copy the `sessionid` value.

## Status

| Component | Status |
|---|---|
| Best times scraper | Complete |
| Full swim history scraper | Complete |
| Normalized table builder | Complete |
| Rankings scraper | Planned |
| Tableau dashboard | In progress |

## Author

David Gapinski — Economics & Computer Science, Georgia Tech
