# MAAC Performance Dashboard

A data pipeline and performance dashboard for MAAC Swim Club built with Python and Tableau.

## Overview

MAAC Swim Club is a competitive youth swim club based in Atlanta, GA. This project collects, structures, and visualizes performance data for all 133 swimmers on the 2025-26 roster across both short course (SCY) and long course (LCM) events.

The goal is to move coaching decisions from feel and memory to data — tracking individual progression, identifying each swimmer's strongest events, and surfacing team-wide trends heading into championship season.

## Project Structure

```
MAAC-Performance-Dashboard/
├── data/                  # Local data files (excluded from version control)
├── scraper/
│   ├── maac_scraper.py    # Best times pipeline — 2,500+ records across 133 swimmers
│   ├── meet_history.py    # Full meet results per swimmer (coming soon)
│   └── rankings.py        # Team rankings by event (coming soon)
└── dashboard/             # Tableau dashboard (coming soon)
```

## Data Pipeline

Performance data is sourced from SwimCloud via their internal API. The scraper collects each swimmer's best time per event and outputs a structured dataset with the following fields:

| Field | Description |
|---|---|
| name | Swimmer full name |
| gender | M / F |
| swimmer_id | SwimCloud swimmer ID |
| distance | Event distance (50, 100, 200, etc.) |
| stroke | Free, Back, Breast, Fly, IM |
| course | SCY or LCM |
| best_time | Personal best time |
| meet | Meet where the best time was swum |
| date | Date of the swim |

## Setup

**Requirements**
```bash
pip install requests beautifulsoup4 pandas openpyxl
```

**Run the scraper**
```bash
python scraper/maac_scraper.py
```

Output files (`maac_best_times.csv` and `maac_best_times.xlsx`) are saved locally and excluded from version control.

## Status

| Component | Status |
|---|---|
| Best times scraper | Complete |
| Meet history scraper | In progress |
| Rankings scraper | Planned |
| Tableau dashboard | Planned |

## Author

David Gapinski — Economics & Computer Science, Georgia Tech
