# Springer Take-Home Test — Data Engineer (Pipeline)

## Overview
This project loads the referral program CSVs, profiles datasets, joins them into a unified referral view, and applies business logic to detect valid/invalid referrals. Outputs are saved to `output/`.

## Folder structure
Place your CSV files in the `data/` directory. The scripts write results to `output/`.

## Scripts
- `scripts/data_profiling.py` — dataset profiling (`output/data_profiling_report.csv`)
- `scripts/main_referral_pipeline.py` — main pipeline producing `output/final_referral_report.csv`

## Requirements
Install Python and dependencies:
```bash
python -m pip install -r requirements.txt
