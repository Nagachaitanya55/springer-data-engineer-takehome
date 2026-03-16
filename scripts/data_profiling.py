"""
Data profiling script.
Saves: ../output/data_profiling_report.csv
"""

import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

CSV_FILES = [
    "lead_log.csv",
    "user_referrals.csv",
    "user_referral_logs.csv",
    "user_logs.csv",
    "user_referral_statuses.csv",
    "referral_rewards.csv",
    "paid_transactions.csv",
]

def profile_table(df: pd.DataFrame, table_name: str):
    rows = []
    for col in df.columns:
        rows.append({
            "table": table_name,
            "column": col,
            "dtype": str(df[col].dtype),
            "num_rows": len(df),
            "num_nulls": int(df[col].isna().sum()),
            "num_distinct": int(df[col].nunique(dropna=True)),
            "sample_values": ";".join(map(str, df[col].dropna().astype(str).unique()[:5]))
        })
    return rows

def main():
    all_profiles = []
    for fname in CSV_FILES:
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            print(f"WARNING: {path} not found; skipping.")
            continue
        df = pd.read_csv(path, dtype=str)  # read as strings to avoid dtype surprises
        # Basic cleanup: trim whitespace from column strings
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        profiles = profile_table(df, fname.replace(".csv", ""))
        all_profiles.extend(profiles)

    prof_df = pd.DataFrame(all_profiles)
    prof_path = os.path.join(OUTPUT_DIR, "data_profiling_report.csv")
    prof_df.to_csv(prof_path, index=False)
    print(f"Saved profiling report to {prof_path}")

if __name__ == "__main__":
    main()
