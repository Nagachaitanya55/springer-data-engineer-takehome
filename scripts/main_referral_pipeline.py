"""
Main referral pipeline.
Reads CSVs from ../data, writes final report to ../output/final_referral_report.csv
"""

import os
from datetime import datetime
import pandas as pd
from dateutil import parser
import pytz

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Expected CSV file mappings
F_FILES = {
    "lead_logs": "lead_log.csv",
    "user_referrals": "user_referrals.csv",
    "user_referral_logs": "user_referral_logs.csv",
    "user_logs": "user_logs.csv",
    "user_referral_statuses": "user_referral_statuses.csv",
    "referral_rewards": "referral_rewards.csv",
    "paid_transactions": "paid_transactions.csv",
}


# ---------------------------- Helper Functions ---------------------------- #

def read_csv(name):
    path = os.path.join(DATA_DIR, F_FILES[name])
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing {path}")
    print(f"✅ Loaded: {F_FILES[name]}")
    return pd.read_csv(path, dtype=str)


def parse_datetime_utc(ts_str):
    if pd.isna(ts_str):
        return None
    try:
        dt = parser.isoparse(str(ts_str))
    except Exception:
        try:
            dt = parser.parse(str(ts_str))
        except Exception:
            return None
    # If naive datetime, treat as UTC
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    else:
        dt = dt.astimezone(pytz.UTC)
    return dt


def convert_utc_to_local(dt_utc, tz_string):
    if dt_utc is None:
        return None
    if not tz_string or pd.isna(tz_string):
        return dt_utc
    try:
        if tz_string.startswith("+") or tz_string.startswith("-"):
            # e.g. "+07:00" → offset minutes
            hours, minutes = tz_string.split(":")
            offset = int(hours) * 60 + int(minutes)
            return dt_utc.astimezone(pytz.FixedOffset(offset))
        tz = pytz.timezone(tz_string)
        return dt_utc.astimezone(tz)
    except Exception:
        return dt_utc


def to_local_str(utc_dt, tz_str):
    """Safely convert UTC datetime to local ISO string if possible."""
    try:
        if utc_dt is None or isinstance(utc_dt, float) or pd.isna(utc_dt):
            return None
        local_dt = convert_utc_to_local(utc_dt, tz_str)
        if local_dt is None or isinstance(local_dt, float):
            return None
        return local_dt.isoformat()
    except Exception:
        return None


def initcap_safe(s):
    if pd.isna(s) or s is None:
        return s
    return str(s).title()


# ---------------------------- Main Pipeline ---------------------------- #

def main():
    print("\n🚀 Starting referral data pipeline...\n")

    # 1. Load all datasets
    lead_logs = read_csv("lead_logs")
    user_referrals = read_csv("user_referrals")
    user_referral_logs = read_csv("user_referral_logs")
    user_logs = read_csv("user_logs")
    user_referral_statuses = read_csv("user_referral_statuses")
    referral_rewards = read_csv("referral_rewards")
    paid_transactions = read_csv("paid_transactions")

    # 2. Basic trimming whitespace
    for df in [
        lead_logs,
        user_referrals,
        user_referral_logs,
        user_logs,
        user_referral_statuses,
        referral_rewards,
        paid_transactions,
    ]:
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # 3. Parse datetime fields
    user_referrals["referral_at_parsed"] = user_referrals["referral_at"].apply(parse_datetime_utc)
    user_referrals["updated_at_parsed"] = user_referrals.get("updated_at", pd.Series([None]*len(user_referrals))).apply(parse_datetime_utc)

    user_referral_logs["created_at_parsed"] = user_referral_logs.get("created_at", pd.Series([None]*len(user_referral_logs))).apply(parse_datetime_utc)
    user_referral_logs["is_reward_granted"] = user_referral_logs.get("is_reward_granted", "False").fillna("False").map(lambda x: str(x).strip().lower() in ("true", "1", "yes"))

    referral_rewards["created_at_parsed"] = referral_rewards.get("created_at", pd.Series([None]*len(referral_rewards))).apply(parse_datetime_utc)
    referral_rewards["reward_granted_at_parsed"] = referral_rewards.get("reward_granted_at", pd.Series([None]*len(referral_rewards))).apply(parse_datetime_utc)
    referral_rewards["reward_value_num"] = pd.to_numeric(referral_rewards.get("reward_value", pd.Series([None]*len(referral_rewards))), errors="coerce").fillna(0)

    paid_transactions["transaction_at_parsed"] = paid_transactions.get("transaction_at", pd.Series([None]*len(paid_transactions))).apply(parse_datetime_utc)
    paid_transactions["transaction_status_norm"] = paid_transactions.get("transaction_status", "").str.strip().str.lower()
    paid_transactions["transaction_type_norm"] = paid_transactions.get("transaction_type", "").str.strip().str.lower()

    user_logs["membership_expired_date_parsed"] = user_logs.get("membership_expired_date", pd.Series([None]*len(user_logs))).apply(lambda s: parse_datetime_utc(s) if pd.notna(s) else None)
    user_logs["is_deleted_flag"] = user_logs.get("is_deleted", "False").fillna("False").map(lambda x: str(x).strip().lower() in ("true", "1", "yes"))

    # Map status IDs → descriptions
    status_map = (
        user_referral_statuses.set_index("id")["description"].to_dict()
        if "id" in user_referral_statuses.columns and "description" in user_referral_statuses.columns
        else {}
    )

    print("🧩 Joining tables...")

    # 4. Join tables progressively
    df = user_referrals.copy()
    df = df.rename(columns={"id": "referral_details_id"})

    # Join referral_rewards
    if "referral_reward_id" in df.columns and "id" in referral_rewards.columns:
        df = df.merge(referral_rewards.add_prefix("reward_"), left_on="referral_reward_id", right_on="reward_id", how="left")
    else:
        df["reward_reward_value_num"] = 0

    # Join user_referral_logs (latest log per referral)
    if "referral_id" in df.columns and "user_referral_id" in user_referral_logs.columns:
        logs = user_referral_logs.sort_values("created_at_parsed").groupby("user_referral_id", as_index=False).last()
        df = df.merge(logs.add_prefix("log_"), left_on="referral_id", right_on="log_user_referral_id", how="left")

    # Join paid_transactions
    if "transaction_id" in df.columns and "transaction_id" in paid_transactions.columns:
        df = df.merge(paid_transactions.add_prefix("txn_"), left_on="transaction_id", right_on="txn_transaction_id", how="left")

    # Join user_logs (referrer details)
    if "referrer_id" in df.columns and "user_id" in user_logs.columns:
        df = df.merge(user_logs.add_prefix("ref_"), left_on="referrer_id", right_on="ref_user_id", how="left")

    # Join lead_logs for lead source info
    if "referee_id" in df.columns and "lead_id" in lead_logs.columns:
        df = df.merge(lead_logs.add_prefix("lead_"), left_on="referee_id", right_on="lead_lead_id", how="left")

    # 5. Derive referral_source_category
    def determine_source_category(row):
        src = str(row.get("referral_source", "")).strip()
        if src == "User Sign Up":
            return "Online"
        elif src == "Draft Transaction":
            return "Offline"
        elif src == "Lead":
            return row.get("lead_source_category")
        return None

    df["referral_source_category"] = df.apply(determine_source_category, axis=1)

    # 6. Normalize strings (InitCap except homeclub)
    for col in ["referrer_name", "referee_name", "referrer_phone", "referee_phone"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: initcap_safe(x) if pd.notna(x) else x)

    # 7. Timezone selection per row
    def pick_timezone(row):
        for key in ["ref_timezone_homeclub", "txn_timezone", "lead_timezone_location"]:
            if key in row and pd.notna(row.get(key)) and row.get(key) != "":
                return row.get(key)
        return None

    df["preferred_tz"] = df.apply(pick_timezone, axis=1)
    df["referral_at_local"] = df.apply(lambda r: to_local_str(r.get("referral_at_parsed"), r.get("preferred_tz")), axis=1)
    df["transaction_at_local"] = df.apply(lambda r: to_local_str(r.get("txn_transaction_at_parsed"), r.get("preferred_tz")), axis=1)
    df["updated_at_local"] = df.apply(lambda r: to_local_str(r.get("updated_at_parsed"), r.get("preferred_tz")), axis=1)
    df["reward_granted_at_local"] = df.apply(lambda r: to_local_str(r.get("reward_reward_granted_at_parsed"), r.get("preferred_tz")), axis=1)

    # 8. Business logic implementation
    print("🧮 Applying business logic...")

    def compute_business_valid(row):
        try:
            reward_value = float(row.get("reward_reward_value_num") or 0)
        except Exception:
            reward_value = 0

        status = status_map.get(row.get("user_referral_status_id"), row.get("referral_status"))
        status_norm = str(status).strip().lower() if status else None

        txn_status = str(row.get("txn_transaction_status_norm", "")).lower()
        txn_type = str(row.get("txn_transaction_type_norm", "")).lower()

        has_txn = pd.notna(row.get("transaction_id")) and str(row.get("transaction_id")).strip() != ""
        txn_at = row.get("txn_transaction_at_parsed")
        referral_at = row.get("referral_at_parsed")

        membership_expiry = row.get("ref_membership_expired_date_parsed")
        deleted = row.get("ref_is_deleted_flag", False)
        reward_granted = row.get("log_is_reward_granted", False) or (not pd.isna(row.get("reward_reward_granted_at_parsed")))

        # Valid conditions
        valid_1 = (
            (reward_value > 0)
            and (status_norm == "berhasil")
            and has_txn
            and (txn_status == "paid")
            and (txn_type == "new")
            and (txn_at and referral_at and txn_at > referral_at)
            and (txn_at and referral_at and txn_at.month == referral_at.month)
            and (membership_expiry is None or membership_expiry > datetime.now(pytz.UTC))
            and not deleted
            and reward_granted
        )

        valid_2 = (status_norm in ("menunggu", "tidak berhasil", "tidakberhasil")) and (reward_value == 0)

        if valid_1 or valid_2:
            return True

        # Invalid cases
        if (reward_value > 0) and (status_norm != "berhasil"):
            return False
        if (reward_value > 0) and (not has_txn):
            return False
        if (reward_value == 0) and has_txn and (txn_status == "paid") and (txn_at and referral_at and txn_at > referral_at):
            return False
        if (status_norm == "berhasil") and (reward_value == 0):
            return False
        if (has_txn and txn_at and referral_at and txn_at < referral_at):
            return False

        return False

    df["is_business_logic_valid"] = df.apply(compute_business_valid, axis=1)

    # 9. Build final output
    out_cols = {
        "referral_details_id": "referral_details_id",
        "referral_id": "referral_id",
        "referral_source": "referral_source",
        "referral_source_category": "referral_source_category",
        "referral_at": "referral_at_parsed",
        "referrer_id": "referrer_id",
        "referrer_name": "referrer_name",
        "referrer_phone_number": "referrer_phone",
        "referrer_homeclub": "ref_homeclub" if "ref_homeclub" in df.columns else "referrer_homeclub",
        "referee_id": "referee_id",
        "referee_name": "referee_name",
        "referee_phone": "referee_phone",
        "referral_status": "user_referral_status_id",
        "transaction_id": "transaction_id",
        "transaction_status": "txn_transaction_status_norm",
        "transaction_at": "txn_transaction_at_parsed",
        "transaction_location": "txn_transaction_location",
        "transaction_type": "txn_transaction_type_norm",
        "updated_at": "updated_at_parsed",
        "reward_granted_at": "reward_reward_granted_at_parsed",
        "is_business_logic_valid": "is_business_logic_valid",
    }

    out = pd.DataFrame()
    for k, v in out_cols.items():
        out[k] = df[v] if v in df.columns else None

    # Map status IDs → descriptions
    out["referral_status"] = out["referral_status"].apply(lambda x: status_map.get(x, x))

    # Convert datetimes to ISO
    for c in ["referral_at", "transaction_at", "updated_at", "reward_granted_at"]:
        if c in out.columns:
            out[c] = out[c].apply(lambda x: x.isoformat() if hasattr(x, "isoformat") else x)

    # Drop duplicates
    out = out.drop_duplicates(subset=["referral_id"])

    # 10. Save output
    final_path = os.path.join(OUTPUT_DIR, "final_referral_report.csv")
    out.to_csv(final_path, index=False)

    print(f"\n💾 Final referral report written to: {final_path}")
    print(f"✅ Rows in final report: {len(out)}\n")


if __name__ == "__main__":
    main()
