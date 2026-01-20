from typing import List, Dict, Any
import pandas as pd


def _count_non_empty(row: pd.Series) -> int:
    """How complete is this row? More non-empty values = better."""
    score = 0
    for v in row.values:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if str(v).strip() != "":
            score += 1
    return score


def build_dedupe_key(row: pd.Series) -> str:
    """
    Build a deterministic key for identifying duplicates.
    Priority:
      1) name + phone
      2) name + email
      3) name + website
      4) name only (last resort; can be risky)
    """
    name = str(row.get("name", "")).strip().lower()
    phone = str(row.get("phone", "")).strip()
    email = str(row.get("email", "")).strip().lower()
    website = str(row.get("website", "")).strip().lower()

    if name and phone:
        return f"{name}|phone:{phone}"
    if name and email:
        return f"{name}|email:{email}"
    if name and website:
        return f"{name}|web:{website}"
    return f"{name}|fallback"


def dedupe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group rows by dedupe_key and keep the best record in each group.
    "Best" = most complete row (highest non-empty count).
    """
    if df.empty:
        return df.copy()

    df = df.copy()
    df["dedupe_key"] = df.apply(build_dedupe_key, axis=1)

    kept_rows: List[pd.Series] = []

    for _, group in df.groupby("dedupe_key", dropna=False):
        # Pick the best row by completeness score
        scores = group.apply(_count_non_empty, axis=1)
        best_idx = scores.idxmax()
        best_row = group.loc[best_idx].copy()

        # Merge: fill missing values in best_row using other rows in the group
        for _, other in group.iterrows():
            for col in df.columns:
                if col == "dedupe_key":
                    continue
                current = best_row.get(col, "")
                incoming = other.get(col, "")

                if (str(current).strip() == "" or pd.isna(current)) and str(incoming).strip() != "" and not pd.isna(incoming):
                    best_row[col] = incoming

        kept_rows.append(best_row)

    out = pd.DataFrame(kept_rows).drop(columns=["dedupe_key"], errors="ignore")
    out = out.reset_index(drop=True)
    return out
