from typing import List
import pandas as pd


def _is_empty(value) -> bool:
    """True if value is None/NaN/empty string after stripping."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        # Some objects don't play nicely with pd.isna
        pass
    return str(value).strip() == ""


def _count_non_empty(row: pd.Series) -> int:
    """How complete is this row? More non-empty values = better."""
    score = 0
    for v in row.values:
        if not _is_empty(v):
            score += 1
    return score


def build_dedupe_key(row: pd.Series) -> str:
    """
    Build a deterministic key for identifying duplicates.
    Priority:
      1) name + phone
      2) name + email
      3) name + website
      4) name + city + state (fallback)
      5) no_name rows should NOT merge together
    """
    name = str(row.get("name", "")).strip().lower()
    phone = str(row.get("phone", "")).strip()
    email = str(row.get("email", "")).strip().lower()
    website = str(row.get("website", "")).strip().lower()
    city = str(row.get("city", "")).strip().lower()
    state = str(row.get("state", "")).strip().lower()

    # Safety: if there's no name, do NOT dedupe (avoid accidental merging)
    if not name:
        # include index-like uniqueness using some content to avoid mass merging
        return f"no_name|{phone}|{email}|{website}|{city}|{state}"

    if name and phone:
        return f"{name}|phone:{phone}"
    if name and email:
        return f"{name}|email:{email}"
    if name and website:
        return f"{name}|web:{website}"

    if name and city and state:
        return f"{name}|loc:{city},{state}"

    return f"{name}|fallback:{city}:{state}"


def dedupe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group rows by dedupe_key and keep the best record in each group.
    "Best" = most complete row (highest non-empty count).
    Then merge: fill missing values in best row from other rows in the group.
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

                if _is_empty(current) and not _is_empty(incoming):
                    best_row[col] = incoming

        kept_rows.append(best_row)

    out = pd.DataFrame(kept_rows).drop(columns=["dedupe_key"], errors="ignore")
    out = out.reset_index(drop=True)
    return out
