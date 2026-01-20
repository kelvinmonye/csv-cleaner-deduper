import pandas as pd

from cleaner.config import COLUMN_ALIASES
from cleaner.normalize import normalize_columns
from cleaner.fields import normalize_email, normalize_phone, normalize_website
from cleaner.dedupe import dedupe_dataframe



def main():
    print("CSV Cleaner & Deduper starting...")

    df = pd.read_csv("data/sample_input.csv")

    print("\nOriginal columns:")
    print(df.columns.tolist())

    # 1) Normalize column names FIRST
    df.columns = normalize_columns(df.columns, COLUMN_ALIASES)

    print("\nNormalized columns:")
    print(df.columns.tolist())

    # 2) Normalize field values AFTER columns are standardized
    if "email" in df.columns:
        df["email"] = df["email"].apply(normalize_email)

    if "phone" in df.columns:
        df["phone"] = df["phone"].apply(normalize_phone)

    if "website" in df.columns:
        df["website"] = df["website"].apply(normalize_website)

    print("\nRows before dedupe:", len(df))

df_deduped = dedupe_dataframe(df)

print("Rows after dedupe:", len(df_deduped))
print("\nDeduped preview:")
print(df_deduped.head())


    print("\nFirst few rows (after cleaning):")
    print(df.head())


if __name__ == "__main__":
    main()
