import pandas as pd
from cleaner.config import COLUMN_ALIASES
from cleaner.normalize import normalize_columns

def main():
    print("CSV Cleaner & Deduper starting...")

    df = pd.read_csv("data/sample_input.csv")

    print("\nOriginal columns:")
    print(df.columns.tolist())

    df.columns = normalize_columns(df.columns, COLUMN_ALIASES)

    print("\nNormalized columns:")
    print(df.columns.tolist())

    print("\nFirst few rows:")
    print(df.head())

if __name__ == "__main__":
    main()
