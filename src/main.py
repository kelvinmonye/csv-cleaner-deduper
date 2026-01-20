import pandas as pd

def main():
    print("CSV Cleaner & Deduper starting...")

    df = pd.read_csv("data/sample_input.csv")

    print("\nColumns found in CSV:")
    print(df.columns.tolist())

    print("\nFirst few rows:")
    print(df.head())

if __name__ == "__main__":
    main()
