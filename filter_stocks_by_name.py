import pandas as pd
import re
import os
import shutil

# File paths
input_csv = "Instruments.csv"
backup_csv = "Instruments_backup.csv"
output_csv = "removed_stocks.csv"

def has_four_or_more_digits(symbol):
    """Check if the symbol contains 4 or more numerical characters."""
    # Count numerical characters in the symbol
    digit_count = len(re.findall(r'\d', str(symbol)))
    return digit_count >= 4

def main():
    # Read input CSV
    try:
        df = pd.read_csv(input_csv)
        if "tradingsymbol" not in df.columns:
            print("Error: 'tradingsymbol' column not found in Instruments.csv")
            return
    except Exception as e:
        print(f"Error reading {input_csv}: {e}")
        return
    
    # Create a backup of the original CSV
    try:
        shutil.copy(input_csv, backup_csv)
        print(f"Created backup of original file at {backup_csv}")
    except Exception as e:
        print(f"Error creating backup {backup_csv}: {e}")
        return
    
    # Filter stocks
    print("Filtering stocks...")
    initial_count = len(df)
    # Stocks with fewer than 4 numerical characters (to keep)
    df_filtered = df[~df["tradingsymbol"].apply(has_four_or_more_digits)]
    # Stocks with 4 or more numerical characters (to remove)
    df_removed = df[df["tradingsymbol"].apply(has_four_or_more_digits)]
    filtered_count = len(df_filtered)
    removed_count = len(df_removed)
    
    # Overwrite original CSV with filtered stocks
    try:
        df_filtered.to_csv(input_csv, index=False)
        print(f"Removed {removed_count} stocks with 4 or more numerical characters.")
        print(f"Updated {input_csv} with {filtered_count} stocks.")
    except Exception as e:
        print(f"Error saving {input_csv}: {e}")
        return
    
    # Save removed stocks to output CSV
    if not df_removed.empty:
        try:
            df_removed.to_csv(output_csv, index=False)
            print(f"Saved {removed_count} removed stocks to {output_csv}.")
        except Exception as e:
            print(f"Error saving {output_csv}: {e}")
    else:
        print("No stocks were removed.")

if __name__ == "__main__":
    main()