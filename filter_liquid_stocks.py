import pandas as pd
import os
import shutil

# File paths
input_csv = "Instruments.csv"
illiquid_csv = "illiquid_stocks.csv"
output_csv = "liquid_stocks.csv"
backup_csv = "Instruments_backup.csv"

def normalize_symbol(symbol):
    """Remove .NS from symbol for comparison."""
    return symbol.replace(".NS", "") if isinstance(symbol, str) else symbol

def main():
    # Create a backup of the original CSV
    try:
        if os.path.exists(input_csv):
            shutil.copy(input_csv, backup_csv)
            print(f"Created backup of original file at {backup_csv}")
        else:
            print(f"Error: {input_csv} does not exist")
            return
    except Exception as e:
        print(f"Error creating backup {backup_csv}: {e}")
        return
    
    # Read input CSVs
    try:
        df_all = pd.read_csv(input_csv)
        if "tradingsymbol" not in df_all.columns:
            print(f"Error: 'tradingsymbol' column not found in {input_csv}")
            return
    except Exception as e:
        print(f"Error reading {input_csv}: {e}")
        return
    
    try:
        df_illiquid = pd.read_csv(illiquid_csv)
        if "tradingsymbol" not in df_illiquid.columns:
            print(f"Error: 'tradingsymbol' column not found in {illiquid_csv}")
            return
    except Exception as e:
        print(f"Error reading {illiquid_csv}: {e}")
        return
    
    # Filter out illiquid stocks
    print("Filtering liquid stocks...")
    initial_count = len(df_all)
    # Normalize illiquid symbols by removing .NS
    illiquid_symbols = set(df_illiquid["tradingsymbol"].apply(normalize_symbol).astype(str))
    # Keep only stocks not in illiquid_symbols
    df_liquid = df_all[~df_all["tradingsymbol"].astype(str).isin(illiquid_symbols)]
    liquid_count = len(df_liquid)
    
    # Save liquid stocks to output CSV
    try:
        df_liquid.to_csv(output_csv, index=False)
        print(f"Removed {initial_count - liquid_count} illiquid stocks.")
        print(f"Saved {liquid_count} liquid stocks to {output_csv}.")
    except Exception as e:
        print(f"Error saving {output_csv}: {e}")
        return
    
    # Report any illiquid stocks not found in Instruments.csv
    missing_symbols = illiquid_symbols - set(df_all["tradingsymbol"].astype(str))
    if missing_symbols:
        print(f"Warning: {len(missing_symbols)} stocks in {illiquid_csv} not found in {input_csv}:")
        print(", ".join(sorted(missing_symbols)))

if __name__ == "__main__":
    main()