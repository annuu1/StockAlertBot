import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timedelta

# File paths
input_csv = "Instruments.csv"
output_csv = "illiquid_stocks.csv"
last_processed_file = "last_processed.txt"

# Illiquidity criteria
MAX_OHLC_EQUAL_DAYS = 5  # Max days where Open=High=Low=Close
MIN_PRICE = 10.0  # Minimum current price (₹)
WINDOW_15_DAYS = 15  # Window to check for same high

def append_ns(symbol):
    """Append .NS to symbol if not already present."""
    return symbol + ".NS" if not symbol.endswith(".NS") else symbol

def is_illiquid(symbol):
    """Check if a stock is illiquid based on OHLC equality, price, 15-day high consistency, or no data."""
    try:
        # Fetch 1 year of daily data
        stock = yf.Ticker(symbol)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        hist = stock.history(start=start_date, end=end_date, interval="1d")
        
        if hist.empty:
            return None, None, None, True, "No data or delisted"
        
        # Check OHLC equality (Open=High=Low=Close)
        ohlc_equal_days = ((hist["Open"] == hist["High"]) & 
                          (hist["High"] == hist["Low"]) & 
                          (hist["Low"] == hist["Close"])).sum()
        
        # Get current price (latest close)
        current_price = hist["Close"].iloc[-1] if not hist.empty else "Price too low"
        
        # Check if highs are the same for any 15 consecutive days
        has_same_high_15_days = False
        highs = hist["High"]
        for i in range(len(highs) - WINDOW_15_DAYS + 1):
            if all(highs.iloc[i] == highs.iloc[i+j] for j in range(WINDOW_15_DAYS)):
                has_same_high_15_days = True
                break
        
        # Determine illiquidity and reason
        reasons = []
        if ohlc_equal_days > MAX_OHLC_EQUAL_DAYS:
            reasons.append("Too many OHLC equal days")
        if current_price <= MIN_PRICE:
            reasons.append("Price too low")
        if has_same_high_15_days:
            reasons.append("Same high for 15 days")
        
        is_illiquid_stock = len(reasons) > 0
        reason = ", ".join(reasons) if reasons else ""
        
        return ohlc_equal_days, current_price, has_same_high_15_days, is_illiquid_stock, reason
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None, None, None, True, "No data or delisted"

def save_illiquid_stock(symbol, ohlc_equal_days, current_price, has_same_high_15_days, reason):
    """Append illiquid stock to CSV."""
    data = {
        "tradingsymbol": symbol,
        "ohlc_equal_days": ohlc_equal_days,
        "current_price": current_price,
        "has_same_high_15_days": has_same_high_15_days,
        "reason": reason
    }
    df = pd.DataFrame([data])
    # Append to CSV (create if doesn't exist)
    if not os.path.exists(output_csv):
        df.to_csv(output_csv, index=False)
    else:
        df.to_csv(output_csv, mode="a", header=False, index=False)

def save_last_processed(symbol):
    """Save the last processed stock symbol to a file."""
    with open(last_processed_file, "w") as f:
        f.write(symbol)

def get_last_processed():
    """Read the last processed stock symbol."""
    if os.path.exists(last_processed_file):
        with open(last_processed_file, "r") as f:
            return f.read().strip()
    return None

def main():
    # Read input CSV
    try:
        df = pd.read_csv(input_csv)
        if "tradingsymbol" not in df.columns:
            print("Error: 'tradingsymbol' column not found in Instruments.csv")
            return
        symbols = df["tradingsymbol"].tolist()
    except Exception as e:
        print(f"Error reading {input_csv}: {e}")
        return
    
    # Get last processed stock
    last_processed = get_last_processed()
    start_index = 0
    if last_processed:
        try:
            start_index = symbols.index(last_processed) + 1
        except ValueError:
            print(f"Last processed symbol {last_processed} not found, starting from beginning.")
    
    # Process each stock
    for i in range(start_index, len(symbols)):
        symbol = append_ns(symbols[i])
        print(f"Processing {symbol} ({i+1}/{len(symbols)})...")
        
        ohlc_equal_days, current_price, has_same_high_15_days, is_illiquid_stock, reason = is_illiquid(symbol)
        
        if is_illiquid_stock:
            print(f"{symbol} is illiquid. Reason: {reason}. Saving to {output_csv}.")
            save_illiquid_stock(symbol, ohlc_equal_days, current_price, has_same_high_15_days, reason)
        
        # Save progress
        save_last_processed(symbols[i])
    
    print("Processing complete.")

if __name__ == "__main__":
    main()