import asyncio
import yfinance as yf
from datetime import datetime
import pytz
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import aiohttp

# Load .env variables
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")

print("🔧 Environment Loaded:")
print("TELEGRAM_TOKEN:", TELEGRAM_TOKEN)
print("TELEGRAM_CHAT_ID:", TELEGRAM_CHAT_ID)
print("MONGO_URI:", MONGO_URI)

client = AsyncIOMotorClient(MONGO_URI)
db = client["stock_zones"]
zone_collection = db["demand_zones"]

IST = pytz.timezone("Asia/Kolkata")

def patch_symbol(symbol: str) -> str:
    """Appends '.NS' if no exchange suffix found (assumes NSE by default)."""
    if '.' not in symbol:
        return symbol + '.NS'
    return symbol

async def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    print("\n📤 Sending Telegram Message:")
    print("URL:", url)
    print("Payload:", payload)

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload) as resp:
            text = await resp.text()
            print("🔁 Response Status:", resp.status)
            print("📨 Response Text:", text)
            if resp.status != 200:
                raise Exception(f"Telegram API Error: {text}")

async def check_zones():
    print("\n📡 Checking Zones...")

    # Step 1: Fetch all fresh zones
    zones = await zone_collection.find({"freshness": {"$gt": 0}}).to_list(None)
    if not zones:
        print("📭 No fresh zones found.")
        return

    # Step 2: Get unique symbols
    tickers = list(set(patch_symbol(zone["ticker"]) for zone in zones))
    print(f"📈 Unique symbols to fetch: {tickers}")

    # Step 3: Fetch data once per symbol
    price_data = {}
    for symbol in tickers:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")
            if not hist.empty:
                price_data[symbol] = hist["Low"].iloc[-1]
                print(f"✅ Fetched data for {symbol}: Day Low ₹{price_data[symbol]:.2f}")
            else:
                print(f"⚠️ No data for {symbol}")
        except Exception as e:
            print(f"❌ Error fetching data for {symbol}: {e}")

    now = datetime.now(IST)

    # Step 4: Process zones using cached price data
    for zone in zones:
        symbol_raw = zone["ticker"]
        symbol = patch_symbol(symbol_raw)
        zone_id = zone["zone_id"]
        proximal = zone["proximal_line"]
        distal = zone["distal_line"]
        day_low = price_data.get(symbol)

        if day_low is None:
            print(f"🚫 Skipping {symbol_raw} (no price data)")
            continue

        zone_alert_sent = zone.get("zone_alert_sent", False)
        zone_entry_sent = zone.get("zone_entry_sent", False)

        print(f"\n🧭 Zone Check: {symbol_raw} | Zone ID: {zone_id}")
        print(f"➡️ Proximal ₹{proximal:.2f} | Distal ₹{distal:.2f} | Day Low ₹{day_low:.2f}")

        try:
            # Approaching alert
            if not zone_alert_sent and 0 < abs(proximal - day_low) / proximal <= 0.03:
                msg = f"📶 *{symbol_raw}* zone approaching entry\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]}, {"$set": {"zone_alert_sent": True}}
                )

            # Entry alert
            if not zone_entry_sent and day_low <= proximal:
                msg = f"🎯 *{symbol_raw}* zone entry hit!\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]}, {"$set": {"zone_entry_sent": True}}
                )

            # Distal breach → freshness = 0, trade_score = 0
            if day_low < distal:
                msg = f"🛑 *{symbol_raw}* zone breached distal!\nZone ID: `{zone_id}`\nDistal: ₹{distal:.2f}\nDay Low: ₹{day_low:.2f}\n⚠️ Marking as no longer fresh"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]},
                    {"$set": {"freshness": 0, "trade_score": 0}}
                )
                print(f"❄️ Marked not fresh: {symbol_raw}")

        except Exception as e:
            print(f"❌ Error processing zone {zone_id}: {e}")

async def main_loop():
    while True:
        try:
            await check_zones()
        except Exception as e:
            print(f"🔥 Error in main loop: {e}")
        await asyncio.sleep(300)  # every 5 minutes

if __name__ == "__main__":
    asyncio.run(main_loop())
