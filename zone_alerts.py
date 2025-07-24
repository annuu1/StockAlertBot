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

    async for zone in zone_collection.find({"freshness": {"$gt": 0}}):  # Only fresh zones
        symbol = patch_symbol(zone["ticker"])
        proximal = zone["proximal_line"]
        distal = zone["distal_line"]
        zone_id = zone["zone_id"]

        # Add flags in DB if not already there
        zone_alert_sent = zone.get("zone_alert_sent", False)
        zone_entry_sent = zone.get("zone_entry_sent", False)

        try:
            print(f"\n🧭 Zone Check: {zone['ticker']} | TFs: {zone.get('timeframes')} | Zone ID: {zone_id}")
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d")

            if data.empty:
                print(f"⚠️ No price data for {symbol}")
                continue

            day_low = data["Low"].iloc[-1]
            now = datetime.now(IST)

            print(f"ℹ️ {symbol} | Proximal: ₹{proximal:.2f} | Distal: ₹{distal:.2f} | Day Low: ₹{day_low:.2f}")

            # Approaching proximal
            if not zone_alert_sent and 0 < abs(proximal - day_low) / proximal <= 0.02:
                msg = f"📶 *{zone['ticker']}* zone approaching entry\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]}, 
                    {"$set": {"zone_alert_sent": True}}
                )

            # Entry hit (at or below proximal)
            if not zone_entry_sent and day_low <= proximal:
                msg = f"🎯 *{zone['ticker']}* zone entry hit!\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]}, 
                    {"$set": {"zone_entry_sent": True}}
                )

            # Breach distal → mark zone as no longer fresh
            if day_low < distal:
                msg = f"🛑 *{zone['ticker']}* zone breached distal line!\nZone ID: `{zone_id}`\nDistal: ₹{distal:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]}, 
                    {"$set": {"freshness": 0}}
                )
                print(f"❄️ Freshness reset to 0 for {zone['ticker']} zone")

        except Exception as e:
            print(f"❌ Error checking zone {zone_id}: {e}")

async def main_loop():
    while True:
        try:
            await check_zones()
        except Exception as e:
            print(f"🔥 Error in main loop: {e}")
        await asyncio.sleep(300)  # Run every 5 minutes

if __name__ == "__main__":
    asyncio.run(main_loop())
