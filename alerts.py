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
trade_collection = db["trades"]

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

async def check_trades():
    print("\n🔎 Checking Trades...")

    async for trade in trade_collection.find({"status": "OPEN"}):
        raw_symbol = trade["symbol"]
        symbol = patch_symbol(raw_symbol)
        entry_price = trade["entry_price"]
        alert_sent = trade.get("alert_sent", False)
        entry_alert_sent = trade.get("entry_alert_sent", False)

        try:
            print(f"\n🔍 Checking trade: {raw_symbol} (patched: {symbol})")
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d")

            if data.empty:
                print(f"⚠️ No price data for {symbol}")
                continue

            day_low = data["Low"].iloc[-1]
            now = datetime.now(IST)

            print(f"ℹ️ {raw_symbol} | Entry: ₹{entry_price:.2f} | Day Low: ₹{day_low:.2f} | Time: {now.strftime('%H:%M')}")

            # Approaching Alert
            if not alert_sent and 0 < abs(entry_price - day_low) / entry_price <= 0.02:
                msg = f"⚠️ *{raw_symbol}* is approaching entry price ₹{entry_price:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await trade_collection.update_one({"_id": trade["_id"]}, {"$set": {"alert_sent": True}})

            # Entry Hit Alert
            elif not entry_alert_sent and day_low <= entry_price:
                msg = f"✅ *{raw_symbol}* has hit the entry price ₹{entry_price:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await trade_collection.update_one({"_id": trade["_id"]}, {"$set": {"entry_alert_sent": True}})

            # Reset alert after market close (3:30 PM IST)
            elif alert_sent and not entry_alert_sent and now.hour >= 15 and now.minute >= 30:
                await trade_collection.update_one({"_id": trade["_id"]}, {"$set": {"alert_sent": False}})
                print(f"🔁 Reset alert for {raw_symbol} at end of day")

        except Exception as e:
            print(f"❌ Error checking {raw_symbol}: {e}")

async def main_loop():
    while True:
        try:
            await check_trades()
        except Exception as e:
            print(f"🔥 Error in main loop: {e}")
        await asyncio.sleep(300)  # Run every 5 minutes

if __name__ == "__main__":
    asyncio.run(main_loop())
