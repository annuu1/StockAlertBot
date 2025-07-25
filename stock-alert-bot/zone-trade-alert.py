import asyncio
import yfinance as yf
from datetime import datetime
import pytz
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import aiohttp

# Load environment variables
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")

# MongoDB setup
client = AsyncIOMotorClient(MONGO_URI)
db = client["stock_zones"]
zone_collection = db["demand_zones"]
trade_collection = db["trades"]

IST = pytz.timezone("Asia/Kolkata")

def patch_symbol(symbol: str) -> str:
    return symbol if '.' in symbol else symbol + '.NS'

async def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise Exception(f"Telegram API Error: {text}")

async def process_alerts():
    print("\n🚦 Processing Combined Alerts")

    zones = await zone_collection.find({"freshness": {"$gt": 0}}).to_list(None)
    trades = await trade_collection.find({"status": "OPEN"}).to_list(None)

    zone_symbols = {patch_symbol(zone["ticker"]) for zone in zones}
    trade_symbols = {patch_symbol(trade["symbol"]) for trade in trades}
    all_symbols = sorted(zone_symbols.union(trade_symbols))
    print(f"📊 Fetching data for {len(all_symbols)} stocks")

    # Fetch all stock data in one batch
    data = yf.download(tickers=all_symbols, period="1d", group_by="ticker", threads=True, progress=False)
    day_lows = {}

    for symbol in all_symbols:
        try:
            df = data[symbol] if len(all_symbols) > 1 else data
            if not df.empty:
                day_lows[symbol] = df["Low"].iloc[-1]
        except Exception as e:
            print(f"⚠️ Failed to parse {symbol}: {e}")

    now = datetime.now(IST)

    # Process Zones
    for zone in zones:
        symbol_raw = zone["ticker"]
        symbol = patch_symbol(symbol_raw)
        day_low = day_lows.get(symbol)
        if day_low is None:
            continue

        proximal = zone["proximal_line"]
        distal = zone["distal_line"]
        zone_id = zone["zone_id"]
        alert_sent = zone.get("zone_alert_sent", False)
        entry_sent = zone.get("zone_entry_sent", False)

        try:
            if not alert_sent and 0 < abs(proximal - day_low) / proximal <= 0.03:
                msg = f"📶 *{symbol_raw}* zone approaching\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}, Day Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one({"_id": zone["_id"]}, {"$set": {"zone_alert_sent": True}})

            if not entry_sent and day_low <= proximal:
                msg = f"🎯 *{symbol_raw}* zone entry hit!\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}, Day Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one({"_id": zone["_id"]}, {"$set": {"zone_entry_sent": True}})

            if day_low < distal:
                msg = f"🛑 *{symbol_raw}* zone breached distal!\nZone ID: `{zone_id}`\nDistal: ₹{distal:.2f}, Day Low: ₹{day_low:.2f}\n⛔ Marking as not fresh"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]},
                    {"$set": {"freshness": 0, "trade_score": 0}}
                )
        except Exception as e:
            print(f"❌ Error in zone {zone_id}: {e}")

    # Process Trades
    for trade in trades:
        symbol_raw = trade["symbol"]
        symbol = patch_symbol(symbol_raw)
        entry_price = trade["entry_price"]
        alert_sent = trade.get("alert_sent", False)
        entry_sent = trade.get("entry_alert_sent", False)
        day_low = day_lows.get(symbol)

        if day_low is None:
            continue

        try:
            if not alert_sent and 0 < abs(entry_price - day_low) / entry_price <= 0.02:
                msg = f"⚠️ *{symbol_raw}* approaching entry ₹{entry_price:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await trade_collection.update_one({"_id": trade["_id"]}, {"$set": {"alert_sent": True}})

            elif not entry_sent and day_low <= entry_price:
                msg = f"✅ *{symbol_raw}* entry hit ₹{entry_price:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await trade_collection.update_one({"_id": trade["_id"]}, {"$set": {"entry_alert_sent": True}})

            elif alert_sent and not entry_sent and (now.hour > 15 or (now.hour == 15 and now.minute >= 30)):
                await trade_collection.update_one({"_id": trade["_id"]}, {"$set": {"alert_sent": False}})
        except Exception as e:
            print(f"❌ Error in trade {symbol_raw}: {e}")

async def main_loop():
    while True:
        try:
            await process_alerts()
        except Exception as e:
            print(f"🔥 Error in main loop: {e}")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main_loop())
