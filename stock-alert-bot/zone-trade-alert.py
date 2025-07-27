import asyncio
import yfinance as yf
from datetime import datetime, time, timedelta
import pytz
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import aiohttp
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MONGO_URI = os.getenv("MONGODB_URI")

# Validate environment variables
if not all([TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, MONGO_URI]):
    error_msg = "Missing required environment variables: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, or MONGODB_URI"
    logger.error(error_msg)
    asyncio.run(send_telegram_message(error_msg))
    exit(1)

logger.info("Environment Loaded: TELEGRAM_TOKEN=%s, TELEGRAM_CHAT_ID=%s, MONGO_URI=%s", 
            TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, MONGO_URI)

# MongoDB setup
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
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    logger.info("Sending Telegram Message: %s", payload)
    
    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            async with session.post(url, data=payload) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 1))
                    logger.warning("Rate limit hit, retrying after %s seconds", retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                if resp.status != 200:
                    raise Exception(f"Telegram API Error: {await resp.text()}")
                logger.info("Message sent successfully")
                return
        raise Exception("Max retries reached for Telegram API")

async def check_zones():
    # Check market hours
    now = datetime.now(IST)
    if now.weekday() >= 7 or now.time() < time(9, 15) or now.time() > time(23, 30):
        logger.info("Outside market hours (9:15 AM - 3:30 PM IST), exiting.")
        exit(0)

    logger.info("Checking Zones...")

    # Fetch all fresh zones
    zones = await zone_collection.find({"freshness": {"$gt": 0}}).to_list(None)
    if not zones:
        logger.info("No fresh zones found.")
        return

    total_zones = len(zones)
    logger.info("Found %d fresh zones.", total_zones)

    # Get unique symbols
    tickers = list(set(patch_symbol(zone["ticker"]) for zone in zones))
    total_tickers = len(tickers)
    logger.info("Unique symbols to fetch: %s (%d total)", tickers, total_tickers)

    # Fetch data once per symbol
    price_data = {}
    logger.info("Fetching stock data...")
    try:
        data = yf.download(tickers, period="1d", group_by="ticker")
        for i, symbol in enumerate(tickers, 1):
            if symbol in data and not data[symbol].empty:
                price_data[symbol] = data[symbol]["Low"].iloc[-1]
                logger.info("Fetched data for %s: Day Low ₹%.2f (%d/%d)", 
                            symbol, price_data[symbol], i, total_tickers)
            else:
                logger.warning("No data for %s (%d/%d)", symbol, i, total_tickers)
    except Exception as e:
        logger.error("Error fetching batch data: %s", e)
        await send_telegram_message(f"⚠️ Error fetching stock data: {str(e)}")

    # Process zones using cached price data
    logger.info("Processing zones...")
    for i, zone in enumerate(zones, 1):
        symbol_raw = zone["ticker"]
        symbol = patch_symbol(symbol_raw)
        zone_id = zone["zone_id"]
        proximal = zone["proximal_line"]
        distal = zone["distal_line"]
        day_low = price_data.get(symbol)

        if day_low is None or not isinstance(proximal, (int, float)) or not isinstance(distal, (int, float)):
            logger.info("Skipping %s: No price data or invalid zone data (%d/%d)", symbol_raw, i, total_zones)
            continue

        if proximal <= distal:
            logger.warning("Invalid zone for %s: proximal=%s, distal=%s (%d/%d)", 
                          symbol_raw, proximal, distal, i, total_zones)
            continue

        zone_alert_sent = zone.get("zone_alert_sent", False)
        zone_entry_sent = zone.get("zone_entry_sent", False)
        last_alert_time = zone.get("last_alert_time")

        # Prevent duplicate alerts within 30 minutes
        if last_alert_time and now - last_alert_time < timedelta(minutes=30):
            logger.info("Skipping alert for %s: Recent alert sent (%d/%d)", symbol_raw, i, total_zones)
            continue

        logger.info("Zone Check: %s | Zone ID: %s (%d/%d)", symbol_raw, zone_id, i, total_zones)
        logger.info("Proximal ₹%.2f | Distal ₹%.2f | Day Low ₹%.2f", proximal, distal, day_low)

        try:
            # Approaching alert (within 3% of proximal)
            if not zone_alert_sent and 0 < abs(proximal - day_low) / proximal <= 0.03:
                msg = f"📶 *{symbol_raw}* zone approaching entry\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]}, 
                    {"$set": {"zone_alert_sent": True, "last_alert_time": now}}
                )
                logger.info("Sent approaching alert for %s (%d/%d)", symbol_raw, i, total_zones)

            # Entry alert
            if not zone_entry_sent and day_low <= proximal:
                msg = f"🎯 *{symbol_raw}* zone entry hit!\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]}, 
                    {"$set": {"zone_entry_sent": True, "last_alert_time": now}}
                )
                logger.info("Sent entry alert for %s (%d/%d)", symbol_raw, i, total_zones)

            # Distal breach → freshness = 0, trade_score = 0
            if day_low < distal:
                msg = f"🛑 *{symbol_raw}* zone breached distal!\nZone ID: `{zone_id}`\nDistal: ₹{distal:.2f}\nDay Low: ₹{day_low:.2f}\n⚠️ Marking as no longer fresh"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]},
                    {"$set": {"freshness": 0, "trade_score": 0, "last_alert_time": now}}
                )
                logger.info("Marked not fresh: %s (%d/%d)", symbol_raw, i, total_zones)

        except Exception as e:
            logger.error("Error processing zone %s: %s (%d/%d)", zone_id, e, i, total_zones)
            await send_telegram_message(f"⚠️ Error processing zone {zone_id}: {str(e)}")

async def main():
    start_time = datetime.now(IST)
    try:
        await check_zones()
    except Exception as e:
        logger.error("Error in main: %s", e)
        await send_telegram_message(f"🔥 Error in zone alert: {str(e)}")
    finally:
        client.close()
        logger.info("MongoDB client closed")
        duration = (datetime.now(IST) - start_time).total_seconds()
        logger.info("Execution completed in %.2f seconds", duration)

if __name__ == "__main__":
    asyncio.run(main())