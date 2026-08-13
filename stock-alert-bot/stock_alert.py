import asyncio
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time
from typing import Optional, Dict, Any, List, Tuple
import pytz
import aiohttp
import yfinance as yf
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MONGO_URI = os.getenv("MONGODB_URI")
FORCE_RUN = os.getenv("FORCE_RUN", "false").lower() in ("true", "1", "yes")

# Define Telegram Topic IDs for different alert types
APPROACH_TOPIC_ID = os.getenv("APPROACH_TOPIC_ID", "4")
ENTRY_TOPIC_ID = os.getenv("ENTRY_TOPIC_ID", "5")
BREACH_TOPIC_ID = os.getenv("BREACH_TOPIC_ID", "6")

logging.info(
    "Environment Loaded: TELEGRAM_CHAT_ID=%s, MONGO_URI=%s, FORCE_RUN=%s",
    TELEGRAM_CHAT_ID,
    (MONGO_URI.split("@")[-1] if "@" in MONGO_URI else MONGO_URI) if MONGO_URI else "Not set",
    FORCE_RUN
)

# MongoDB Setup
client = AsyncIOMotorClient(MONGO_URI)
db = client["stock_zones"]
zone_collection = db["demand_zones"]
symbols_collection = db["symbols"]
stats_collection = db["stats"]
config_collection = db["config"]

IST = pytz.timezone("Asia/Kolkata")


def patch_symbol(symbol: str) -> str:
    """Appends '.NS' if no exchange suffix found (assumes NSE by default)."""
    if '.' not in symbol:
        return symbol + '.NS'
    return symbol


def parse_timestamp_to_ist(val: Any) -> Optional[datetime]:
    """Parses various timestamp formats (datetime, ISO string, epoch) to IST datetime."""
    if val is None:
        return None
    if isinstance(val, datetime):
        if val.tzinfo is None:
            # Stored in Mongo typically as UTC naive
            return pytz.utc.localize(val).astimezone(IST)
        return val.astimezone(IST)
    if isinstance(val, (int, float)):
        # Epoch timestamp (seconds or milliseconds)
        if val > 1e11:
            val = val / 1000.0
        return datetime.fromtimestamp(val, tz=pytz.utc).astimezone(IST)
    if isinstance(val, str):
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return pytz.utc.localize(dt).astimezone(IST)
            return dt.astimezone(IST)
        except Exception:
            pass
    return None


async def send_telegram_message(message: str, message_thread_id: Optional[str] = None, chat_id: Optional[str] = None):
    """Sends a Telegram message asynchronously."""
    if not TELEGRAM_TOKEN or not (chat_id or TELEGRAM_CHAT_ID):
        logging.warning("Telegram credentials not configured. Message skipped: %s", message[:50])
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": chat_id or TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    if message_thread_id is not None:
        payload["message_thread_id"] = str(message_thread_id)

    logging.info("Sending Telegram Message: ChatID=%s, ThreadID=%s", payload["chat_id"], message_thread_id)

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload) as resp:
            text = await resp.text()
            if resp.status != 200:
                logging.error("Telegram API Error (%s): %s", resp.status, text)
                raise Exception(f"Telegram API Error: {text}")


async def get_additional_alert_groups() -> List[Dict[str, str]]:
    """Loads dynamically configured secondary alert groups from MongoDB config collection."""
    try:
        docs = await config_collection.find({"key": {"$regex": "^alertGroup"}}).to_list(None)
        groups: Dict[str, Dict[str, str]] = {}
        for d in docs:
            k = d.get("key", "")
            val = d.get("value")
            if not k:
                continue
            parts = k.split("_", 1)
            base = parts[0]
            suffix = parts[1] if len(parts) > 1 else None
            if base not in groups:
                groups[base] = {"key": base}
            if suffix is None:
                groups[base]["chat_id"] = str(val) if val is not None else None
            else:
                groups[base][suffix] = str(val) if val is not None else None

        result = [g for g in groups.values() if g.get("chat_id")]
        logging.info("Loaded additional alert groups: %s", len(result))
        return result
    except Exception as e:
        logging.error("Failed to load additional alert groups: %s", e)
        return []


async def get_price_last_update() -> Tuple[Optional[datetime], Optional[float]]:
    """
    Checks the stats collection (and fallback to symbols collection) for the latest price update time.
    Returns (price_last_update_dt_ist, age_in_minutes).
    """
    now = datetime.now(IST)
    last_update_dt: Optional[datetime] = None

    # 1. Check stats collection for price_last_update
    try:
        stats_doc = await stats_collection.find_one({
            "$or": [
                {"key": "price_last_update"},
                {"price_last_update": {"$exists": True}},
                {"name": "price_last_update"}
            ]
        })
        if stats_doc:
            raw_val = stats_doc.get("value") or stats_doc.get("price_last_update") or stats_doc.get("updatedAt")
            last_update_dt = parse_timestamp_to_ist(raw_val)
            if last_update_dt:
                logging.info("Found price_last_update from stats collection: %s", last_update_dt)
    except Exception as e:
        logging.warning("Could not fetch price_last_update from stats collection: %s", e)

    # 2. Fallback: check latest updated_at in symbols collection
    if not last_update_dt:
        try:
            latest_sym = await symbols_collection.find_one(sort=[("updated_at", -1)])
            if latest_sym:
                raw_sym_val = latest_sym.get("updated_at") or latest_sym.get("last_updated")
                last_update_dt = parse_timestamp_to_ist(raw_sym_val)
                if last_update_dt:
                    logging.info("Found latest symbol update timestamp from symbols collection: %s", last_update_dt)
        except Exception as e:
            logging.warning("Could not fetch latest timestamp from symbols collection: %s", e)

    if last_update_dt:
        age_minutes = (now - last_update_dt).total_seconds() / 60.0
        return last_update_dt, age_minutes

    return None, None


async def fetch_prices_from_symbols_collection(tickers: List[str]) -> Dict[str, float]:
    """Fetches intraday day_low / ltp from MongoDB symbols collection."""
    raw_tickers = [t.replace(".NS", "") for t in tickers]
    symbol_filter = list(set(tickers + raw_tickers))

    price_data: Dict[str, float] = {}
    cursor = symbols_collection.find(
        {"symbol": {"$in": symbol_filter}},
        {"symbol": 1, "day_low": 1, "ltp": 1}
    )

    async for doc in cursor:
        sym = doc.get("symbol")
        price = doc.get("day_low") or doc.get("ltp")
        if sym and price is not None:
            price_val = float(price)
            price_data[sym] = price_val
            price_data[patch_symbol(sym)] = price_val

    return price_data


def _download_chunk(chunk: List[str]) -> Dict[str, float]:
    """Worker function to download a single chunk of tickers via yfinance."""
    results: Dict[str, float] = {}
    try:
        data = yf.download(chunk, period="1d", group_by="ticker", threads=True, progress=False)
        for symbol in chunk:
            try:
                ticker_df = data if len(chunk) == 1 else (data[symbol] if symbol in data else None)
                if ticker_df is not None and not ticker_df.empty:
                    low_series = ticker_df["Low"].dropna()
                    if not low_series.empty:
                        low_val = float(low_series.iloc[-1])
                        results[symbol] = low_val
                        results[symbol.replace(".NS", "")] = low_val
            except Exception:
                pass
    except Exception as e:
        logging.error("Error downloading chunk: %s", e)
    return results


def fetch_prices_from_yfinance_batch(tickers: List[str], chunk_size: int = 100, max_workers: int = 8) -> Dict[str, float]:
    """Batch-fetches prices in parallel using ThreadPoolExecutor for fast downloads."""
    price_data: Dict[str, float] = {}
    patched_tickers = list(set(patch_symbol(t) for t in tickers))
    chunks = [patched_tickers[i:i + chunk_size] for i in range(0, len(patched_tickers), chunk_size)]

    logging.info("Fetching yfinance data for %d tickers in %d parallel batches...", len(patched_tickers), len(chunks))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_download_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            chunk_res = future.result()
            price_data.update(chunk_res)

    return price_data


async def check_zones() -> Dict[str, Any]:
    """
    Main evaluation pipeline:
    1. Checks market hours (or FORCE_RUN)
    2. Queries fresh zones (freshness > 1.5)
    3. Checks price freshness in stats collection (<= 10 mins) -> Uses symbols collection or yfinance batch
    4. Proximity-based filtering (Smart Tiering) -> Evaluates hot zones
    5. Dispatches alerts & updates MongoDB state
    6. Returns execution summary stats
    """
    now = datetime.now(IST)
    start_time = datetime.now(IST)

    # 1. Check market hours (Mon-Fri, 9:15 AM - 3:30 PM IST)
    is_weekend = now.weekday() >= 5  # 5=Saturday, 6=Sunday
    is_outside_hours = now.time() < time(9, 15) or now.time() > time(15, 30)

    if (is_weekend or is_outside_hours) and not FORCE_RUN:
        logging.info("Outside market hours (Mon-Fri 9:15 AM - 3:30 PM IST), skipping. (FORCE_RUN=false)")
        return {
            "status": "SKIPPED_MARKET_CLOSED",
            "message": "Outside market hours",
            "duration_seconds": 0
        }

    logging.info("Starting Zone Alert Check...")

    # 2. Fetch all fresh zones (Filter out tested zones: freshness must be > 1.5)
    zones = await zone_collection.find({"freshness": {"$gt": 1.5}}).to_list(None)
    if not zones:
        logging.info("No fresh zones (freshness > 1.5) found in database.")
        return {
            "status": "SUCCESS",
            "message": "No fresh zones found",
            "total_fresh_zones": 0,
            "duration_seconds": (datetime.now(IST) - start_time).total_seconds()
        }

    logging.info("Found %d fresh zones (freshness > 1.5).", len(zones))

    # Extract unique tickers
    tickers = list(set(zone["ticker"] for zone in zones if "ticker" in zone))
    logging.info("Unique symbols across fresh zones: %d", len(tickers))

    # 3. Determine price data source based on stats collection price_last_update (within 10 mins)
    price_last_update_dt, price_age_minutes = await get_price_last_update()
    is_cache_fresh = (price_age_minutes is not None and price_age_minutes <= 10.0)

    price_data: Dict[str, float] = {}
    data_source_used = ""

    if is_cache_fresh:
        data_source_used = "symbols_collection"
        logging.info("✅ Price cache is FRESH (%.1f mins old <= 10m). Using symbols collection for prices.", price_age_minutes)
        price_data = await fetch_prices_from_symbols_collection(tickers)
        logging.info("Retrieved prices for %d symbols from DB symbols collection.", len(price_data) // 2)
    else:
        data_source_used = "yfinance_batch"
        age_info = f"{price_age_minutes:.1f} mins old" if price_age_minutes is not None else "missing"
        logging.info("⚠️ Price cache is STALE (%s > 10m). Falling back to parallel yfinance batch download.", age_info)
        loop = asyncio.get_event_loop()
        price_data = await loop.run_in_executor(None, fetch_prices_from_yfinance_batch, tickers)
        logging.info("Retrieved prices for %d symbols from yfinance batch.", len(price_data) // 2)

    # 4. Proximity-Based Filtering (Smart Tiering) & Alert Processing
    additional_groups = await get_additional_alert_groups()
    now_str = datetime.now(IST).isoformat()

    hot_zones_count = 0
    approaching_sent_count = 0
    entry_sent_count = 0
    breach_count = 0

    for zone in zones:
        symbol_raw = zone.get("ticker", "")
        symbol_patched = patch_symbol(symbol_raw)
        zone_id = zone.get("zone_id", "N/A")
        timeframes = zone.get("timeframes", ["1D"])
        timeframe = timeframes[0].upper() if timeframes else "1D"
        proximal = zone.get("proximal_line")
        distal = zone.get("distal_line")
        freshness = zone.get("freshness", 3.0)

        # Get day low or current price
        day_low = price_data.get(symbol_raw) or price_data.get(symbol_patched)

        if day_low is None or proximal is None or distal is None:
            continue

        if not isinstance(proximal, (int, float)) or not isinstance(distal, (int, float)) or proximal <= distal:
            continue

        # Proximity Check (Tiered Scheduling):
        # Distance % between day_low and proximal line
        dist_pct = abs(proximal - day_low) / proximal

        # Consider zone "Hot" if price is within 8% or already entered/below proximal
        is_hot = (dist_pct <= 0.08) or (day_low <= proximal)
        if not is_hot:
            # Cold / Far zone: skip alerting evaluation
            continue

        hot_zones_count += 1
        zone_alert_sent = zone.get("zone_alert_sent", False)
        zone_entry_sent = zone.get("zone_entry_sent", False)

        try:
            # Approaching Zone Alert (within 3% of proximal line)
            if not zone_alert_sent and 0 < dist_pct <= 0.03:
                msg = f"📶 *{symbol_raw}* - *Approaching Zone*\n ----------- \nTF: `{timeframe}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}\n"
                await send_telegram_message(msg, APPROACH_TOPIC_ID)
                for grp in additional_groups:
                    thread_id = grp.get("approach") or APPROACH_TOPIC_ID
                    await send_telegram_message(msg, thread_id, chat_id=grp.get("chat_id"))

                await zone_collection.update_one(
                    {"_id": zone["_id"]},
                    {"$set": {"zone_alert_sent": True, "zone_alert_time": now_str}}
                )
                approaching_sent_count += 1
                logging.info("Sent Approaching Alert for %s (Zone: %s)", symbol_raw, zone_id)

            # Zone Entry Alert (day_low <= proximal)
            if not zone_entry_sent and day_low <= proximal:
                msg = f"🎯 *{symbol_raw}* Zone Entry!\n ----------- \nTF: `{timeframe}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}\n"
                await send_telegram_message(msg, ENTRY_TOPIC_ID)
                for grp in additional_groups:
                    thread_id = grp.get("entry") or ENTRY_TOPIC_ID
                    await send_telegram_message(msg, thread_id, chat_id=grp.get("chat_id"))

                await zone_collection.update_one(
                    {"_id": zone["_id"]},
                    {"$set": {"zone_entry_sent": True, "zone_entry_time": now_str}}
                )
                entry_sent_count += 1
                logging.info("Sent Zone Entry Alert for %s (Zone: %s)", symbol_raw, zone_id)

            # Distal Breach -> Mark freshness = 0, trade_score = 0
            if day_low < distal:
                await zone_collection.update_one(
                    {"_id": zone["_id"]},
                    {"$set": {"freshness": 0, "trade_score": 0, "zone_breach_time": now_str}}
                )
                breach_count += 1
                logging.info("Marked zone as breached (freshness=0): %s (Zone: %s)", symbol_raw, zone_id)

        except Exception as e:
            logging.error("Error processing zone %s: %s", zone_id, e)

    execution_duration = (datetime.now(IST) - start_time).total_seconds()
    logging.info(
        "Zone Check Completed in %.2fs. Fresh: %d, Hot: %d, Approach Alerts: %d, Entry Alerts: %d, Breaches: %d",
        execution_duration, len(zones), hot_zones_count, approaching_sent_count, entry_sent_count, breach_count
    )

    return {
        "status": "SUCCESS",
        "duration_seconds": round(execution_duration, 2),
        "data_source_used": data_source_used,
        "price_last_update": price_last_update_dt.isoformat() if price_last_update_dt else None,
        "price_age_minutes": round(price_age_minutes, 2) if price_age_minutes is not None else None,
        "total_fresh_zones": len(zones),
        "unique_symbols_count": len(tickers),
        "hot_zones_count": hot_zones_count,
        "approaching_alerts_sent": approaching_sent_count,
        "entry_alerts_sent": entry_sent_count,
        "breaches_count": breach_count
    }


async def save_execution_stats(summary: Dict[str, Any], error_msg: Optional[str] = None):
    """Updates/upserts the execution stats in the MongoDB stats collection for UI & monitoring."""
    now = datetime.now(IST)
    try:
        stat_payload = {
            "key": "stock_alert_bot_stats",
            "name": "Stock Alert Bot Execution Stats",
            "last_run_time": now,
            "last_run_iso": now.isoformat(),
            "execution_duration_seconds": summary.get("duration_seconds", 0),
            "data_source_used": summary.get("data_source_used", "unknown"),
            "price_last_update": summary.get("price_last_update"),
            "price_age_minutes": summary.get("price_age_minutes"),
            "total_fresh_zones_evaluated": summary.get("total_fresh_zones", 0),
            "unique_symbols_monitored": summary.get("unique_symbols_count", 0),
            "hot_zones_evaluated": summary.get("hot_zones_count", 0),
            "approaching_alerts_sent": summary.get("approaching_alerts_sent", 0),
            "entry_alerts_sent": summary.get("entry_alerts_sent", 0),
            "breaches_recorded": summary.get("breaches_count", 0),
            "status": summary.get("status", "SUCCESS" if not error_msg else "FAILED"),
            "error": error_msg,
            "updated_at": now
        }
        await stats_collection.update_one(
            {"key": "stock_alert_bot_stats"},
            {"$set": stat_payload},
            upsert=True
        )
        logging.info("Successfully updated execution stats in stats collection.")
    except Exception as e:
        logging.error("Failed to write execution stats to stats collection: %s", e)


async def main():
    summary: Dict[str, Any] = {}
    error_msg: Optional[str] = None
    try:
        summary = await check_zones()
    except Exception as e:
        error_msg = str(e)
        logging.error("Error in main execution: %s", e, exc_info=True)
        try:
            await send_telegram_message(f"⚠️ Stock Alert Bot Execution Error: {e}")
        except Exception:
            pass
    finally:
        await save_execution_stats(summary, error_msg)
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
