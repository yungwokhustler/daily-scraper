import asyncio
import json
import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from src.classification import LLMClassifier
from src.config.database import SupabaseDB
from src.config.sources import get_all_sources
from src.scraper.telegram_scrap import TelegramScraper
from src.logger import get_logger
from src.notification import send_dataframe_to_telegram, send_notify_telegram, send_error_to_telegram, close_notification_session
from src.utils import scrape_all_sources

load_dotenv()
logger = get_logger("Main")


async def main():

    session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_session")
    session_file = f"{session_name}.session"
    if not os.path.exists(session_file):
        error_msg = f"❌ File session '{session_file}' not found. \nManual login is required once in an interactive environment."
        logger.error(error_msg)
        await send_error_to_telegram(error_msg)
        raise RuntimeError(error_msg)

    # Load sources from CSV (no DB needed)
    sources = get_all_sources()
    list_channel_id_elfa = [s["channel_id"] for s in sources if s["platform"] == "elfa"]
    list_channel_id_discord = [s["channel_id"] for s in sources if s["platform"] == "discord"]
    list_channel_id_telegram = [s["channel_id"] for s in sources if s["platform"] == "telegram"]

    # Telegram Init
    tg_scraper = TelegramScraper()
    client = await tg_scraper.login()

    db = None
    try:

        df_combined, run_stats = await scrape_all_sources(telethon_client=client,
                                                          telegram_groups=list_channel_id_telegram,
                                                          elfa_endpoints=list_channel_id_elfa,
                                                          discord_channels=list_channel_id_discord,
                                                          max_concurrent=10)
        # Classification
        classification_class = LLMClassifier()
        df = await classification_class.classify(df=df_combined, batch_size=10, max_concurrent=5)

        df_merged = pd.merge(df_combined, df, on=["id", "platform"], how="left")

        records = df_merged.to_dict(orient="records")
        # records = df_combined.to_dict(orient="records")
        json_output = json.dumps(
            records,
            indent=2,
            ensure_ascii=False,
            default=str
        )
        today = datetime.now().strftime("%Y-%m-%d")
        os.makedirs("out", exist_ok=True)
        output_path = f"out/{today}.json"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(json_output)

        # Log Summery
        total_pulled = sum(log["pulled"] for log in run_stats)
        total_kept = sum(log["kept"] for log in run_stats)
        logger.info(f"📊 [TOTAL]  Pulled: {total_pulled} | Kept: {total_kept}")
        await send_notify_telegram(f"📊 [TOTAL] Pulled: {total_pulled} | Kept: {total_kept}")
        await send_dataframe_to_telegram(df_merged, today)

        # Save to database (lazy init — only connect when we need to log)
        if run_stats:
            db = SupabaseDB()
            await db.initialize()
            await db.insert_log_runs_batch(run_stats)
            await db.close()
        else:
            logger.warning("⚠️ No scraping stats collected.")

    finally:
        await tg_scraper.close()
        await close_notification_session()


if __name__ == "__main__":
    asyncio.run(main())
