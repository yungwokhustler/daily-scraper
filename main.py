import asyncio
import json
import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from src.classification import LLMClassifier
from src.config.sources import get_all_sources
from src.scraper.telegram_scrap import TelegramScraper
from src.logger import get_logger
from src.notification import send_error_to_telegram, close_notification_session
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
    list_channel_id_discord = [s["channel_id"] for s in sources if s["platform"] == "discord"]
    list_channel_id_telegram = [s["channel_id"] for s in sources if s["platform"] == "telegram"]

    # Telegram Init
    tg_scraper = TelegramScraper()
    client = await tg_scraper.login()

    try:

        df_combined, run_stats = await scrape_all_sources(telethon_client=client,
                                                          telegram_groups=list_channel_id_telegram,
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
        # Date the file in the process's local TZ; the launchd job pins TZ=Asia/Jakarta
        # so the name stays a WIB date even when the Mac is in another timezone.
        today = datetime.now().strftime("%Y-%m-%d")
        # OUTPUT_DIR lets the local launchd run write straight into the CoWork folder
        # (no git, no rsync). Defaults to repo out/ for manual/CI runs.
        out_dir = os.getenv("OUTPUT_DIR", "out")
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, f"{today}-crypto_tg_news.json")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(json_output)

        # Log Summery
        total_pulled = sum(log["pulled"] for log in run_stats)
        total_kept = sum(log["kept"] for log in run_stats)
        logger.info(f"📊 [TOTAL]  Pulled: {total_pulled} | Kept: {total_kept}")

    finally:
        await tg_scraper.close()
        await close_notification_session()


if __name__ == "__main__":
    asyncio.run(main())
