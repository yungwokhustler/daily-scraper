import asyncio

import pandas as pd
from typing import List, Union, Optional
from datetime import datetime, timezone

from src.logger import get_logger
from src.scraper.discord_scrap import DiscordScraper
from src.scraper.telegram_scrap import TelegramScraper
from src.notification import send_error_to_telegram, send_notify_telegram
from src.types import ScrapeStats


logger = get_logger(__name__)


def get_endpoint_name(path_and_query: str) -> str:
    """
    Extract endpoint name from Elfa path+query string.

    Examples:
        "data/event-summary?keywords=..."      → "event-summary"
        "data/trending-narratives?timeFrame=day" → "trending-narratives"
        "/trending-narratives"                 → "trending-narratives"
    """

    path_part = path_and_query.split("?", 1)[0]

    return path_part.strip("/").split("/")[-1]


async def scrape_all_sources(
        telethon_client,  # Already-connected TelegramClient from TelegramScraper
        discord_channels: List[str],
        telegram_groups: List[Union[str, int]],
        elfa_endpoints: Optional[List[str]] = None,
        max_concurrent: int = 10
) -> tuple[pd.DataFrame, list[ScrapeStats]]:
    """
    Scrape messages from Discord, Telegram, and Elfa API concurrently.

    Args:
        discord_channels: List of Discord channel IDs (as strings).
        telegram_groups: List of Telegram group usernames or invite links.
        telethon_client: An already-connected and authorized TelegramClient.
        elfa_endpoints: Optional list of full Elfa API URLs.
        max_concurrent: Maximum number of concurrent scraping tasks.

    Returns:
        pd.DataFrame: Combined and deduplicated messages.
    """
    from src.scraper.elfa_scrap import ElfaScraper
    semaphore = asyncio.Semaphore(max_concurrent)
    all_dfs = []
    all_stats: list[ScrapeStats] = []

    # Initialize scrapers
    discord_scraper = DiscordScraper()
    elfa_scraper = ElfaScraper() if elfa_endpoints else None

    async def _scrape_discord(channel_id: str):
        async with semaphore:
            try:
                df_discord, stats = await discord_scraper.fetch_and_filter_messages(channel_id, max_retries=3)

                all_stats.append(stats)

                if not df_discord.empty:
                    all_dfs.append(df_discord)
            except Exception as e:
                await send_error_to_telegram(f"❌ Discord {channel_id} error: {str(e)}")
                logger.error(f"❌ Discord {channel_id} error: {str(e)}")
                all_stats.append(ScrapeStats(channel_id=channel_id,
                                             platform="discord",
                                             pulled=0,
                                             kept=0,
                                             success=False,
                                             error=str(e)))

    async def _scrape_telegram(group_id: Union[str, int]):
        async with semaphore:
            try:
                # Reuse the provided connected client
                scraper = TelegramScraper()
                scraper.client = telethon_client

                df_telegram, stats = await scraper.scrape_24h_to_df_telegram(group_id)

                all_stats.append(stats)

                if not df_telegram.empty:
                    all_dfs.append(df_telegram)
            except Exception as e:
                await send_error_to_telegram(f"❌ Telegram {group_id} error: {str(e)}")
                logger.error(f"❌ Telegram {group_id} error: {str(e)}")
                all_stats.append(ScrapeStats(channel_id=group_id,
                                             platform="telegram",
                                             pulled=0,
                                             kept=0,
                                             success=False,
                                             error=str(e)))

    async def _scrape_elfa(endpoint_url: str):
        async with semaphore:
            try:
                df_elfa, stats = await elfa_scraper.fetch_endpoint(endpoint_url)

                all_stats.append(stats)

                title_elfa = get_endpoint_name(endpoint_url)

                if not df_elfa.empty:
                    all_dfs.append(df_elfa)
            except Exception as e:
                await send_error_to_telegram(f"❌ Elfa {title_elfa} error: {str(e)}")
                logger.error(f"❌ Elfa {title_elfa} error: {str(e)}")
                all_stats.append(ScrapeStats(channel_id=title_elfa,
                                             platform="elfa",
                                             pulled=0,
                                             kept=0,
                                             success=False,
                                             error=str(e)))

    # Build tasks
    tasks = []
    tasks.extend(_scrape_discord(cid) for cid in discord_channels)
    tasks.extend(_scrape_telegram(gid) for gid in telegram_groups)
    if elfa_endpoints:
        tasks.extend(_scrape_elfa(url) for url in elfa_endpoints)

    # Run concurrently
    await asyncio.gather(*tasks)

    # Combine and deduplicate
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined['timestamp'] = pd.to_datetime(combined['timestamp'], errors='coerce', utc=True)
        combined = (
            combined
            .sort_values("timestamp")
            .drop_duplicates(subset=["text"], keep="last")
            .sort_values("timestamp", ascending=False)
            .reset_index(drop=True)
        )
        logger.info(f"✅ Total combined messages: {len(combined)}")
        return combined, all_stats
    else:
        await send_error_to_telegram("⚠️ No messages collected from any source.")
        logger.warning("⚠️ No messages collected from any source.")
        return pd.DataFrame(columns=["id", "text", "timestamp", "author", "source", "links"]), all_stats


def is_valid_endpoint_path(path_and_query: str) -> str:
    """
    Validate ONLY the path part (e.g., '/event-summary' or '/trending-narratives').

    Accepts full path+query like: '/event-summary?keywords=btc'
    """

    path = path_and_query.split("?")[0].rstrip("/")

    if path == "/event-summary":
        return "event-summary"
    elif path == "/trending-narratives":
        return "trending-narratives"
    else:
        raise ValueError(
            f"Unsupported endpoint path: \n'{path}'\n Only '/event-summary' and '/trending-narratives' allowed.")


def tweet_id_to_timestamp(tweet_id: str) -> datetime:
    TWITTER_EPOCH_MS = 1288834974657

    try:
        tweet_id_int = int(tweet_id)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)

    timestamp_ms = (tweet_id_int >> 22) + TWITTER_EPOCH_MS
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
