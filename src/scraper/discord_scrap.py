import os
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from src.logger import get_logger
from src.types import ScrapeStats
from src.notification import send_error_to_telegram
from src.normalization import filter_text, is_low_value_message

load_dotenv()


class DiscordScraper:
    def __init__(self):
        """
        Initialize the Discord scraper.
        Credentials are loaded from .env.
        """
        self.auth_token = os.getenv("DISCORD_TOKEN")
        if not self.auth_token:
            raise ValueError("DISCORD_TOKEN not found in .env")
        self.logger = get_logger(self.__class__.__name__)

    async def fetch_and_filter_messages(self, channel_id: str, max_retries: int = 3) -> tuple[pd.DataFrame, ScrapeStats]:
        """
        Fetch Discord messages from the last 24 hours, filter out spam, and return as a DataFrame.

        Args:
            channel_id (str): Discord channel ID.
            max_retries (int): Maximum retry attempts on network errors.

        Returns:
            pd.DataFrame: Columns → id, text, timestamp, author, source, links
        """
        url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
        headers = {
            "Authorization": self.auth_token,
            "User-Agent": "DiscordBot (https://github.com/your-repo, 1.0)"
        }

        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
        all_messages = []
        before_id = None
        retry_count = 0

        async with aiohttp.ClientSession() as session:
            while True:
                params = {"limit": 100}
                if before_id:
                    params["before"] = before_id

                try:
                    async with session.get(
                            url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        retry_count = 0  # reset on success

                        if resp.status == 200:
                            batch = await resp.json()
                        elif resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", 1))
                            self.logger.warning(f"⚠️ Rate limited. Waiting {retry_after} seconds...")
                            await asyncio.sleep(retry_after)
                            continue
                        elif resp.status == 401:
                            await send_error_to_telegram("❌ Invalid token or missing 'Read Message History' permission.")
                            raise PermissionError("❌ Invalid token or missing 'Read Message History' permission.")
                        elif resp.status == 403:
                            await send_error_to_telegram("❌ Bot lacks access to this channel.")
                            raise PermissionError("❌ Bot lacks access to this channel.")
                        elif resp.status == 404:
                            await send_error_to_telegram("❌ Channel ID not found.")
                            raise ValueError("❌ Channel ID not found.")
                        else:
                            error_text = await resp.text()
                            await send_error_to_telegram(f"⚠️ HTTP {resp.status}: {error_text[:200]}")
                            self.logger.error(f"⚠️ HTTP {resp.status}: {error_text[:200]}")
                            break

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    retry_count += 1
                    self.logger.warning(f"🔄 Network error (attempt {retry_count}/{max_retries}): {e}")
                    if retry_count >= max_retries:
                        await send_error_to_telegram("❌ Max retries reached. Stopping fetch.")
                        self.logger.error("❌ Max retries reached. Stopping fetch.")
                        break
                    await asyncio.sleep(2 ** retry_count)  # exponential backoff
                    continue

                if not batch:
                    break

                valid_messages = []
                for msg in batch:
                    try:
                        ts_str = msg["timestamp"].rstrip("Z")
                        msg_time = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
                    except (ValueError, KeyError, TypeError):
                        continue

                    if msg_time >= cutoff_time:
                        valid_messages.append(msg)
                    else:
                        break  # messages are in descending order

                if not valid_messages:
                    break

                all_messages.extend(valid_messages)

                if len(batch) == 100 and len(valid_messages) == 100:
                    before_id = valid_messages[-1]["id"]
                else:
                    break

        filtered_data = []

        for msg in all_messages:
            raw_content = msg.get("content", "") or ""

            if is_low_value_message(raw_content):
                continue

            text_clean = filter_text(raw_content)

            # Extract URLs from embeds
            links = []
            for embed in msg.get("embeds", []):
                url = embed.get("url")
                if url and isinstance(url, str):
                    url_clean = url.strip()
                    if url_clean:
                        links.append(url_clean)

            filtered_data.append({
                "id": msg["id"],
                "text": text_clean,
                "timestamp": msg["timestamp"],
                "author": f"{msg['author']['username']}#{msg['author']['id']}",
                # "source": ,
                "platform": "discord",
                "links": links
            })

        total_pulled = len(all_messages)
        total_kept = len(filtered_data)

        self.logger.info(f"📊 [Discord] Pulled: {total_pulled} | Kept: {total_kept}")
        df = pd.DataFrame(filtered_data)
        df.sort_values("timestamp").drop_duplicates(subset=["text"], keep="last")
        return df, ScrapeStats(channel_id=channel_id, platform="discord", pulled=total_pulled, kept=total_kept)
