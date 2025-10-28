import os
from datetime import datetime, timedelta, timezone
from getpass import getpass

import pandas as pd
from telethon import TelegramClient
from telethon.tl.types import PeerUser, MessageEntityTextUrl
from dotenv import load_dotenv

from src.logger import get_logger
from src.types import ScrapeStats
from src.notification import send_error_to_telegram
from src.normalization import filter_text, is_low_value_message

load_dotenv()


class TelegramScraper:
    def __init__(self):
        """
        Initialize the Telegram scraper.
        All credentials are loaded from the .env file.
        """
        self.api_id = int(os.getenv("TELEGRAM_API_ID"))
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        self.session_name = os.getenv("TELEGRAM_SESSION_NAME")
        self.client: TelegramClient | None = None
        self.logger = get_logger(self.__class__.__name__)

    async def login(self) -> TelegramClient:
        """
        Log in to Telegram using an existing session or interactive authentication.
        Returns an authorized and connected TelegramClient instance.
        """
        self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        await self.client.connect()
        self.logger.info("🔌 Connected to Telegram.")

        if not await self.client.is_user_authorized():
            self.logger.warning("🔓 No valid session found. Starting interactive login...")

            phone = getpass("Enter your phone number (e.g., +628123xxxxxxx): ")
            await self.client.send_code_request(phone)

            code = getpass("Enter the verification code you received in Telegram: ")
            await self.client.sign_in(phone, code)

            self.logger.info("✅ Login successful! Session saved locally.")
        else:
            self.logger.info("✅ Valid session found. Automatic login successful.")

        return self.client

    async def ensure_connected(self) -> TelegramClient:
        """
        Ensure the client is connected and authorized.
        If not, perform login automatically.
        """
        if self.client is None:
            self.client = await self.login()
        else:
            if not self.client.is_connected():
                await self.client.connect()
            if not await self.client.is_user_authorized():
                self.client = await self.login()
        return self.client

    async def scrape_24h_to_df_telegram(self, group_id: str) -> tuple[pd.DataFrame, ScrapeStats]:
        """
        Scrape messages from the last 24 hours in a Telegram group/channel and return as a pandas DataFrame.

        Args:
            group_id (str): Group/channel username or invite link (e.g., 'https://t.me/JKT48Live').

        Returns:
            pd.DataFrame: Cleaned and filtered messages with columns:
                ['id', 'text', 'timestamp', 'author', 'source', 'links']
        """
        await self.ensure_connected()
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(hours=24)

        records = []
        total_pulled = 0
        try:
            entity = await self.client.get_entity(group_id)

            async for message in self.client.iter_messages(entity, limit=None):
                msg_time = message.date
                if msg_time.tzinfo is None:
                    msg_time = msg_time.replace(tzinfo=timezone.utc)

                if msg_time < cutoff:
                    self.logger.info("⏹️  Messages older than 24h — stopping iteration.")
                    break

                if not message.text:
                    continue

                total_pulled += 1

                if is_low_value_message(message.message):
                    continue

                # Extract author
                author = None
                if message.from_id and isinstance(message.from_id, PeerUser):
                    author = message.from_id.user_id
                elif hasattr(message.peer_id, 'channel_id'):
                    author = f"admin#{message.peer_id.channel_id}"

                # Extract URLs from Telegram entities (accurate)
                normalized_urls = []
                if message.entities:
                    for ent in message.entities:
                        if isinstance(ent, MessageEntityTextUrl):
                            url = ent.url.strip()
                            normalized_urls.append(url)

                # Clean text: remove URLs, emojis, normalize whitespace
                clean_text = filter_text(message.message)

                records.append({
                    "id": message.id,
                    "text": clean_text,
                    "timestamp": message.date,
                    "author": author,
                    # "source": "telegram",
                    "platform": "telegram",
                    "links": normalized_urls
                })

            total_pulled = len(records)
            # Convert to DataFrame
            if not records:
                df = pd.DataFrame(columns=["id", "text", "timestamp", "author", "platform", "links"])
            else:
                df = pd.DataFrame(records)
                df = df.sort_values("timestamp").drop_duplicates(subset=["text"], keep="last")

            total_kept = len(df)
            self.logger.info(f"✅ Successfully scraped {len(df)} messages after filtering.")
            self.logger.info(f"📊 [Telegram] Pulled: {total_pulled} | Kept: {total_kept}")

            return df, ScrapeStats(channel_id=group_id, platform="telegram", pulled=total_pulled, kept=total_kept)


        except Exception as e:
            await send_error_to_telegram(f"❌ Error during scraping: {e}")
            self.logger.error(f"❌ Error during scraping: {e}")
            return (pd.DataFrame(columns=["id", "text", "timestamp", "author", "platform", "links"]),
                    ScrapeStats(channel_id=group_id, platform="telegram", pulled=0, kept=0))

    async def close(self):
        """Close the Telegram client connection if active."""
        if self.client and self.client.is_connected():
            await self.client.disconnect()
            self.logger.info("🔌 Telegram connection closed.")
