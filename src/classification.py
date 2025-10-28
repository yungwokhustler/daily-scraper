import os
import asyncio
import json
import pandas as pd
from openai import AsyncOpenAI
from dotenv import load_dotenv

from src.logger import get_logger
from src.notification import send_error_to_telegram, send_notify_telegram

load_dotenv()


class LLMClassifier:
    """
    A classifier that uses DeepSeek LLM to categorize Web3/crypto messages.
    """

    def __init__(self):
        """
        Initialize the classifier with API credentials from .env.
        Only DeepSeek is supported.
        """
        self.api_key = os.getenv("DEEPSEEK_API_KEY")
        if not self.api_key:
            raise ValueError("DEEPSEEK_API_KEY must be set in .env")

        self.base_url = "https://api.deepseek.com"
        self.model = "deepseek-chat"
        self.logger = get_logger(self.__class__.__name__)

        # self.allowed_tags = [
        #     "news", "governance", "product", "feedback",
        #     "scam", "education", "security"
        # ]
        # {', '.join(self.allowed_tags)}

        self.system_prompt = self.build_system_prompt()

    def build_system_prompt(self) -> str:
        """Construct the system prompt for the LLM."""
        return f"""
You are a precise Web3/crypto content classifier. The user will provide a list of messages in JSON format.
Analyze ONLY "text" "links" field of each message.

Classify based on relevance to these **allowed tag topics ONLY**:

Definitions:
- **news**: Official announcements (listings, partnerships, hacks, audits).
- **governance**: DAO proposals, voting, protocol upgrades.
- **product**: dApps, NFTs, token launches, staking, bridges.
- **feedback**: User complaints, bug reports, UX suggestions.
- **scam**: Phishing, fake sites, rug pulls, impersonation.
- **education**: Explanations of crypto concepts (e.g., gas fee, zk-proof).
- **security**: Exploits, vulnerability reports, wallet safety tips.

Rules:
1. Assign **only tag topics from the allowed definitions**.
2. Include a tag **only if semantically relevant to Web3/crypto**.
3. Compute a **relevance score (0.0 to 1.0 as string) based on how clearly and directly the content relevance to the topics**.
4. Set "keep" = "true" if score >= "0.70", else "false".
5. Output **ONLY a valid JSON array** — no extra text.
6. Each object must have: "id", "source", "keep", "score", "tags".


This is a json classification task. Return clean json.

EXAMPLE INPUT:
[
  {{"id": 101, "text": "example text", "platform": "discord", "links": ["http://example.com"]}},
  {{"id": 102, "text": "example text", "platform": "telegram", "links": ["http://example2.com"]}}
]

EXAMPLE JSON OUTPUT:
[
  {{"id": 101, "platform": "discord", "keep": "true", "score": "0.92", "tags": ["governance", "product"]}},
  {{"id": 102, "platform": "telegram", "keep": "true", "score": "0.88", "tags": ["scam", "security"]}}
]
""".strip()

    async def _classify_batch(self, batch: list[dict]) -> list[dict]:
        """Classify a single batch of messages using DeepSeek API."""
        user_prompt = json.dumps(batch, ensure_ascii=False)
        client = AsyncOpenAI(api_key=self.api_key, base_url=self.base_url)

        for attempt in range(3):
            try:
                response = await client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    timeout=30
                )

                content = response.choices[0].message.content.strip()
                result = json.loads(content)
                if isinstance(result, list) and len(result) == len(batch):
                    return result
            except Exception as e:
                print(f"⚠️ Retry {attempt + 1}/3: {e}")
                await asyncio.sleep(2 ** attempt)
                # Fallback
            return [{
                "id": item["id"],
                "platform": item["platform"],
                "keep": "false",
                "score": "0.0",
                "tags": []
            } for item in batch]

    async def classify(self, df: pd.DataFrame, batch_size: int = 250, max_concurrent: int = 3) -> pd.DataFrame:
        """
        Classify a full DataFrame of messages using concurrent batch processing.

        Expected columns in df: "id", "platform", "text" (required), "links" (optional).
        """
        # Validasi kolom wajib
        required_cols = {"id", "platform", "text"}
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Buat salinan dan isi kolom opsional
        df_clean = df.copy()

        # Pastikan 'links' ada dan berupa list
        if "links" not in df_clean.columns:
            df_clean["links"] = [[] for _ in range(len(df_clean))]
        else:
            df_clean["links"] = df_clean["links"].apply(
                lambda x: x if isinstance(x, list) else []
            )

        # Hapus baris dengan text null/NaN dan pastikan text string
        df_clean = df_clean.dropna(subset=["text"]).copy()
        df_clean["text"] = df_clean["text"].astype(str)

        # Jika tidak ada data valid
        if df_clean.empty:
            self.logger.info("No valid messages to classify.")
            await send_error_to_telegram("❌ No valid messages to classify.")
            return pd.DataFrame(columns=["id", "platform", "keep", "score", "tags"])

        # Konversi ke list of dict untuk pengiriman ke LLM
        input_data = df_clean[["id", "text", "platform", "links"]].to_dict(orient="records")

        # Batch processing
        batches = [input_data[i:i + batch_size] for i in range(0, len(input_data), batch_size)]
        semaphore = asyncio.Semaphore(max_concurrent)
        results = []

        async def _process_batch(batch):
            async with semaphore:
                result = await self._classify_batch(batch)
                results.extend(result)
                self.logger.info(f"✅ Batch completed: {len(result)} items classified")

        tasks = [_process_batch(batch) for batch in batches]
        await asyncio.gather(*tasks)

        # Kembalikan sebagai DataFrame
        result_df = pd.DataFrame(results)
        self.logger.info(f"✅ Classification completed. Total: {len(result_df)} items.")
        return result_df
