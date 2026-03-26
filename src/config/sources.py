import csv
import os
from typing import List, Dict, Any

from src.logger import get_logger

logger = get_logger(__name__)

_SOURCES_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "sources_rows.csv")


def get_all_sources() -> List[Dict[str, Any]]:
    """Load sources from the CSV file bundled in the repo."""
    path = os.path.normpath(_SOURCES_CSV)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        sources = [
            {
                "id": int(row["id"]),
                "channel_id": row["channel_id"],
                "platform": row["platform"],
                "channel_name": row["channel_name"],
            }
            for row in reader
        ]
    logger.info(f"✅ Loaded {len(sources)} sources from {os.path.basename(path)}")
    return sources
