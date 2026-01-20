"""
Migration script to move conversation history from JSON file to SQLite DB.
Usage: python scripts/migrate_conversations.py
"""

import json
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from config import PROJECT_ROOT
from utils.storage_backend import get_storage_backend

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration")

def migrate():
    json_path = PROJECT_ROOT / "logs" / "conversations.json"
    
    if not json_path.exists():
        logger.info(f"No legacy JSON file found at {json_path}. Nothing to migrate.")
        return

    logger.info(f"Found legacy JSON file at {json_path}. Loading...")
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read JSON file: {e}")
        return

    if not data:
        logger.info("JSON file is empty.")
        return

    storage = get_storage_backend()
    count = 0
    
    logger.info(f"Migrating {len(data)} conversations to SQLite...")
    
    for cid, (content, expires_at) in data.items():
        try:
            # We use the internal _get_conn or just set_with_ttl logic manually
            # But set_with_ttl calculates expiry from TTL.
            # We want to preserve exact expiry.
            
            # Direct SQL insert to preserve expires_at
            with storage._get_conn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO conversations (id, content, expires_at) VALUES (?, ?, ?)",
                    (cid, content, expires_at)
                )
                conn.commit()
            count += 1
        except Exception as e:
            logger.error(f"Failed to migrate conversation {cid}: {e}")

    logger.info(f"Successfully migrated {count} conversations.")
    
    # Rename old file
    backup_path = json_path.with_suffix(".json.bak")
    json_path.rename(backup_path)
    logger.info(f"Renamed legacy file to {backup_path}")

if __name__ == "__main__":
    migrate()
