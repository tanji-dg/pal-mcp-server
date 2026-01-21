"""
SQLite storage backend for conversation threads

Replaces the in-memory/JSON-file storage with a robust SQLite database to handle
concurrent access from multiple MCP server processes safely.

Key Features:
- Process-safe concurrent access (SQLite handling locking)
- Persistent storage in logs/conversations.db
- Automatic cleanup of expired entries
"""

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

from config import PROJECT_ROOT
from utils.env import get_env

logger = logging.getLogger(__name__)


class SQLiteStorage:
    """Thread-safe and process-safe storage using SQLite"""

    def __init__(self):
        self._storage_dir = PROJECT_ROOT / "logs"
        self._storage_dir.mkdir(exist_ok=True)
        self._db_path = self._storage_dir / "conversations.db"
        
        self._init_db()

        # Cleanup settings
        timeout_hours = int(get_env("CONVERSATION_TIMEOUT_HOURS", "24") or "24")
        self._cleanup_interval = (timeout_hours * 3600) // 10
        self._cleanup_interval = max(300, self._cleanup_interval)
        self._shutdown = False

        # Start background cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True)
        self._cleanup_thread.start()

        logger.info(f"SQLite storage initialized at {self._db_path}")

    def _get_conn(self):
        """Get a new database connection"""
        return sqlite3.connect(str(self._db_path), timeout=10.0)

    def _init_db(self):
        """Initialize database schema"""
        try:
            with self._get_conn() as conn:
                # Optimize for concurrency
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS conversations (
                        id TEXT PRIMARY KEY,
                        content TEXT NOT NULL,
                        expires_at REAL NOT NULL
                    )
                """)
                # Create index for cleanup optimization
                conn.execute("CREATE INDEX IF NOT EXISTS idx_expires_at ON conversations(expires_at)")
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to initialize SQLite DB: {e}")

    def set_with_ttl(self, key: str, ttl_seconds: int, value: str) -> None:
        """Store value with expiration time"""
        expires_at = time.time() + ttl_seconds
        try:
            with self._get_conn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO conversations (id, content, expires_at) VALUES (?, ?, ?)",
                    (key, value, expires_at)
                )
                conn.commit()
            logger.debug(f"Stored key {key} with TTL {ttl_seconds}s")
        except sqlite3.Error as e:
            logger.error(f"Failed to set key {key}: {e}")

    def get(self, key: str) -> Optional[str]:
        """Retrieve value if not expired"""
        try:
            with self._get_conn() as conn:
                cursor = conn.execute(
                    "SELECT content, expires_at FROM conversations WHERE id = ?", 
                    (key,)
                )
                row = cursor.fetchone()
                
                if row:
                    content, expires_at = row
                    if time.time() < expires_at:
                        logger.debug(f"Retrieved key {key}")
                        return content
                    else:
                        # Lazy delete on read if expired
                        conn.execute("DELETE FROM conversations WHERE id = ?", (key,))
                        conn.commit()
                        logger.debug(f"Key {key} expired (on read)")
        except sqlite3.Error as e:
            logger.error(f"Failed to get key {key}: {e}")
        return None

    def list_all(self, include_expired: bool = False) -> dict[str, tuple[str, float]]:
        """List all conversations. Returns dict {id: (content, expires_at)}"""
        current_time = time.time()
        result = {}
        try:
            with self._get_conn() as conn:
                if include_expired:
                    query = "SELECT id, content, expires_at FROM conversations"
                    params = ()
                else:
                    query = "SELECT id, content, expires_at FROM conversations WHERE expires_at > ?"
                    params = (current_time,)
                
                cursor = conn.execute(query, params)
                for row in cursor.fetchall():
                    result[row[0]] = (row[1], row[2])
        except sqlite3.Error as e:
            logger.error(f"Failed to list conversations: {e}")
        return result

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        """Redis-compatible setex method"""
        self.set_with_ttl(key, ttl_seconds, value)

    def _cleanup_worker(self):
        """Background thread that periodically cleans up expired entries"""
        while not self._shutdown:
            time.sleep(self._cleanup_interval)
            self._cleanup_expired()

    def _cleanup_expired(self):
        """Remove all expired entries"""
        current_time = time.time()
        try:
            with self._get_conn() as conn:
                cursor = conn.execute(
                    "DELETE FROM conversations WHERE expires_at < ?", 
                    (current_time,)
                )
                count = cursor.rowcount
                if count > 0:
                    conn.commit()
                    logger.debug(f"Cleaned up {count} expired conversation threads")
        except sqlite3.Error as e:
            logger.error(f"Cleanup failed: {e}")

    def flushall(self) -> None:
        """Clear all conversation data from the database"""
        try:
            with self._get_conn() as conn:
                conn.execute("DELETE FROM conversations")
                conn.commit()
            logger.info("Cleared all conversation data (flushall)")
        except sqlite3.Error as e:
            logger.error(f"Failed to clear conversation data: {e}")

    def shutdown(self):
        """Graceful shutdown of background thread"""
        self._shutdown = True
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=1)


# Global singleton instance
_storage_instance = None
_storage_lock = threading.Lock()


def get_storage_backend() -> SQLiteStorage:
    """Get the global storage instance (singleton pattern)"""
    global _storage_instance
    if _storage_instance is None:
        with _storage_lock:
            if _storage_instance is None:
                _storage_instance = SQLiteStorage()
    return _storage_instance
