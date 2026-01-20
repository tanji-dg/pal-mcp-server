"""
Tests for SQLite-based storage persistence and concurrency.
"""

import threading
import time
import sqlite3
import pytest
from utils.storage_backend import SQLiteStorage

@pytest.fixture
def temp_storage(tmp_path):
    """Fixture to provide a SQLiteStorage instance using a temp file."""
    db_path = tmp_path / "test_conversations.db"
    
    # Initialize storage with custom path
    storage = SQLiteStorage()
    storage._db_path = db_path
    storage._init_db() # Re-init with new path
    
    yield storage
    
    storage.shutdown()

def test_sqlite_persistence(tmp_path):
    """Test that data persists across instance recreations."""
    db_path = tmp_path / "test_persist.db"
    
    # 1. Store data
    storage1 = SQLiteStorage()
    storage1._db_path = db_path
    storage1._init_db()
    
    storage1.set_with_ttl("key1", 3600, "data1")
    storage1.shutdown()
    
    # 2. Re-open same DB
    storage2 = SQLiteStorage()
    storage2._db_path = db_path
    # No need to explicitly init if file exists, but good practice in test to ensure connection points there
    storage2._init_db() 
    
    assert storage2.get("key1") == "data1"
    storage2.shutdown()

def test_sqlite_concurrency(tmp_path):
    """Test concurrent writes from multiple threads (simulating processes)."""
    db_path = tmp_path / "test_concurrent.db"
    
    def writer_task(key_prefix, count):
        # Create separate instance per thread to simulate processes sharing DB file
        storage = SQLiteStorage()
        storage._db_path = db_path
        # Allow implicit init or rely on first creator. 
        # SQLite handles concurrent connection creation fine.
        
        for i in range(count):
            storage.set_with_ttl(f"{key_prefix}_{i}", 3600, f"value_{i}")
        
        storage.shutdown()

    # Ensure DB exists first
    init_store = SQLiteStorage()
    init_store._db_path = db_path
    init_store._init_db()
    init_store.shutdown()

    threads = []
    num_threads = 5
    items_per_thread = 20
    
    for i in range(num_threads):
        t = threading.Thread(target=writer_task, args=(f"thread_{i}", items_per_thread))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    # Verify all data is present
    verify_store = SQLiteStorage()
    verify_store._db_path = db_path
    
    count = 0
    for i in range(num_threads):
        for j in range(items_per_thread):
            val = verify_store.get(f"thread_{i}_{j}")
            assert val == f"value_{j}"
            count += 1
            
    assert count == num_threads * items_per_thread
    verify_store.shutdown()

def test_sqlite_ttl_expiration(temp_storage):
    """Test that expired keys are not returned and are cleaned up."""
    # Set with short TTL
    temp_storage.set_with_ttl("short_lived", 0.1, "temp_data")
    assert temp_storage.get("short_lived") == "temp_data"
    
    # Wait for expiration
    time.sleep(0.2)
    
    # Should return None and lazily delete
    assert temp_storage.get("short_lived") is None
    
    # Verify deletion in DB
    with temp_storage._get_conn() as conn:
        cursor = conn.execute("SELECT count(*) FROM conversations WHERE id = 'short_lived'")
        assert cursor.fetchone()[0] == 0

def test_sqlite_cleanup_logic(temp_storage):
    """Test cleanup logic explicitly."""
    # Insert expired data directly to avoid waiting
    with temp_storage._get_conn() as conn:
        conn.execute(
            "INSERT INTO conversations (id, content, expires_at) VALUES (?, ?, ?)",
            ("expire_soon", "gone", time.time() - 10)
        )
        conn.commit()
    
    # Manually trigger cleanup
    temp_storage._cleanup_expired()
    
    with temp_storage._get_conn() as conn:
        cursor = conn.execute("SELECT count(*) FROM conversations WHERE id = 'expire_soon'")
        assert cursor.fetchone()[0] == 0