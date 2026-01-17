
import json
import time

from utils.storage_backend import InMemoryStorage


def test_storage_persistence(tmp_path):
    # Setup storage with a temporary directory for tests
    # We need to monkeypatch the storage dir
    storage = InMemoryStorage()

    # Use a specific test file
    test_file = tmp_path / "test_conversations.json"
    storage._persistence_file = test_file

    # 1. Store some data
    key = "test_thread_id"
    value = "test_conversation_data"
    ttl = 100
    storage.set_with_ttl(key, ttl, value)

    # Verify it was saved to file
    assert test_file.exists()
    with open(test_file) as f:
        data = json.load(f)
        assert key in data
        assert data[key][0] == value

    # 2. Create a new storage instance and load from the same file
    new_storage = InMemoryStorage()
    new_storage._persistence_file = test_file
    new_storage._load_from_disk()

    # Verify data was reloaded
    assert new_storage.get(key) == value

def test_storage_expiration_on_load(tmp_path):
    test_file = tmp_path / "expired_conversations.json"

    # Create a file with an expired entry
    expired_time = time.time() - 10
    valid_time = time.time() + 100

    data = {
        "expired_key": ["expired_value", expired_time],
        "valid_key": ["valid_value", valid_time]
    }

    with open(test_file, "w") as f:
        json.dump(data, f)

    # Load into storage
    storage = InMemoryStorage()
    storage._persistence_file = test_file
    storage._load_from_disk()

    # Verify only valid key exists
    assert storage.get("valid_key") == "valid_value"
    assert storage.get("expired_key") is None
