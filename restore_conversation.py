import json
import logging
import sqlite3
import re
import sys
import os
from datetime import datetime, timezone
from typing import Optional, List, Any, Dict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = "logs/conversations.db"

def parse_log_file(target_thread_id: str, log_file: str):
    events = []
    
    if not os.path.exists(log_file):
        logger.error(f"Log file not found: {log_file}")
        return []

    with open(log_file, 'r') as f:
        for line in f:
            if target_thread_id not in line:
                continue
                
            # Extract JSON payload from TOOL_LOG event
            # Format: ... Publisher: Sent TOOL_LOG event: {...}
            match = re.search(r'Publisher: Sent TOOL_LOG event: ({.*})', line)
            if match:
                try:
                    event_json_str = match.group(1)
                    event = json.loads(event_json_str)
                    
                    if event.get("session_id") == f"thread:{target_thread_id}":
                        log_data_str = event.get("log_data")
                        if log_data_str:
                            log_data = json.loads(log_data_str)
                            events.append(log_data)
                except Exception as e:
                    # Some lines might be truncated or malformed JSON
                    continue

    logger.info(f"Found {len(events)} events for thread {target_thread_id}")
    return events

def extract_content_text(content_blocks):
    if isinstance(content_blocks, str):
        return content_blocks
        
    text_parts = []
    for block in content_blocks:
        if isinstance(block, dict):
            if block.get("type") == "text":
                text_parts.append(block.get("text", ""))
            elif block.get("type") == "tool_use":
                tool_name = block.get("name")
                tool_input = block.get("input")
                text_parts.append(f"[Tool Use: {tool_name} {json.dumps(tool_input)}]")
            elif block.get("type") == "tool_result":
                 tool_content = block.get("content")
                 text_parts.append(f"[Tool Result: {tool_content}]")
        elif isinstance(block, str):
            text_parts.append(block)
            
    return "\n".join(text_parts)

def reconstruct_thread(target_thread_id: str, events: List[Dict]):
    turns = []
    
    for event in events:
        msg_type = event.get("type")
        
        if msg_type in ["assistant", "user"]:
            message = event.get("message", {})
            role = message.get("role")
            content_raw = message.get("content")
            
            if not role or not content_raw:
                continue
                
            content_text = extract_content_text(content_raw)
            
            turn = {
                "role": role,
                "content": content_text,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "files": [], 
                "images": [],
                "tool_name": "clink",
                "model_provider": "google",
                "model_name": message.get("model") or "gemini-3-pro-high",
                "model_metadata": {}
            }
            
            turns.append(turn)
            
    logger.info(f"Reconstructed {len(turns)} turns")
    
    thread_context = {
        "thread_id": target_thread_id,
        "parent_thread_id": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "last_updated_at": datetime.now(timezone.utc).isoformat(),
        "tool_name": "clink",
        "turns": turns,
        "initial_context": {
            "prompt": "Restored from logs",
            "working_directory": os.getcwd()
        }
    }
    
    return thread_context

def save_to_db(target_thread_id: str, thread_context: Dict):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                expires_at REAL NOT NULL
            )
        """)
        
        expires_at = datetime.now(timezone.utc).timestamp() + 86400
        json_data = json.dumps(thread_context)
        
        cursor.execute(
            "INSERT OR REPLACE INTO conversations (id, content, expires_at) VALUES (?, ?, ?)",
            (f"thread:{target_thread_id}", json_data, expires_at)
        )
        
        conn.commit()
        conn.close()
        logger.info(f"Successfully saved thread {target_thread_id} to database")
        return True
    except Exception as e:
        logger.error(f"Database error: {e}")
        return False

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 restore_conversation.py <thread_id> <log_file>")
        print("Example: python3 restore_conversation.py a5625cb8-9c0f-4f6a-929c-882eeba372ce logs/mcp_server_317187.log")
        return

    thread_id = sys.argv[1]
    log_file = sys.argv[2]

    logger.info(f"Starting restoration for thread {thread_id} from {log_file}...")
    events = parse_log_file(thread_id, log_file)
    if not events:
        logger.error("No events found or log file inaccessible. Aborting.")
        return
        
    thread_context = reconstruct_thread(thread_id, events)
    save_to_db(thread_id, thread_context)
    logger.info("Restoration complete.")

if __name__ == "__main__":
    main()
