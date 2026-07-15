#!/usr/bin/env python3
"""Safely normalize legacy processed-message keys without deleting records."""

import argparse
import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any


DEFAULT_MESSAGES_FILE = os.getenv("PROCESSED_MESSAGES_PATH", "processed_messages.json")


def _load(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("processed messages must contain a JSON object")
    if any(not isinstance(record, dict) for record in data.values()):
        raise ValueError("every processed message must be a JSON object")
    return data


def _unique_key(result: Dict[str, Any], preferred: str) -> str:
    if preferred not in result:
        return preferred
    suffix = 2
    while f"{preferred}#{suffix}" in result:
        suffix += 1
    return f"{preferred}#{suffix}"


def fix_processed_messages(messages_file: str = DEFAULT_MESSAGES_FILE) -> str:
    """Rewrite keys to chat_id:message_id while preserving every input record.

    Returns the backup path. The original is replaced atomically only after the
    replacement has been written and fsynced successfully.
    """
    path = Path(messages_file).expanduser()
    data = _load(path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup = path.with_name(f"{path.stem}_backup_{timestamp}{path.suffix}")
    shutil.copy2(path, backup)

    normalized: Dict[str, Any] = {}
    for old_key, record in data.items():
        chat_id = record.get("chat_id", 0)
        message_id = str(record.get("message_id", old_key))
        key = _unique_key(normalized, f"{chat_id}:{message_id}")
        normalized[key] = record

    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=str(path.parent),
            prefix=path.name + ".", suffix=".tmp", delete=False,
        ) as handle:
            temp_name = handle.name
            json.dump(normalized, handle, indent=2, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except Exception:
        if temp_name:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass
        # os.replace is atomic; if it somehow completed before a later error,
        # restore the byte-for-byte backup.
        shutil.copy2(backup, path)
        raise
    return str(backup)


def show_statistics(messages_file: str = DEFAULT_MESSAGES_FILE) -> Dict[str, int]:
    data = _load(Path(messages_file).expanduser())
    zero_chat = sum(record.get("chat_id", 0) == 0 for record in data.values())
    sent = sum(bool(record.get("sent_to_telegram", record.get("is_sent_to_telegram", False))) for record in data.values())
    stats = {"total": len(data), "chat_id_zero": zero_chat, "sent": sent}
    print(json.dumps(stats, ensure_ascii=False))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize processed-message identities safely")
    parser.add_argument("--messages", default=DEFAULT_MESSAGES_FILE, help="Legacy JSON path")
    parser.add_argument("--apply", action="store_true", help="Create backup and normalize keys")
    args = parser.parse_args()
    show_statistics(args.messages)
    if args.apply:
        backup = fix_processed_messages(args.messages)
        print(f"Normalized without deleting records. Backup: {backup}")
    else:
        print("Dry run only; pass --apply to modify the file.")


if __name__ == "__main__":
    main()
