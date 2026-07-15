"""Opt-in, integrity-checked application updater."""

import aiohttp
import hashlib
import logging
import os
import re
import shutil
import stat
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import quote


REPOSITORY = "voterol/ggsel_seller_helper"
BOT_DIR = Path(__file__).resolve().parent
MAX_ARCHIVE_BYTES = 100 * 1024 * 1024
MAX_EXTRACTED_BYTES = 250 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 10_000
PRESERVE_NAMES = {
    ".env",
    ".venv",
    "venv",
    "bot_lang.json",
    "orders.json",
    "topics.json",
    "processed_reviews.json",
    "processed_purchases.json",
    "processed_messages.json",
    "pending_topics.json",
    "autoresponder.json",
    "autoresponder_config.json",
    "ggsel_bot.log",
}
_VERSION_RE = re.compile(r'__version__\s*=\s*["\']([^"\']+)["\']')
_SAFE_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def get_current_version() -> str:
    """Read the installed package version."""
    try:
        match = _VERSION_RE.search((BOT_DIR / "__init__.py").read_text(encoding="utf-8"))
        if match:
            return match.group(1)
    except (OSError, UnicodeError) as exc:
        logging.error("Failed to read installed version: %s", exc)
    return "0.0.0"


def _validate_update_parameters(version: Optional[str], sha256: Optional[str]) -> Tuple[str, str]:
    version = (version or "").strip()
    sha256 = (sha256 or "").strip().lower()
    if not _SAFE_VERSION_RE.fullmatch(version):
        raise ValueError("UPDATE_VERSION must be a simple release tag (letters, digits, '.', '_' or '-')")
    if not _SHA256_RE.fullmatch(sha256):
        raise ValueError("UPDATE_SHA256 must contain exactly 64 hexadecimal characters")
    return version, sha256


def _archive_url(version: str) -> str:
    # A versioned release URL replaces the former mutable main.zip URL.
    return f"https://github.com/{REPOSITORY}/archive/refs/tags/{quote(version, safe='')}.zip"


async def get_remote_version(version: Optional[str] = None) -> Optional[str]:
    """Read __version__ from an explicitly pinned release tag."""
    if not version or not _SAFE_VERSION_RE.fullmatch(version):
        return None
    url = (
        f"https://raw.githubusercontent.com/{REPOSITORY}/"
        f"refs/tags/{quote(version, safe='')}/__init__.py"
    )
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, allow_redirects=True) as response:
                if response.status != 200:
                    return None
                match = _VERSION_RE.search(await response.text())
                return match.group(1) if match else None
    except (aiohttp.ClientError, UnicodeError, TimeoutError) as exc:
        logging.error("Failed to read release version: %s", exc)
        return None


def _safe_extract(archive: zipfile.ZipFile, destination: Path) -> None:
    """Extract regular files/directories while rejecting traversal and links."""
    members = archive.infolist()
    if len(members) > MAX_ARCHIVE_MEMBERS:
        raise ValueError("Update archive contains too many entries")
    total_size = 0
    destination = destination.resolve()
    for member in members:
        name = member.filename.replace("\\", "/")
        path = Path(name)
        mode = member.external_attr >> 16
        file_type = stat.S_IFMT(mode)
        if (
            not name
            or name.startswith("/")
            or path.is_absolute()
            or ".." in path.parts
            # ZIPs made on DOS commonly have no Unix file type. When one is
            # present, accept regular files/directories only (no links/devices).
            or (file_type and file_type not in (stat.S_IFREG, stat.S_IFDIR))
        ):
            raise ValueError(f"Unsafe archive entry: {member.filename!r}")
        total_size += member.file_size
        if total_size > MAX_EXTRACTED_BYTES:
            raise ValueError("Update archive is too large when extracted")
        target = (destination / path).resolve()
        if target != destination and destination not in target.parents:
            raise ValueError(f"Archive entry escapes destination: {member.filename!r}")

    for member in members:
        target = destination / Path(member.filename.replace("\\", "/"))
        if member.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        with archive.open(member) as source, target.open("wb") as output:
            shutil.copyfileobj(source, output)


def _find_source(extract_dir: Path) -> Path:
    roots = [item for item in extract_dir.iterdir() if item.is_dir()]
    if len(roots) != 1:
        raise ValueError("Update archive must contain exactly one root directory")
    source = roots[0]
    nested = source / "ggsel_bot"
    if nested.is_dir():
        source = nested
    if not (source / "main.py").is_file() or not (source / "__init__.py").is_file():
        raise ValueError("Update archive does not contain the expected application package")
    return source


def _database_preserve_paths(database_path: Optional[str]) -> set[Path]:
    """Return in-install DB paths; external DBs are deliberately left alone."""
    value = database_path if database_path is not None else os.getenv("DATABASE_PATH", "ggsel_bot.db")
    if not value or not value.strip() or "\x00" in value:
        raise ValueError("DATABASE_PATH must be a non-empty filesystem path")

    configured = Path(value.strip()).expanduser()
    is_absolute = configured.is_absolute()
    candidate = configured if is_absolute else BOT_DIR / configured
    # abspath normalizes '..' without following the final path. Parent symlinks
    # are rejected below because moving through one could modify external data.
    candidate = Path(os.path.abspath(candidate))
    bot_dir = Path(os.path.abspath(BOT_DIR))
    backup = bot_dir.with_name(bot_dir.name + ".update-backup")
    old_backup = bot_dir.with_name(bot_dir.name + ".update-backup.old")
    if candidate == backup or backup in candidate.parents or candidate == old_backup or old_backup in candidate.parents:
        raise ValueError("DATABASE_PATH cannot be inside an updater backup directory")
    if candidate != bot_dir and bot_dir not in candidate.parents:
        if not is_absolute:
            raise ValueError("A relative DATABASE_PATH cannot escape the application directory")
        return set()
    if candidate == bot_dir:
        raise ValueError("DATABASE_PATH cannot be the application directory")

    relative = candidate.relative_to(bot_dir)
    current = bot_dir
    for part in relative.parts[:-1]:
        current /= part
        if current.is_symlink():
            raise ValueError("DATABASE_PATH cannot traverse an in-tree symbolic-link directory")
    if candidate.is_symlink():
        raise ValueError("DATABASE_PATH cannot be an in-tree symbolic link")
    # SQLite may have live durability sidecars. Move them with the database if
    # present rather than taking a non-atomic copy while the service is running.
    return {
        relative,
        Path(str(relative) + "-wal"),
        Path(str(relative) + "-shm"),
        Path(str(relative) + "-journal"),
    }


def _preserved_paths(database_path: Optional[str]) -> list[Path]:
    paths = {Path(name) for name in PRESERVE_NAMES}
    paths.update(_database_preserve_paths(database_path))
    # An ancestor preservation already includes descendants. Keeping a minimal
    # list also prevents a nested path from being moved after its parent.
    return sorted(
        (path for path in paths if not any(parent in paths for parent in path.parents)),
        key=lambda path: (len(path.parts), str(path)),
    )


def _remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def _install_staged(source: Path, work_dir: Path, database_path: Optional[str] = None) -> None:
    """Build a stage, then swap directories; restore the backup on swap failure."""
    stage = work_dir / "stage"
    shutil.copytree(source, stage, symlinks=False)
    preserve_paths = _preserved_paths(database_path)

    backup = BOT_DIR.with_name(BOT_DIR.name + ".update-backup")
    old_backup = BOT_DIR.with_name(BOT_DIR.name + ".update-backup.old")
    if old_backup.exists():
        shutil.rmtree(old_backup)
    if backup.exists():
        os.replace(backup, old_backup)

    moved_paths = []
    os.replace(BOT_DIR, backup)
    try:
        for relative in preserve_paths:
            installed = backup / relative
            if not installed.exists() and not installed.is_symlink():
                continue
            target = stage / relative
            _remove_path(target)
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(installed, target)
            moved_paths.append(relative)
        os.replace(stage, BOT_DIR)
    except BaseException:
        for relative in reversed(moved_paths):
            staged = stage / relative
            target = backup / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, target)
        os.replace(backup, BOT_DIR)
        raise
    finally:
        if old_backup.exists():
            shutil.rmtree(old_backup, ignore_errors=True)


async def download_and_extract_update(
    version: str, expected_sha256: str, database_path: Optional[str] = None
) -> bool:
    """Download, verify, safely stage and install one pinned release."""
    try:
        version, expected_sha256 = _validate_update_parameters(version, expected_sha256)
        parent = BOT_DIR.parent
        with tempfile.TemporaryDirectory(prefix=".ggsel-update-", dir=parent) as temp_name:
            work_dir = Path(temp_name)
            archive_path = work_dir / "update.zip"
            digest = hashlib.sha256()
            downloaded = 0
            timeout = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(_archive_url(version), allow_redirects=True) as response:
                    if response.status != 200:
                        raise RuntimeError(f"Update download returned HTTP {response.status}")
                    with archive_path.open("wb") as output:
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            downloaded += len(chunk)
                            if downloaded > MAX_ARCHIVE_BYTES:
                                raise ValueError("Update archive exceeds the download limit")
                            digest.update(chunk)
                            output.write(chunk)
            if digest.hexdigest() != expected_sha256:
                raise ValueError("Update archive SHA-256 does not match UPDATE_SHA256")

            extract_dir = work_dir / "extracted"
            extract_dir.mkdir()
            with zipfile.ZipFile(archive_path) as archive:
                _safe_extract(archive, extract_dir)
            source = _find_source(extract_dir)
            match = _VERSION_RE.search((source / "__init__.py").read_text(encoding="utf-8"))
            if not match or match.group(1) != version:
                raise ValueError("Release package version does not match UPDATE_VERSION")
            _install_staged(source, work_dir, database_path)
        logging.info("Installed verified update %s; backup retained next to application", version)
        return True
    except (aiohttp.ClientError, OSError, ValueError, RuntimeError, zipfile.BadZipFile) as exc:
        logging.error("Update failed: %s", exc)
        return False


async def check_and_update(
    auto_update_enabled: bool = False,
    target_version: Optional[str] = None,
    expected_sha256: Optional[str] = None,
    database_path: Optional[str] = None,
) -> Tuple[bool, str]:
    """Install only an explicitly enabled, pinned and verified release."""
    if not auto_update_enabled:
        return False, "Automatic updates are disabled"
    try:
        target_version, expected_sha256 = _validate_update_parameters(
            target_version, expected_sha256
        )
    except ValueError as exc:
        return False, f"Unsafe update configuration: {exc}"
    current = get_current_version()
    if current == target_version:
        return False, f"Version is current: {current}"
    if await download_and_extract_update(target_version, expected_sha256, database_path):
        return True, f"Updated: {current} -> {target_version}"
    return False, "Update installation failed"


async def check_update_available(
    auto_update_enabled: bool = False,
    target_version: Optional[str] = None,
    expected_sha256: Optional[str] = None,
) -> str:
    """Check a pinned tag without downloading, extracting, or installing it.

    This is the only operation intended for the periodic task. Keeping it
    separate from ``check_and_update`` makes the live-service contract
    explicit: a periodic check can notify, but can never swap the install.
    """
    if not auto_update_enabled:
        return "Automatic updates are disabled"
    try:
        target_version, _ = _validate_update_parameters(target_version, expected_sha256)
    except ValueError as exc:
        return f"Unsafe update configuration: {exc}"
    current = get_current_version()
    if current == target_version:
        return f"Version is current: {current}"
    remote = await get_remote_version(target_version)
    if remote != target_version:
        return f"Pinned release {target_version} is unavailable or has an unexpected version"
    return f"Pinned update is available: {current} -> {target_version}; it will be downloaded and verified at next startup"


def restart_bot() -> None:
    logging.info("Restarting bot...")
    sys.exit(1)
