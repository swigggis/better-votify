#!/usr/bin/env python3
"""
Database cleanup script - removes entries for files that no longer exist on disk.
"""

import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def cleanup_database(db_path: Path, dry_run: bool = False) -> None:
    """
    Check all database entries and remove those where the file no longer exists.
    
    Args:
        db_path: Path to the SQLite database file
        dry_run: If True, only report missing files without deleting them
    """
    # Import here to use the existing Database class
    from database import Database

    if not db_path.exists():
        logger.error(f"Database file not found: {db_path}")
        sys.exit(1)

    logger.info(f"Opening database: {db_path}")
    logger.info(f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE (entries will be deleted)'}")

    with Database(db_path) as db:
        # --- Fetch all entries ---
        db.cursor.execute("SELECT id, path FROM media")
        all_entries: list[tuple[str, str]] = db.cursor.fetchall()

        stats = db.get_stats()
        total = stats["total_entries"]
        logger.info(f"Total entries in database: {total}")

        if total == 0:
            logger.info("Database is empty, nothing to clean up.")
            return

        # --- Check each entry ---
        missing_ids: list[str] = []
        missing_paths: list[str] = []

        for media_id, path in all_entries:
            if not Path(path).exists():
                missing_ids.append(media_id)
                missing_paths.append(path)

        found_count = total - len(missing_ids)
        logger.info(f"Files found on disk:    {found_count}/{total}")
        logger.info(f"Files missing on disk:  {len(missing_ids)}/{total}")

        if not missing_ids:
            logger.info("No missing files found. Database is clean.")
            return

        # --- Report missing entries ---
        logger.info("Missing files:")
        for media_id, path in zip(missing_ids, missing_paths):
            logger.info(f"  [{media_id}] {path}")

        # --- Remove missing entries (unless dry run) ---
        if dry_run:
            logger.info(f"DRY RUN: Would have removed {len(missing_ids)} entries.")
        else:
            logger.info(f"Removing {len(missing_ids)} entries from database...")
            db.remove_batch(missing_ids)
            logger.info("Cleanup complete.")

            # --- Final stats ---
            stats_after = db.get_stats()
            logger.info(f"Entries remaining in database: {stats_after['total_entries']}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Remove database entries for media files that no longer exist on disk."
    )
    parser.add_argument(
        "db_path",
        type=Path,
        help="Path to the SQLite database file (e.g. media.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report missing entries without deleting them",
    )
    args = parser.parse_args()

    cleanup_database(db_path=args.db_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
