"""File inventory stage for recursively listing PDF files."""

from pathlib import Path

import pandas as pd

from budget.utils import (
    build_file_id,
    build_file_id_from_content_hash,
    compute_file_hash,
    infer_country_year,
    logger,
)


def build_file_inventory(pdf_root: Path) -> pd.DataFrame:
    """Scan a PDF folder recursively and return inventory metadata."""
    if not pdf_root.exists():
        logger.warning("PDF root does not exist: %s", pdf_root)
        return pd.DataFrame(
            columns=[
                "file_id",
                "filepath",
                "filename",
                "country_guess",
                "year_guess",
                "extension",
                "file_size",
                "content_hash",
            ]
        )

    records = []
    files = sorted([p for p in pdf_root.rglob("*") if p.is_file()])
    logger.info("Files discovered in %s: %s", pdf_root, len(files))

    for file_path in files:
        extension = file_path.suffix.lower()
        if extension != ".pdf":
            continue

        country_guess, year_guess = infer_country_year(file_path)
        try:
            content_hash = compute_file_hash(file_path)
        except Exception as exc:
            logger.warning("Could not hash %s: %s", file_path, exc)
            content_hash = ""
        record = {
            "file_id": build_file_id_from_content_hash(content_hash) if content_hash else build_file_id(file_path),
            "filepath": str(file_path),
            "filename": file_path.name,
            "country_guess": country_guess,
            "year_guess": year_guess,
            "extension": extension,
            "file_size": file_path.stat().st_size,
            "content_hash": content_hash,
        }
        records.append(record)

    inventory_df = pd.DataFrame(records)
    if not inventory_df.empty:
        inventory_df = inventory_df.sort_values(["country_guess", "year_guess", "filename"]).reset_index(drop=True)
    logger.info("PDF inventory rows: %s", len(inventory_df))
    return inventory_df
