"""Export utilities for saving full-document text outputs per PDF."""

from pathlib import Path
import gzip

import pandas as pd

from budget.config import FULLTEXT_DIR, FULLTEXT_EN_DIR
from budget.translation_utils import translate_to_english_glossary
from budget.utils import logger

def _safe_stem(value: str) -> str:
    """Create a filesystem-safe stem from arbitrary text."""
    clean = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)
    return clean[:120] if len(clean) > 120 else clean


def _build_full_text_content(file_df: pd.DataFrame, translate_to_english: bool = False) -> str:
    """Concatenate page texts into one document-level text block."""
    lines: list[str] = []
    sorted_df = file_df.sort_values("page_number")
    for row in sorted_df.itertuples(index=False):
        lines.append(f"=== Page {row.page_number} | method: {row.extraction_method} ===")
        page_text = row.text if isinstance(row.text, str) else ""
        if translate_to_english:
            page_text = translate_to_english_glossary(page_text)
        lines.append(page_text)
        lines.append("")
    return "\n".join(lines)


def _export_full_documents_to_dir(
    pages_df: pd.DataFrame,
    target_dir: Path,
    translate_to_english: bool = False,
    filename_suffix: str = "",
) -> pd.DataFrame:
    """Export one full text file per PDF to a target directory."""
    target_dir.mkdir(parents=True, exist_ok=True)

    if pages_df.empty:
        return pd.DataFrame(
            columns=[
                "file_id",
                "filepath",
                "txt_output_path",
                "docx_output_path",
                "docx_status",
                "total_pages",
                "total_chars",
            ]
        )

    records = []
    grouped = pages_df.groupby(["file_id", "filepath"], dropna=False)
    for (file_id, filepath), file_df in grouped:
        source_path = Path(str(filepath))
        base_name = _safe_stem(f"{file_id}_{source_path.stem}{filename_suffix}")
        txt_path = target_dir / f"{base_name}.txt.gz"
        # Word export disabled (not used downstream, saves space/time)
        docx_path = None

        try:
            content = _build_full_text_content(file_df, translate_to_english=translate_to_english)
            with gzip.open(txt_path, "wt", encoding="utf-8") as f:
                f.write(content)
            docx_status = "disabled"
            final_docx_path = ""

            records.append(
                {
                    "file_id": file_id,
                    "filepath": str(filepath),
                    "txt_output_path": str(txt_path),
                    "docx_output_path": final_docx_path,
                    "docx_status": docx_status,
                    "total_pages": int(file_df["page_number"].nunique()),
                    "total_chars": int(file_df["char_count"].fillna(0).sum()),
                }
            )
        except Exception as exc:
            logger.error("Full text export failed for %s: %s", filepath, exc)
            records.append(
                {
                    "file_id": file_id,
                    "filepath": str(filepath),
                    "txt_output_path": str(txt_path) if txt_path.exists() else "",
                    "docx_output_path": "",
                    "docx_status": f"error: {exc}",
                    "total_pages": int(file_df["page_number"].nunique()),
                    "total_chars": int(file_df["char_count"].fillna(0).sum()),
                }
            )

    return pd.DataFrame(records).sort_values("filepath").reset_index(drop=True)


def export_full_documents(pages_df: pd.DataFrame) -> pd.DataFrame:
    """Export full-document text in original language."""
    export_df = _export_full_documents_to_dir(
        pages_df,
        target_dir=FULLTEXT_DIR,
        translate_to_english=False,
    )
    logger.info("Full document exports created: %s", len(export_df))
    return export_df


def export_full_documents_english(pages_df: pd.DataFrame) -> pd.DataFrame:
    """Export full-document text translated to English with glossary replacement."""
    export_df = _export_full_documents_to_dir(
        pages_df,
        target_dir=FULLTEXT_EN_DIR,
        translate_to_english=True,
        filename_suffix="_en",
    )
    logger.info("Full document English exports created: %s", len(export_df))
    return export_df
