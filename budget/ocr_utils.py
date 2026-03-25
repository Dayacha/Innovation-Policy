"""OCR helper utilities with graceful fallback when OCR is unavailable."""

import io
from typing import Optional

import fitz
from PIL import Image

from budget.config import OCR_ZOOM, TESSERACT_LANGS
from budget.utils import logger

try:
    import pytesseract
except Exception:  # pragma: no cover - import failure is handled at runtime
    pytesseract = None


def is_ocr_available() -> bool:
    """Check if pytesseract and the Tesseract binary are available."""
    if pytesseract is None:
        return False
    try:
        _ = pytesseract.get_tesseract_version()
        return True
    except Exception:
        return False


def run_ocr_on_page(page: fitz.Page, lang: Optional[str] = None) -> str:
    """Run OCR on a single PDF page and return extracted text."""
    if not is_ocr_available():
        return ""

    lang_code = lang if lang else TESSERACT_LANGS
    try:
        pixmap = page.get_pixmap(matrix=fitz.Matrix(OCR_ZOOM, OCR_ZOOM), alpha=False)
        image = Image.open(io.BytesIO(pixmap.tobytes("png")))
        text = pytesseract.image_to_string(image, lang=lang_code)
        return text or ""
    except Exception as exc:
        logger.warning("OCR failed for page %s: %s", page.number + 1, exc)
        return ""

