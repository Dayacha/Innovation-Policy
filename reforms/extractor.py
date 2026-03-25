"""
Text extraction from PDF files.

Extracts text from OECD Economic Survey PDFs, handling multi-column
layouts, headers/footers, and other formatting challenges.
"""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_text_from_pdf(pdf_path, output_path=None):
    """Extract text from a PDF file.

    Uses pdfplumber as primary extractor (better for complex layouts),
    falls back to PyPDF2 if needed.

    Args:
        pdf_path: Path to the PDF file.
        output_path: Optional path to save extracted text.

    Returns:
        Extracted text as a string.
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    text = _extract_with_pdfplumber(pdf_path)
    if not text or len(text.strip()) < 100:
        logger.warning(
            f"pdfplumber extraction yielded little text for {pdf_path.name}, "
            f"trying PyPDF2"
        )
        text = _extract_with_pypdf2(pdf_path)

    # Clean the extracted text
    text = _clean_text(text)

    # Save if output path provided
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(text)
        logger.info(
            f"Extracted text saved: {output_path.name} "
            f"({len(text)} characters)"
        )

    return text


def _extract_with_pdfplumber(pdf_path):
    """Extract text using pdfplumber (better for complex layouts)."""
    try:
        import pdfplumber

        text_parts = []
        with pdfplumber.open(str(pdf_path)) as pdf:
            for i, page in enumerate(pdf.pages):
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(f"\n--- Page {i + 1} ---\n")
                    text_parts.append(page_text)

        return "\n".join(text_parts)
    except Exception as e:
        logger.warning(f"pdfplumber extraction failed: {e}")
        return ""


def _extract_with_pypdf2(pdf_path):
    """Extract text using PyPDF2 (fallback)."""
    try:
        from PyPDF2 import PdfReader

        reader = PdfReader(str(pdf_path))
        text_parts = []
        for i, page in enumerate(reader.pages):
            page_text = page.extract_text()
            if page_text:
                text_parts.append(f"\n--- Page {i + 1} ---\n")
                text_parts.append(page_text)

        return "\n".join(text_parts)
    except Exception as e:
        logger.warning(f"PyPDF2 extraction failed: {e}")
        return ""


def _clean_text(text):
    """Clean extracted text: fix common OCR/extraction artifacts."""
    # Remove excessive whitespace while preserving paragraph breaks
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Remove common header/footer patterns from OECD publications
    text = re.sub(
        r"OECD ECONOMIC SURVEYS.*?\n",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"© OECD \d{4}\s*", "", text
    )
    # Remove page numbers that appear on their own line
    text = re.sub(r"\n\s*\d{1,3}\s*\n", "\n", text)

    # Fix common ligature issues
    text = text.replace("ﬁ", "fi")
    text = text.replace("ﬂ", "fl")
    text = text.replace("ﬀ", "ff")
    text = text.replace("ﬃ", "ffi")
    text = text.replace("ﬄ", "ffl")

    return text.strip()


def chunk_text(text, chunk_size=12000, overlap=500):
    """Split text into overlapping chunks suitable for LLM processing.

    Attempts to split at paragraph boundaries to avoid cutting
    mid-sentence.

    Args:
        text: Full text to chunk.
        chunk_size: Target size of each chunk in characters.
        overlap: Number of characters to overlap between chunks.

    Returns:
        List of text chunks.
    """
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0

    while start < len(text):
        end = start + chunk_size

        if end >= len(text):
            chunks.append(text[start:])
            break

        # Try to find a good break point (paragraph boundary)
        break_point = text.rfind("\n\n", start + chunk_size // 2, end)
        if break_point == -1:
            # Fall back to sentence boundary
            break_point = text.rfind(". ", start + chunk_size // 2, end)
            if break_point != -1:
                break_point += 2  # Include the period and space
        if break_point == -1:
            # Fall back to word boundary
            break_point = text.rfind(" ", start + chunk_size // 2, end)
        if break_point == -1:
            break_point = end

        chunks.append(text[start:break_point])
        start = break_point - overlap

    logger.info(
        f"Split text into {len(chunks)} chunks "
        f"(avg {sum(len(c) for c in chunks) // len(chunks)} chars)"
    )
    return chunks


def identify_sections(text):
    """Identify major sections/chapters in the survey text.

    OECD Economic Surveys typically have:
    - Assessment and recommendations (Chapter 1) -- key for reforms
    - Macroeconomic outlook
    - Thematic chapters (varying topics)

    Returns:
        List of dicts with keys: title, start_pos, end_pos, text
    """
    # Common section header patterns in OECD surveys
    section_patterns = [
        # Chapter headers
        r"(?:Chapter\s+\d+[\.\s]*)(.*?)(?:\n)",
        # All-caps headers
        r"\n([A-Z][A-Z\s]{10,})\n",
        # Numbered sections
        r"\n(\d+\.\s+[A-Z].*?)\n",
    ]

    header_positions = []

    for pattern in section_patterns:
        for match in re.finditer(pattern, text):
            title = match.group(1).strip() if match.lastindex else match.group().strip()
            if len(title) > 5 and len(title) < 200:
                header_positions.append({
                    "title": title,
                    "start_pos": match.start(),
                })

    # Sort by position
    header_positions.sort(key=lambda x: x["start_pos"])

    # Deduplicate nearby headers: if two headers are within 100 chars
    # of each other, keep only the first (or the one with longer title
    # if at the same position).
    if header_positions:
        deduped = [header_positions[0]]
        for hp in header_positions[1:]:
            prev = deduped[-1]
            distance = hp["start_pos"] - prev["start_pos"]
            if distance < 100:
                # Keep the one with the longer (more informative) title
                if len(hp["title"]) > len(prev["title"]):
                    deduped[-1] = hp
            else:
                deduped.append(hp)
        header_positions = deduped

    # Build sections from headers
    sections = []
    for i, header in enumerate(header_positions):
        end_pos = (
            header_positions[i + 1]["start_pos"]
            if i + 1 < len(header_positions)
            else len(text)
        )
        sections.append({
            "title": header["title"],
            "start_pos": header["start_pos"],
            "end_pos": end_pos,
            "text": text[header["start_pos"]:end_pos],
        })

    return sections


def _merge_spans(spans):
    """Merge overlapping or adjacent (start, end) spans into
    non-overlapping spans sorted by start position."""
    if not spans:
        return []
    spans = sorted(spans)
    merged = [spans[0]]
    for start, end in spans[1:]:
        prev_start, prev_end = merged[-1]
        if start <= prev_end:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged


def _complement_spans(spans, total_length):
    """Return the complement of a set of spans within [0, total_length).

    Given the spans that are covered, returns the spans that are NOT covered.
    """
    complement = []
    pos = 0
    for start, end in spans:
        if pos < start:
            complement.append((pos, start))
        pos = max(pos, end)
    if pos < total_length:
        complement.append((pos, total_length))
    return complement


def get_priority_sections(text):
    """Extract non-overlapping priority and remaining text spans.

    The "Assessment and recommendations" section is the highest priority
    as it typically summarizes all reforms discussed in the survey.

    Returns:
        Tuple of (priority_text, remaining_text) where:
        - priority_text is the concatenation of reform-relevant sections
        - remaining_text is the concatenation of everything else
        Each section of the text is included at most once.
    """
    sections = identify_sections(text)

    priority_keywords = [
        # General reform sections
        "assessment", "recommendation", "reform", "policy", "structural",
        # Innovation-specific (most relevant for this project)
        "innovation", "research", "r&d", "science", "technology",
        "knowledge", "startup", "venture", "commerciali",
        # Other structural themes
        "regulation", "competition", "labour", "labor", "tax",
        "education", "housing",
    ]

    # Collect priority section spans
    priority_spans = []
    for section in sections:
        title_lower = section["title"].lower()
        if any(kw in title_lower for kw in priority_keywords):
            priority_spans.append(
                (section["start_pos"], section["end_pos"])
            )

    if not priority_spans:
        # Can't identify sections -> return full text, no remaining
        return text, ""

    # Merge overlapping priority spans
    priority_spans = _merge_spans(priority_spans)

    # Build priority text from merged spans
    priority_text = "\n\n".join(
        text[start:end] for start, end in priority_spans
    )

    # Build remaining text from the complement spans
    remaining_spans = _complement_spans(priority_spans, len(text))
    remaining_text = "\n\n".join(
        text[start:end].strip()
        for start, end in remaining_spans
        if len(text[start:end].strip()) > 200
    )

    return priority_text, remaining_text
