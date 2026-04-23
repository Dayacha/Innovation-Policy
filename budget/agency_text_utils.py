from __future__ import annotations

import gzip
import hashlib
import json
import re
from pathlib import Path
from typing import Optional

from budget import config as cfg

_PAGE_RE = re.compile(r"=== Page\s+([0-9]+(?:\.[0-9]+)?)")
_SHARED_LOOKUP_NAMESPACE = "agency_lookup"


def agency_variants(agency: dict) -> list[str]:
    canonical = agency.get("canonical_name", "")
    variants = list(agency.get("name_variants", []))
    vals = variants + [canonical, agency.get("source_entity", "")]
    out: list[str] = []
    seen: set[str] = set()
    for v in vals:
        if not isinstance(v, str) or not v.strip():
            continue
        low = v.strip().lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(v.strip())
    return out


def text_mentions(text: str, variants: list[str]) -> bool:
    low = text.lower()
    for v in variants:
        q = v.lower()
        if len(q) <= 4:
            if re.search(r"(?<![a-z])" + re.escape(q) + r"(?![a-z])", low):
                return True
        elif q in low:
            return True
    return False


def load_gzip_text(path: Path) -> str:
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception:
        return ""


def extract_snippets_from_text(
    text: str,
    variants: list[str],
    max_snippets: int = 3,
    before: int = 450,
    after: int = 1200,
) -> list[dict]:
    snippets: list[dict] = []
    low = text.lower()
    seen: set[str] = set()
    for v in variants:
        q = v.lower()
        if not q:
            continue
        if len(q) <= 4:
            matches = list(re.finditer(r"(?<![a-z])" + re.escape(q) + r"(?![a-z])", low))
        else:
            matches = list(re.finditer(re.escape(q), low))
        for m in matches[:max_snippets]:
            start = max(0, m.start() - before)
            end = min(len(text), m.end() + after)
            snippet = text[start:end].strip()
            key = snippet[:200]
            if key in seen:
                continue
            seen.add(key)
            page_match = _PAGE_RE.search(snippet)
            snippets.append(
                {
                    "page_number": page_match.group(1) if page_match else "",
                    "text": snippet,
                }
            )
            if len(snippets) >= max_snippets:
                return snippets
    return snippets


def load_lookup_cache(namespace: str, key_parts: list[str]) -> Optional[dict]:
    cache_dir = cfg.LLM_CACHE_DIR / namespace
    raw = "|".join(key_parts)
    p = cache_dir / f"{hashlib.md5(raw.encode()).hexdigest()}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_lookup_cache(namespace: str, key_parts: list[str], data: dict) -> None:
    cache_dir = cfg.LLM_CACHE_DIR / namespace
    cache_dir.mkdir(parents=True, exist_ok=True)
    raw = "|".join(key_parts)
    p = cache_dir / f"{hashlib.md5(raw.encode()).hexdigest()}.json"
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_shared_agency_lookup_cache(
    country: str,
    source_name: str,
    canonical_name: str,
) -> Optional[dict]:
    return load_lookup_cache(
        _SHARED_LOOKUP_NAMESPACE,
        [country, source_name, canonical_name.lower()],
    )


def save_shared_agency_lookup_cache(
    country: str,
    source_name: str,
    canonical_name: str,
    data: dict,
) -> None:
    save_lookup_cache(
        _SHARED_LOOKUP_NAMESPACE,
        [country, source_name, canonical_name.lower()],
        data,
    )
