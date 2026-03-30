"""Generic Danish-structure budget parsing pipeline."""

from __future__ import annotations

import re

import pandas as pd

from budget.section_parser import (
    parse_page_lines,
    _RE_AMOUNT_TAIL, _RE_MILL_AMOUNT, _RE_STANDALONE_AMOUNT,
    _RE_SUBITEM, _RE_SUBSECTION, _RE_ITEM_HEADER, _RE_PROGRAM_HEADER,
)
from budget.temporal_smoothing import compute_temporal_prior
from budget.taxonomy import INCLUDE_THRESHOLD, REVIEW_THRESHOLD, score_text
from budget.translation_utils import translate_to_english_glossary, preclean_text
from budget.extractor_common import (
    _BUDGET_KEYWORDS,
    _CODE_RE,
    _LEGAL_REF_RE,
    _NEG_KEYWORDS,
    _POS_KEYWORDS,
    DK_SECTION_MAP,
    currency,
    file_label,
    filepath_from_row,
    pillar,
)


def process_generic_file(
    *,
    sorted_pages,
    file_id,
    filepath_col: str,
    prior_results_df,
    records: list[dict],
    tax,
) -> None:
    """Process one file through the generic Danish-style parser."""
    section_code = ""
    section_name = ""
    current_program_code = ""
    current_program_desc = ""

    _re_old_amount = re.compile(r"^\s*[÷\-\+]?\s*\d{1,3}(?:[.,]\d{3})+\s*\d?\)?\s*$")
    _re_mill_amount_detect = re.compile(r"^\s*[-÷]?\s*\d{1,4}(?:\.\d{3})*[,\s]\d{1,2}\s*\d?\)?\s*$")
    mill_count = 0
    old_count = 0
    for r in sorted_pages.itertuples(index=False):
        for line in str(r.text if isinstance(r.text, str) else "").splitlines():
            if _re_mill_amount_detect.match(line):
                mill_count += 1
            if _re_old_amount.match(line):
                old_count += 1
    mill_kr_mode = mill_count > old_count and mill_count > 20

    in_artsoversigt = False

    for row in sorted_pages.itertuples(index=False):
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue

        filepath = filepath_from_row(row, filepath_col)
        country = str(row.country_guess)
        year = str(row.year_guess)
        source_filename = filepath.split("/")[-1]
        row_currency = currency(country, year)
        file_lbl = file_label(country, year, source_filename)

        budget_lines, section_code, section_name, in_artsoversigt = parse_page_lines(
            page_text, section_code, section_name,
            mill_kr_mode=mill_kr_mode, in_artsoversigt=in_artsoversigt,
        )

        for bl in budget_lines:
            if bl.line_type == "group_header":
                current_program_code = ""
                current_program_desc = ""
                continue

            if re.match(r"^ad\s+\d{1,2}\.\d{2}", preclean_text(bl.section_name), re.IGNORECASE):
                continue

            if bl.amount_value == 0:
                if bl.line_code and _CODE_RE.match(bl.line_code) and not _LEGAL_REF_RE.match(preclean_text(bl.description)):
                    current_program_code = bl.line_code
                    current_program_desc = bl.description
                continue

            merged_desc = bl.description
            ctx_after_is_amount = bool(
                _RE_AMOUNT_TAIL.search(bl.context_after)
                or _RE_STANDALONE_AMOUNT.match(bl.context_after)
                or (mill_kr_mode and _RE_MILL_AMOUNT.match(bl.context_after))
            )
            ctx_after_is_new_line = bool(
                _RE_SUBITEM.match(bl.context_after)
                or _RE_SUBSECTION.match(bl.context_after)
                or _RE_ITEM_HEADER.match(bl.context_after)
                or _RE_PROGRAM_HEADER.match(bl.context_after)
            )
            if (
                bl.context_after
                and not ctx_after_is_amount
                and not ctx_after_is_new_line
                and len(bl.description.split()) < 5
                and not _LEGAL_REF_RE.match(bl.description)
            ):
                merged_desc = f"{bl.description} {bl.context_after}".strip()

            program_code = current_program_code
            program_desc = current_program_desc
            if bl.line_code and _CODE_RE.match(bl.line_code):
                program_code = bl.line_code
                program_desc = merged_desc
                current_program_code = program_code
                current_program_desc = program_desc
            elif not bl.line_code and bl.item_code and bl.item_code != current_program_code:
                program_code = ""
                program_desc = ""

            desc_clean = preclean_text(merged_desc)
            desc_lower = merged_desc.lower()
            budget_type = ""
            for kw in ("driftsudgifter", "anlægsudgifter", "anlægstilskud", "tilskud"):
                if kw in desc_lower:
                    budget_type = kw
                    break

            if mill_kr_mode and not bl.line_code and not budget_type:
                orig_desc_clean = preclean_text(bl.description)
                orig_no_parens = re.sub(r"\([^)]*\)", "", orig_desc_clean).strip()
                if not any(c.isdigit() for c in orig_no_parens):
                    program_code = ""
                    program_desc = ""

            if re.search(r"indtægter|revenue|income", desc_lower):
                continue

            code_in_line = _CODE_RE.search(desc_clean)
            if code_in_line:
                derived_code = code_in_line.group()
                derived_desc = desc_clean[code_in_line.end():].strip(" .,:;-")
                if not program_code:
                    program_code = derived_code
                    current_program_code = derived_code
                if derived_desc and not program_desc:
                    program_desc = derived_desc
                    current_program_desc = derived_desc

            section_clean = preclean_text(bl.section_name)
            desc_clean = preclean_text(merged_desc)
            prog_clean = preclean_text(program_desc)

            item_code = bl.item_code or program_code
            item_desc = bl.item_description or program_desc

            scoring_text = prog_clean or desc_clean
            if prog_clean and _CODE_RE.search(desc_clean):
                scoring_text = f"{prog_clean} {desc_clean}".strip()
            rd_signal_re = re.compile(
                r"forskning|forsøgs|udvikling|research|teknolog|universit|videnskab",
                re.IGNORECASE,
            )
            item_clean = preclean_text(item_desc)
            if item_clean and not rd_signal_re.search(scoring_text):
                scoring_text = f"{item_clean} {scoring_text}".strip()
            content = f"{section_clean} {scoring_text}"
            content_score, content_hits, content_cat = score_text(content, tax)
            desc_score, _, _ = score_text(scoring_text, tax)
            if rd_signal_re.search(scoring_text):
                desc_score = max(desc_score, INCLUDE_THRESHOLD)

            context = f"{section_clean} {scoring_text} {bl.context_before} {bl.context_after}"
            context_score, context_hits, context_cat = score_text(context, tax)

            if context_score > content_score:
                score, hits, category = context_score, context_hits, context_cat
            else:
                score, hits, category = content_score, content_hits, content_cat

            desc_lower_clean = desc_clean.lower()
            if desc_lower_clean in {
                "bevilling i alt", "beviling i alt", "aktivitet i alt",
                "nettostyrede aktiviteter", "udgifter i alt",
                "artsoversigt", "artsoversigt:",
                "driftsindtægter", "anlægsindtægter", "overførselsudgifter",
                "skatter og overførselsindtægter",
                "overførsler mellem offentlige myndigheder", "finansielle poster",
            }:
                continue
            if not any(char.isdigit() for char in desc_clean) and not program_code:
                continue
            if re.fullmatch(r"[0-9 .,÷\-]+", desc_clean.strip()):
                continue
            if "......" in merged_desc or "......" in bl.description or "......" in program_desc:
                continue
            if _LEGAL_REF_RE.match(desc_clean):
                continue
            if len(desc_clean.split()) < 2 and not program_code:
                continue
            if program_code and _LEGAL_REF_RE.match(prog_clean):
                continue
            has_code = bool(program_code or _CODE_RE.search(desc_clean) or _CODE_RE.search(section_clean))
            has_budget_kw = any(k in desc_clean.lower() for k in _BUDGET_KEYWORDS)
            if not (has_code or has_budget_kw):
                continue
            has_pos = any(k in desc_clean.lower() for k in _POS_KEYWORDS)
            has_neg = any(k in desc_clean.lower() for k in _NEG_KEYWORDS)
            if has_neg and not has_pos:
                continue
            raw_prog = program_desc or bl.item_description or ""
            raw_desc = bl.description or ""
            full_text_raw = f"{raw_prog} {raw_desc} {merged_desc}"
            if re.search(r"\btilskud under\b.+ministeriet", full_text_raw, re.IGNORECASE):
                continue
            if re.search(r"\bkøbsmoms\b|\bfritidsundervisning\b", desc_clean, re.IGNORECASE):
                continue
            if desc_clean.lower() in {
                "tilskud", "driftsudgifter", "anlægsudgifter", "anlægstilskud",
                "indtægter", "udlån", "kapitalindtægter",
            } and not program_code:
                continue
            if desc_score == 0 and context_score < INCLUDE_THRESHOLD:
                continue

            boost_match = re.search(r"teknologisk service|teknologirådet|tekniske prøvenævn", scoring_text, re.IGNORECASE)
            if boost_match:
                content_score = max(content_score, INCLUDE_THRESHOLD)
                context_score = max(context_score, REVIEW_THRESHOLD)
                score = max(score, content_score, context_score)
                category = "innovation_system"

            temporal_prior = compute_temporal_prior(
                country=country,
                year=year,
                section_code=bl.section_code,
                program_code=program_code,
                program_description=program_desc,
                line_description=merged_desc,
                history_df=prior_results_df,
            )
            smoothed_score = score + temporal_prior.boost
            if max(context_score, smoothed_score) < REVIEW_THRESHOLD:
                continue

            if re.search(r"ændringer i medfør", desc_clean, re.IGNORECASE):
                continue

            if not program_code:
                m_pc = _CODE_RE.search(desc_clean)
                if m_pc:
                    program_code = m_pc.group()
                    if not program_desc:
                        tail = desc_clean[m_pc.end():].strip(" .,:;-")
                        if tail:
                            program_desc = tail
            if not item_code and program_code:
                item_code = program_code
            if not item_desc and program_desc:
                item_desc = program_desc

            has_rd_word = bool(re.search(r"forskning|forsøgs|udvikling|research|teknolog", scoring_text, re.IGNORECASE))
            decision = "include" if (smoothed_score >= INCLUDE_THRESHOLD and (desc_score > 0 or has_rd_word)) else "review"

            parse_error = False
            code_in_orig = _CODE_RE.search(preclean_text(bl.description))
            if (
                code_in_orig and item_code
                and code_in_orig.group() != item_code
                and code_in_orig.group() != bl.line_code
            ):
                parse_error = True
            orig_lower = bl.description.lower()
            if re.search(r"(tilskud|driftsudgifter|anlægsudgifter|indtægter)\s+\d{1,2}\.\d{2}", orig_lower):
                parse_error = True
            if parse_error:
                decision = "review"
            if boost_match:
                decision = "include"

            confidence = 0.40 + 0.10 * smoothed_score
            if has_code:
                confidence += 0.10
            if has_budget_kw:
                confidence += 0.10
            if has_pos:
                confidence += 0.05
            if has_neg:
                confidence -= 0.10
            if temporal_prior.boost:
                confidence += 0.03
            confidence = round(min(0.99, max(0.05, confidence)), 3)
            display_desc = f"{program_code} {program_desc}".strip() if program_code else (program_desc or merged_desc)

            snippet_en = translate_to_english_glossary(display_desc)
            merged_en = translate_to_english_glossary(desc_clean)
            program_desc_en = translate_to_english_glossary(program_desc) if program_desc else ""
            section_en = translate_to_english_glossary(section_clean)
            if country.lower() == "denmark":
                section_en = DK_SECTION_MAP.get(bl.section_code, section_en)
            row_pillar = pillar(category, hits, scoring_text)

            rationale = f"score={score}; hits=[{', '.join(hits[:6])}]; section={bl.section_code}"
            if temporal_prior.boost:
                rationale = (
                    f"{rationale}; prior_boost={temporal_prior.boost}; "
                    f"prior_match={temporal_prior.match_type}; prior_years={temporal_prior.matched_years}"
                )

            records.append({
                "country": country,
                "year": year,
                "section_code": bl.section_code,
                "section_name": bl.section_name,
                "section_name_en": section_en,
                "program_code": program_code,
                "program_description": program_desc,
                "program_description_en": program_desc_en,
                "budget_type": budget_type,
                "item_code": item_code,
                "item_description": item_desc,
                "line_code": bl.line_code,
                "line_description": display_desc,
                "line_description_en": snippet_en,
                "merged_line": merged_desc,
                "merged_line_en": merged_en,
                "raw_line": bl.original_line,
                "context_before": bl.context_before,
                "context_after": bl.context_after,
                "line_type": bl.line_type,
                "amount_local": bl.amount_value,
                "currency": row_currency,
                "amount_raw": bl.raw_amount,
                "rd_category": category,
                "pillar": row_pillar,
                "rd_label": row_pillar if row_pillar else category,
                "taxonomy_score": score,
                "smoothed_taxonomy_score": smoothed_score,
                "content_score": content_score,
                "context_score": context_score,
                "taxonomy_hits": "; ".join(hits[:8]),
                "decision": decision,
                "confidence": confidence,
                "parse_error": parse_error,
                "temporal_prior_boost": temporal_prior.boost,
                "temporal_prior_match_type": temporal_prior.match_type,
                "temporal_prior_years": temporal_prior.matched_years,
                "rationale": rationale,
                "source_file": source_filename,
                "page_number": row.page_number,
                "file_id": file_id,
                "file_label": file_lbl,
                "source_filename": source_filename,
                "keywords_matched": "; ".join(hits),
                "text_snippet": bl.description,
                "text_snippet_en": snippet_en,
                "detected_amount_raw": bl.raw_amount,
                "detected_amount_value": bl.amount_value,
                "detected_currency": row_currency,
                "is_header_total": bl.line_type == "section_header",
                "is_program_level": bl.line_type in ("subsection", "line_item"),
            })


def postprocess_records(df: pd.DataFrame) -> pd.DataFrame:
    """Apply final sorting and deduplication for extracted records."""
    if df.empty:
        return df

    for col in (
        "file_label", "source_filename", "keywords_matched",
        "text_snippet", "text_snippet_en", "detected_amount_raw",
        "detected_amount_value", "detected_currency",
        "is_header_total", "is_program_level",
    ):
        if col not in df.columns:
            df[col] = ""

    df = df.sort_values(
        ["taxonomy_score", "amount_local", "file_label", "page_number"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)

    if "file_id" in df.columns and "program_code" in df.columns:
        dup_key = ["file_id", "program_code", "amount_local"]
        has_prog = df["program_code"].notna() & (df["program_code"] != "")
        df_with_prog = df[has_prog].copy()
        df_no_prog = df[~has_prog].copy()
        df_with_prog = (
            df_with_prog.sort_values(["file_id", "program_code", "page_number"])
            .drop_duplicates(subset=dup_key, keep="first")
        )

        if not df_with_prog.empty:
            prog = df_with_prog["program_code"].astype(str)
            file_id_col = df_with_prog["file_id"].astype(str)
            child_keys: set[tuple[str, str]] = set(zip(file_id_col, prog))

            def is_covered_parent(row: pd.Series) -> bool:
                prefix = str(row["program_code"]) + "."
                fid = str(row["file_id"])
                return any(
                    pc.startswith(prefix) for (f, pc) in child_keys
                    if f == fid and pc != str(row["program_code"])
                )

            parent_mask = df_with_prog.apply(is_covered_parent, axis=1)
            df_with_prog = df_with_prog[~parent_mask]

        df = pd.concat([df_with_prog, df_no_prog], ignore_index=True)
        df = df.sort_values(
            ["taxonomy_score", "amount_local", "file_label", "page_number"],
            ascending=[False, False, True, True],
        ).reset_index(drop=True)

    return df


__all__ = ["postprocess_records", "process_generic_file"]
