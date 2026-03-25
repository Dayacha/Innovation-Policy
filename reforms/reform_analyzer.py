"""
Core reform analyzer: uses LLM to extract, classify, and assess reforms.

This is the central module of the pipeline. It:
1. Takes extracted text from an OECD Economic Survey
2. Chunks it appropriately for LLM processing
3. Sends chunks to the LLM with carefully designed prompts
4. Consolidates results across chunks using programmatic deduplication
5. Resolves page numbers from --- Page N --- markers
6. Ranks reforms within importance bins (hierarchical classification)
7. Outputs structured reform data as JSON
"""

import json
import logging
import re
import time
from difflib import SequenceMatcher
from pathlib import Path

from .extractor import chunk_text, get_priority_sections
from .llm_client import LLMClient
from .prompts import (
    DEDUP_PROMPT,
    EXTRACTION_PROMPT,
    SYSTEM_PROMPT,
    THEMES_SUBTHEMES,
    VALID_THEMES,
    VALID_SUBTHEMES,
    THEME_LIST,
    build_innovation_system_prompt,
)
from .countries import CODE_TO_NAME

logger = logging.getLogger(__name__)


def _similarity(a, b):
    """Compute similarity ratio between two strings (0 to 1)."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


class ReformAnalyzer:
    """Analyzes OECD Economic Survey text to extract reforms."""

    def __init__(self, config):
        self.config = config
        self.llm = LLMClient(config)
        self.chunk_size = config.get("processing", {}).get("chunk_size", 12000)
        self.chunk_overlap = config.get("processing", {}).get(
            "chunk_overlap", 500
        )
        self.output_dir = Path(
            config.get("paths", {}).get("reforms_json", "data/reforms_json")
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Similarity threshold for considering two reforms as duplicates
        self.dedup_threshold = config.get("processing", {}).get(
            "dedup_threshold", 0.65
        )
        # Themes to extract — defaults to all themes if not specified in config
        self.theme_list = config.get("themes") or THEME_LIST

        # Use specialised innovation system prompt when extracting innovation only
        innovation_only = (
            len(self.theme_list) == 1 and self.theme_list[0] == "innovation"
        )
        self._system_prompt = (
            build_innovation_system_prompt() if innovation_only else SYSTEM_PROMPT
        )

    def analyze_survey(self, text, country_code, survey_year,
                       use_priority_sections=True):
        """Analyze a complete survey text and extract reforms.

        Args:
            text: Full extracted text from the survey PDF.
            country_code: ISO 3166-1 alpha-3 country code.
            survey_year: Year of survey publication.
            use_priority_sections: If True, prioritize the "Assessment and
                recommendations" section and other reform-heavy sections.

        Returns:
            Dict with consolidated reform data.
        """
        country_name = CODE_TO_NAME.get(country_code, country_code)
        survey_start = time.time()

        print(f"\n  [EXTRACTION] Starting analysis: "
              f"{country_name} {survey_year} ({len(text):,} characters)")

        # ── Step 1: Identify text segments to process ──
        if use_priority_sections:
            priority_text, remaining_text = get_priority_sections(text)
            texts_to_process = [("priority sections", priority_text)]
            if remaining_text and len(remaining_text.strip()) > 500:
                texts_to_process.append(("remaining sections", remaining_text))
            print(f"  [EXTRACTION] Split text: "
                  f"{len(priority_text):,} chars priority, "
                  f"{len(remaining_text):,} chars remaining "
                  f"(no overlap)")
        else:
            texts_to_process = [("full text", text)]

        # ── Step 2: Chunk and extract from each text segment ──
        all_raw_reforms = []
        total_chunks = 0
        for label, segment_text in texts_to_process:
            chunks = chunk_text(
                segment_text, self.chunk_size, self.chunk_overlap
            )
            total_chunks += len(chunks)

        print(f"  [EXTRACTION] Text split into {total_chunks} chunks, "
              f"sending to LLM...")

        chunk_idx = 0
        for label, segment_text in texts_to_process:
            chunks = chunk_text(
                segment_text, self.chunk_size, self.chunk_overlap
            )

            for i, chunk in enumerate(chunks):
                chunk_idx += 1
                chunk_start = time.time()
                reforms = self._extract_from_chunk(
                    chunk, country_name, survey_year
                )
                elapsed = time.time() - chunk_start
                all_raw_reforms.extend(reforms)
                print(f"    Chunk {chunk_idx}/{total_chunks} "
                      f"({label}, {len(chunk):,} chars) -> "
                      f"{len(reforms)} reforms "
                      f"({elapsed:.1f}s)")

        print(f"  [EXTRACTION] Raw extraction complete: "
              f"{len(all_raw_reforms)} reforms from {total_chunks} chunks")

        if not all_raw_reforms:
            print(f"  [EXTRACTION] No reforms found in this survey")
            result = self._empty_result(country_code, country_name, survey_year)
            self._save_result(result, country_code, survey_year)
            return result

        # ── Step 3a: Fast text-similarity dedup (obvious duplicates) ──
        print(f"  [DEDUP] Pass 1: text similarity "
              f"(threshold {self.dedup_threshold})...")
        dedup_start = time.time()
        after_text_dedup = self._deduplicate_by_text(all_raw_reforms)
        removed_text = len(all_raw_reforms) - len(after_text_dedup)
        print(f"    {len(after_text_dedup)} remain "
              f"({removed_text} obvious duplicates removed)")

        # ── Step 3b: LLM-assisted dedup within theme groups ──
        print(f"  [DEDUP] Pass 2: LLM-assisted grouping by theme...")
        deduplicated = self._deduplicate_by_llm_groups(
            after_text_dedup, country_name, survey_year
        )
        removed_llm = len(after_text_dedup) - len(deduplicated)
        dedup_elapsed = time.time() - dedup_start
        total_removed = len(all_raw_reforms) - len(deduplicated)
        print(f"  [DEDUP] Done: {len(deduplicated)} unique reforms "
              f"({total_removed} total duplicates removed, "
              f"{dedup_elapsed:.1f}s)")

        # ── Step 4: Resolve page numbers from text markers ──
        print(f"  [PAGES] Resolving source page numbers...")
        self._resolve_page_numbers(deduplicated, text)
        pages_found = sum(
            1 for r in deduplicated if r.get("source_page_start") is not None
        )
        print(f"    Page numbers resolved for {pages_found}/"
              f"{len(deduplicated)} reforms")

        # ── Step 5: Assign IDs and build final result ──
        print(f"  [FINALIZE] Assigning reform IDs and validating...")
        consolidated = self._build_result(
            deduplicated, country_code, country_name, survey_year
        )

        # ── Step 6: Save ──
        self._save_result(consolidated, country_code, survey_year)

        n_reforms = consolidated["total_reforms"]
        n_big = consolidated["big_reforms_count"]
        total_elapsed = time.time() - survey_start
        print(f"  [DONE] {country_name} {survey_year}: "
              f"{n_reforms} reforms ({n_big} major), "
              f"completed in {total_elapsed:.1f}s")

        # Breakdown: show sub_theme when extracting innovation only
        if len(self.theme_list) == 1 and self.theme_list[0] == "innovation":
            sub_counts = {}
            actor_counts = {}
            stage_counts = {}
            for r in consolidated["reforms"]:
                st = r.get("sub_theme", "other")
                sub_counts[st] = sub_counts.get(st, 0) + 1
                actor_counts[r.get("rd_actor", "unknown")] = \
                    actor_counts.get(r.get("rd_actor", "unknown"), 0) + 1
                stage_counts[r.get("rd_stage", "unknown")] = \
                    stage_counts.get(r.get("rd_stage", "unknown"), 0) + 1
            if sub_counts:
                sub_str = ", ".join(
                    f"{t}: {c}" for t, c in sorted(sub_counts.items())
                )
                print(f"    Sub-types:   {sub_str}")
                actor_str = ", ".join(
                    f"{k}: {v}" for k, v in sorted(actor_counts.items())
                )
                print(f"    Actors:      {actor_str}")
                stage_str = ", ".join(
                    f"{k}: {v}" for k, v in sorted(stage_counts.items())
                )
                print(f"    Stages:      {stage_str}")
        else:
            theme_counts = {}
            for r in consolidated["reforms"]:
                theme = r.get("theme", "other")
                theme_counts[theme] = theme_counts.get(theme, 0) + 1
            if theme_counts:
                breakdown = ", ".join(
                    f"{t}: {c}" for t, c in sorted(theme_counts.items())
                )
                print(f"    Themes: {breakdown}")

        return consolidated

    def _extract_from_chunk(self, text_chunk, country_name, survey_year):
        """Extract reforms from a single text chunk using the LLM.

        Returns:
            List of reform dicts extracted from this chunk.
        """
        # Build theme keys string for the prompt
        theme_keys = ", ".join(f'"{t}"' for t in self.theme_list) + ', "other"'

        prompt = EXTRACTION_PROMPT.format(
            country=country_name,
            survey_year=survey_year,
            text=text_chunk,
            theme_keys=theme_keys,
        )

        try:
            response = self.llm.call(
                self._system_prompt, prompt, operation=LLMClient.OP_EXTRACTION
            )
            result = self._parse_json_response(response)

            if result and "reforms" in result:
                return result["reforms"]
            else:
                logger.warning("No 'reforms' key in LLM response")
                return []

        except Exception as e:
            logger.error(f"Error extracting reforms from chunk: {e}")
            return []

    # ──────────────────────────────────────────────────────────
    # Pass 1: fast text-similarity dedup
    # ──────────────────────────────────────────────────────────

    def _deduplicate_by_text(self, reforms):
        """Remove near-identical reforms using text similarity.

        Fast O(n^2) pass that catches verbatim or near-verbatim
        duplicates arising from chunk overlap. Uses both description
        and source_quote similarity (the latter is often closer to
        verbatim when the LLM paraphrases differently).
        """
        if len(reforms) <= 1:
            return reforms

        merged_into = {}

        for i in range(len(reforms)):
            if i in merged_into:
                continue
            desc_i = reforms[i].get("description", "")
            quote_i = reforms[i].get("source_quote", "")
            if not desc_i:
                continue
            for j in range(i + 1, len(reforms)):
                if j in merged_into:
                    continue
                desc_j = reforms[j].get("description", "")
                quote_j = reforms[j].get("source_quote", "")
                if not desc_j:
                    continue
                desc_sim = _similarity(desc_i, desc_j)
                # Also check source quotes (often near-verbatim even when
                # descriptions are paraphrased differently)
                quote_sim = (
                    _similarity(quote_i, quote_j)
                    if quote_i and quote_j
                    else 0.0
                )
                best_sim = max(desc_sim, quote_sim)
                if best_sim >= self.dedup_threshold:
                    if len(desc_j) > len(desc_i):
                        merged_into[i] = j
                        self._merge_reform_metadata(reforms[j], reforms[i])
                        break
                    else:
                        merged_into[j] = i
                        self._merge_reform_metadata(reforms[i], reforms[j])

        return [
            reforms[i] for i in range(len(reforms))
            if i not in merged_into
        ]

    # ──────────────────────────────────────────────────────────
    # Pass 2: LLM-assisted dedup within (theme, sub_theme) groups
    # ──────────────────────────────────────────────────────────

    def _deduplicate_by_llm_groups(self, reforms, country_name, survey_year):
        """Group reforms by theme, then use one LLM call per group to
        identify which entries refer to the same reform.

        Groups by primary theme, but also pulls in alternative_theme and
        secondary_theme so that a reform classified as theme=X with either
        field pointing to Y will meet reforms classified as theme=Y during
        dedup.  Overlapping groups are merged into connected components, so
        no reform is processed twice.

        Only sends groups with >1 reform to the LLM. Single-entry groups
        pass through unchanged.
        """
        # Step 1: record which theme keys each reform belongs to
        memberships = {}  # reform index → set of theme keys
        for i, r in enumerate(reforms):
            primary = r.get("theme", "other")
            themes = {primary}
            alt = r.get("alternative_theme")
            if alt and alt != primary:
                themes.add(alt)
            sec = r.get("secondary_type")
            if sec and sec != primary:
                themes.add(sec)
            memberships[i] = themes

        # Step 2: theme key → set of reform indices
        theme_to_reforms = {}
        for i, themes in memberships.items():
            for t in themes:
                theme_to_reforms.setdefault(t, set()).add(i)

        # Step 3: find connected components via BFS over theme keys
        # (two themes are connected if they share at least one reform)
        visited_themes = set()
        components = []  # each entry: (frozenset of reform indices,
                         #              frozenset of theme keys)
        for start_theme in theme_to_reforms:
            if start_theme in visited_themes:
                continue
            comp_themes = set()
            comp_reforms = set()
            queue = [start_theme]
            while queue:
                t = queue.pop()
                if t in visited_themes:
                    continue
                visited_themes.add(t)
                comp_themes.add(t)
                for idx in theme_to_reforms.get(t, set()):
                    comp_reforms.add(idx)
                    for t2 in memberships[idx]:
                        if t2 not in visited_themes:
                            queue.append(t2)
            components.append((frozenset(comp_reforms),
                                frozenset(comp_themes)))

        # Count groups
        multi_components = [(ri, ti) for ri, ti in components if len(ri) > 1]
        single_count = sum(1 for ri, _ in components if len(ri) == 1)
        cross_count = sum(1 for _, ti in multi_components if len(ti) > 1)
        print(f"    {len(components)} dedup groups: "
              f"{single_count} singletons, "
              f"{len(multi_components)} groups with potential duplicates "
              f"({cross_count} cross-theme)")

        result_reforms = []

        # Pass through singletons unchanged
        for reform_indices, _ in components:
            if len(reform_indices) == 1:
                result_reforms.append(reforms[next(iter(reform_indices))])

        # LLM dedup for multi-entry groups
        for group_num, (reform_indices, theme_keys) in enumerate(
            multi_components
        ):
            indices = sorted(reform_indices)
            group_reforms = [reforms[i] for i in indices]

            # Label: primary theme of the group (most common among members)
            from collections import Counter
            primary_counts = Counter(
                reforms[i].get("theme", "other") for i in indices
            )
            primary_theme = primary_counts.most_common(1)[0][0]
            theme_label = (
                primary_theme
                if len(theme_keys) == 1
                else " / ".join(sorted(theme_keys))
            )

            print(f"    Group {group_num + 1}/{len(multi_components)}: "
                  f"{theme_label} ({len(group_reforms)} entries)")

            merged = self._llm_dedup_group(
                group_reforms, country_name, survey_year,
                primary_theme, theme_label
            )
            result_reforms.extend(merged)

        return result_reforms

    def _llm_dedup_group(self, group_reforms, country_name, survey_year,
                         theme, theme_label=None):
        """Use a single LLM call to deduplicate reforms within a group.

        theme        — the primary (most common) theme key, used to look up
                       valid sub_themes.
        theme_label  — the string shown in the prompt (may be "X / Y" for
                       cross-theme groups); defaults to theme if not given.

        Falls back to returning the group unchanged if the LLM call fails.
        """
        if theme_label is None:
            theme_label = theme

        # Build the numbered description list for the prompt
        desc_lines = []
        for i, r in enumerate(group_reforms):
            desc = r.get("description", "(no description)")
            year = r.get("implementation_year", "?")
            status = r.get("status", "?")
            is_major = r.get("is_major_reform", False)
            sub = r.get("sub_theme", "?")
            alt = r.get("alternative_theme", "")
            sec = r.get("secondary_type", "")
            rd_actor = r.get("rd_actor", "")
            rd_stage = r.get("rd_stage", "")
            alt_str = f", alt_theme={alt}" if alt else ""
            sec_str = f", secondary_type={sec}" if sec else ""
            actor_str = f", rd_actor={rd_actor}" if rd_actor and rd_actor != "unknown" else ""
            stage_str = f", rd_stage={rd_stage}" if rd_stage and rd_stage != "unknown" else ""
            desc_lines.append(
                f"[{i}] (sub_theme={sub}{alt_str}{sec_str}{actor_str}{stage_str}, "
                f"year={year}, status={status}, is_major_reform={is_major}) {desc}"
            )
        descriptions_text = "\n".join(desc_lines)

        # Get valid subthemes — union across all theme keys in the label
        theme_keys = [t.strip() for t in theme_label.split("/")]
        all_subthemes = []
        for tk in theme_keys:
            info = THEMES_SUBTHEMES.get(tk, {})
            all_subthemes.extend(info.get("subthemes", {"other": {}}).keys())
        valid_subthemes = ", ".join(dict.fromkeys(all_subthemes))  # deduped

        prompt = DEDUP_PROMPT.format(
            country=country_name,
            survey_year=survey_year,
            theme=theme_label,
            descriptions=descriptions_text,
            max_index=len(group_reforms) - 1,
            valid_subthemes=valid_subthemes,
        )

        try:
            dedup_start = time.time()
            response = self.llm.call(
                self._system_prompt, prompt, operation=LLMClient.OP_WITHIN_SURVEY_DEDUP
            )
            result = self._parse_json_response(response)
            elapsed = time.time() - dedup_start

            if not result or "groups" not in result:
                logger.warning(
                    f"LLM dedup failed for {theme}, keeping all entries"
                )
                return group_reforms

            # Validate that all indices are covered
            all_indices = set()
            for g in result["groups"]:
                all_indices.update(g.get("indices", []))
            expected = set(range(len(group_reforms)))
            if all_indices != expected:
                logger.warning(
                    f"LLM dedup returned incomplete indices "
                    f"({len(all_indices)}/{len(expected)}), "
                    f"keeping all entries"
                )
                return group_reforms

            # Build merged reforms from the LLM groupings
            merged_reforms = []
            for g in result["groups"]:
                indices = g["indices"]
                # Start from the reform with the longest description
                base_idx = max(
                    indices,
                    key=lambda i: len(
                        group_reforms[i].get("description", "")
                    ),
                )
                merged = dict(group_reforms[base_idx])

                # Apply LLM's merged fields
                for field in ("merged_description", "description"):
                    if g.get("merged_description"):
                        merged["description"] = g["merged_description"]
                        break
                for field in (
                    "is_major_reform", "importance_bucket",
                    "importance_rationale", "importance_confidence",
                    "implementation_year", "implementation_year_source",
                    "implementation_year_confidence",
                    "announcement_year", "announcement_year_source",
                    "announcement_year_confidence",
                    "legislation_year", "legislation_year_source",
                    "legislation_year_confidence",
                    "status", "status_evidence", "status_confidence",
                    "growth_orientation",
                    "growth_orientation_rationale",
                    "growth_orientation_confidence",
                    "secondary_type",
                    "alternative_theme",
                    "sub_theme",
                    "rd_actor",
                    "rd_stage",
                    "package_name", "component_name", "is_component",
                ):
                    if g.get(field) is not None:
                        merged[field] = g[field]

                # Merge source quotes: keep the longest
                quotes = [
                    group_reforms[i].get("source_quote", "")
                    for i in indices
                    if group_reforms[i].get("source_quote")
                ]
                if quotes:
                    merged["source_quote"] = max(quotes, key=len)

                merged_reforms.append(merged)

            n_in = len(group_reforms)
            n_out = len(merged_reforms)
            print(f"      -> {n_in} entries merged into {n_out} "
                  f"({elapsed:.1f}s)")
            return merged_reforms

        except Exception as e:
            logger.error(f"LLM dedup error for {theme}: {e}")
            return group_reforms

    # ──────────────────────────────────────────────────────────
    # Page number resolution
    # ──────────────────────────────────────────────────────────

    def _resolve_page_numbers(self, reforms, full_text):
        """Add source_page_start and source_page_end to each reform
        by locating source_quote in the full text and reading the
        nearest --- Page N --- markers.
        """
        # Build page index: list of (char_position, page_number)
        page_markers = [
            (m.start(), int(m.group(1)))
            for m in re.finditer(r'--- Page (\d+) ---', full_text)
        ]

        if not page_markers:
            for r in reforms:
                r["source_page_start"] = None
                r["source_page_end"] = None
            return

        text_lower = full_text.lower()

        for r in reforms:
            quote = r.get("source_quote", "")
            if not quote or len(quote) < 15:
                r["source_page_start"] = None
                r["source_page_end"] = None
                continue

            # Try to find the quote in the text, with progressively
            # shorter substrings for robustness
            pos = -1
            quote_lower = quote.lower()
            for length in (len(quote), 80, 50, 30):
                search = quote_lower[:length].strip()
                if len(search) < 15:
                    break
                pos = text_lower.find(search)
                if pos != -1:
                    break

            if pos == -1:
                r["source_page_start"] = None
                r["source_page_end"] = None
                continue

            # Find which page this position falls on
            page_start = page_markers[0][1]  # default to first page
            for marker_pos, page_num in page_markers:
                if marker_pos <= pos:
                    page_start = page_num
                else:
                    break

            # Find end page
            end_pos = pos + len(quote)
            page_end = page_start
            for marker_pos, page_num in page_markers:
                if marker_pos <= end_pos:
                    page_end = page_num
                else:
                    break

            r["source_page_start"] = page_start
            r["source_page_end"] = page_end

    # ──────────────────────────────────────────────────────────
    # Metadata merge helper
    # ──────────────────────────────────────────────────────────

    def _merge_reform_metadata(self, keeper, duplicate):
        """Merge metadata from a duplicate reform into the keeper.

        Used by the text-similarity pass. Prefers more specific / non-null
        values.
        """
        if keeper.get("implementation_year") is None:
            keeper["implementation_year"] = duplicate.get("implementation_year")

        # is_major_reform is canonical: if either is major, keep major
        if duplicate.get("is_major_reform") and not keeper.get("is_major_reform"):
            keeper["is_major_reform"] = True
            keeper["importance_bucket"] = 3
            keeper["importance_rationale"] = duplicate.get(
                "importance_rationale",
                keeper.get("importance_rationale", ""),
            )
        elif not keeper.get("is_major_reform"):
            # Both non-major: take higher bucket as soft supporting metadata
            dup_importance = duplicate.get("importance_bucket", 0)
            keep_importance = keeper.get("importance_bucket", 0)
            if isinstance(dup_importance, int) and isinstance(keep_importance, int):
                if dup_importance > keep_importance:
                    keeper["importance_bucket"] = dup_importance
                    keeper["importance_rationale"] = duplicate.get(
                        "importance_rationale",
                        keeper.get("importance_rationale", ""),
                    )

        if not keeper.get("sub_theme") and duplicate.get("sub_theme"):
            keeper["sub_theme"] = duplicate["sub_theme"]

        if not keeper.get("secondary_type") and duplicate.get("secondary_type"):
            keeper["secondary_type"] = duplicate["secondary_type"]

        # rd_actor/rd_stage: prefer non-unknown values
        if keeper.get("rd_actor", "unknown") == "unknown" and \
                duplicate.get("rd_actor", "unknown") != "unknown":
            keeper["rd_actor"] = duplicate["rd_actor"]

        if keeper.get("rd_stage", "unknown") == "unknown" and \
                duplicate.get("rd_stage", "unknown") != "unknown":
            keeper["rd_stage"] = duplicate["rd_stage"]

        dup_quote = duplicate.get("source_quote", "")
        keep_quote = keeper.get("source_quote", "")
        if len(dup_quote) > len(keep_quote):
            keeper["source_quote"] = dup_quote

    def _build_result(self, reforms, country_code, country_name, survey_year):
        """Build the final result dict from deduplicated reforms."""
        cleaned = []
        for i, reform in enumerate(reforms):
            # Remove internal metadata fields
            r = {
                k: v for k, v in reform.items()
                if not k.startswith("_")
            }
            r["reform_id"] = f"{country_code}_{survey_year}_{i + 1:03d}"
            self._validate_reform(r, country_code, survey_year)
            cleaned.append(r)

        big_count = sum(
            1 for r in cleaned if r.get("is_major_reform", False)
        )

        return {
            "country_code": country_code,
            "country_name": country_name,
            "survey_year": survey_year,
            "reforms": cleaned,
            "survey_summary": "",
            "total_reforms": len(cleaned),
            "big_reforms_count": big_count,
        }

    def _save_result(self, result, country_code, survey_year):
        """Save result to JSON file."""
        output_path = self.output_dir / f"{country_code}_{survey_year}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        logger.info(f"Results saved: {output_path.name}")

    def _validate_reform(self, reform, country_code, survey_year):
        """Ensure a reform dict has all required fields with valid values."""
        valid_statuses = {
            "implemented", "legislated", "announced", "recommended",
            "unclear",
        }
        valid_year_sources = {
            "explicit", "inferred", "imputed_survey_year",
        }
        valid_confidence = {"high", "medium", "low"}
        valid_growth_orientations = {
            "growth_supporting", "growth_hindering", "mixed",
            "unclear_or_neutral",
        }

        # Set defaults for missing fields
        reform.setdefault("reform_id", "")
        reform.setdefault("description", "")
        reform.setdefault("theme", "other")
        reform.setdefault("sub_theme", "other")
        reform.setdefault("secondary_type", None)
        reform.setdefault("alternative_theme", None)
        reform.setdefault("rd_actor", "unknown")
        reform.setdefault("rd_stage", "unknown")
        reform.setdefault("package_name", "")
        reform.setdefault("component_name", None)
        reform.setdefault("is_component", False)
        reform.setdefault("growth_orientation", "unclear_or_neutral")
        reform.setdefault("growth_orientation_rationale", "")
        reform.setdefault("growth_orientation_confidence", "medium")
        reform.setdefault("implementation_year", None)
        reform.setdefault("announcement_year", None)
        reform.setdefault("announcement_year_source", None)
        reform.setdefault("announcement_year_confidence", None)
        reform.setdefault("legislation_year", None)
        reform.setdefault("legislation_year_source", None)
        reform.setdefault("legislation_year_confidence", None)
        reform.setdefault("implementation_year_end", None)
        reform.setdefault("implementation_year_source", None)
        reform.setdefault("implementation_year_confidence", None)
        reform.setdefault("status", "unclear")
        reform.setdefault("status_evidence", "")
        reform.setdefault("status_confidence", "low")
        reform.setdefault("is_major_reform", False)
        reform.setdefault("importance_bucket", 2)
        reform.setdefault("importance_rationale", "")
        reform.setdefault("importance_confidence", "medium")
        reform.setdefault("source_quote", "")
        reform.setdefault("source_page_start", None)
        reform.setdefault("source_page_end", None)

        # Validate theme
        if reform["theme"] not in VALID_THEMES:
            reform["theme"] = "other"

        # Validate sub_theme against allowed list for the theme
        theme_info = THEMES_SUBTHEMES.get(reform["theme"])
        if theme_info:
            if reform["sub_theme"] not in theme_info["subthemes"]:
                reform["sub_theme"] = "other"
        else:
            reform["sub_theme"] = "other"

        if reform.get("secondary_type") and \
                reform["secondary_type"] not in VALID_SUBTHEMES:
            reform["secondary_type"] = None

        if reform.get("alternative_theme") and \
                reform["alternative_theme"] not in VALID_THEMES:
            reform["alternative_theme"] = None

        if reform["status"] not in valid_statuses:
            logger.warning(
                f"[{country_code} {survey_year}] Invalid status "
                f"{reform['status']!r} for reform "
                f"{reform.get('reform_id') or reform.get('description', '')[:60]!r}"
                f" — flagged as 'unclear'"
            )
            reform["status"] = "unclear"
            reform["status_confidence"] = "low"

        if reform["growth_orientation"] not in valid_growth_orientations:
            reform["growth_orientation"] = "unclear_or_neutral"

        # Validate confidence fields
        if reform.get("growth_orientation_confidence") not in valid_confidence:
            reform["growth_orientation_confidence"] = "medium"
        if reform.get("status_confidence") not in valid_confidence:
            reform["status_confidence"] = "medium"

        if not isinstance(reform.get("importance_bucket"), int):
            try:
                reform["importance_bucket"] = int(reform["importance_bucket"])
            except (ValueError, TypeError):
                reform["importance_bucket"] = 2

        reform["importance_bucket"] = max(1, min(3, reform["importance_bucket"]))

        # is_major_reform is canonical — derive importance_bucket from it
        if reform.get("is_major_reform"):
            # Major flag set: ensure bucket is 3
            reform["importance_bucket"] = 3
        elif reform.get("importance_bucket") == 3:
            # Bucket says major but flag says no — trust the flag, demote bucket
            reform["importance_bucket"] = 2

        if reform.get("importance_confidence") not in valid_confidence:
            reform["importance_confidence"] = "medium"

        # Validate implementation year
        if reform["implementation_year"] is not None:
            try:
                year = int(reform["implementation_year"])
                if year < 1990 or year > 2030:
                    reform["implementation_year"] = None
                else:
                    reform["implementation_year"] = year
            except (ValueError, TypeError):
                reform["implementation_year"] = None

        # Validate year values and their source/confidence for all year fields
        for year_field, src_field, conf_field in (
            ("announcement_year",
             "announcement_year_source",
             "announcement_year_confidence"),
            ("legislation_year",
             "legislation_year_source",
             "legislation_year_confidence"),
            ("implementation_year",
             "implementation_year_source",
             "implementation_year_confidence"),
        ):
            if reform[year_field] is not None:
                try:
                    year = int(reform[year_field])
                    if year < 1990 or year > 2030:
                        reform[year_field] = None
                    else:
                        reform[year_field] = year
                except (ValueError, TypeError):
                    reform[year_field] = None

            if reform[year_field] is None:
                reform[src_field] = None
                reform[conf_field] = None
            else:
                src = reform.get(src_field)
                if src not in valid_year_sources:
                    reform[src_field] = "inferred"
                conf = reform.get(conf_field)
                if conf not in valid_confidence:
                    reform[conf_field] = "medium"

        # Ensure package_name has a value (use description prefix as fallback)
        if not reform.get("package_name"):
            desc = reform.get("description", "")
            reform["package_name"] = desc[:80] if desc else ""

    def _empty_result(self, country_code, country_name, survey_year):
        """Return an empty result structure."""
        return {
            "country_code": country_code,
            "country_name": country_name,
            "survey_year": survey_year,
            "reforms": [],
            "survey_summary": "No reforms identified in this survey.",
            "total_reforms": 0,
            "big_reforms_count": 0,
        }

    def _parse_json_response(self, response_text):
        """Parse JSON from LLM response, handling common formatting issues."""
        text = response_text.strip()

        # Remove markdown code fences if present
        if text.startswith("```"):
            # Remove opening fence
            first_newline = text.index("\n")
            text = text[first_newline + 1:]
            # Remove closing fence
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        # Try direct JSON parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try to find JSON object in the response
        json_match = re.search(r"\{[\s\S]*\}", text)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass

        # Try to fix common issues
        # Remove trailing commas before closing brackets
        cleaned = re.sub(r",\s*([}\]])", r"\1", text)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        logger.error(
            f"Could not parse JSON from LLM response "
            f"(first 200 chars): {text[:200]}"
        )
        return None

    def load_results(self, country_code, survey_year):
        """Load previously saved reform analysis results."""
        path = self.output_dir / f"{country_code}_{survey_year}.json"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        return None

    def get_usage_report(self):
        """Return LLM API usage report."""
        return self.llm.get_usage_summary()
