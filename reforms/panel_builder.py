"""
Panel dataset builder: constructs country×year panel from extracted reforms.

Creates the final output datasets:
1. reforms_mentions.csv  – one row per mention (raw, as extracted per survey)
2. reforms_events.csv   – one row per deduplicated real-world reform event
3. reform_panel.csv     – country×year panel built from events (for regressions)
4. summary_statistics.txt
"""

import json
import logging
import re
import time
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

from .countries import CODE_TO_NAME, get_country_list
from .llm_client import LLMClient
from .prompts import (
    CROSS_SURVEY_DEDUP_PROMPT,
    THEME_LIST,
    THEMES_SUBTHEMES,
)

logger = logging.getLogger(__name__)

# Reform themes (imported from the canonical definitions in prompts.py)
THEMES = THEME_LIST

_CROSS_SURVEY_SYSTEM_PROMPT = """\
You are a strict JSON clustering service.

Task:
- Group reform mentions that refer to the same real-world reform event.
- Return valid JSON only.
- Return exactly one top-level key: "events".
- Each event must be an object with exactly one key: "indices".
- "indices" must be an array of integer indices.
- Do not return descriptions, rationales, notes, markdown, or code fences.
"""


def _similarity(a, b):
    """Compute similarity ratio between two strings (0 to 1)."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _parse_json_response(response_text):
    """Parse JSON from LLM response, handling common formatting issues."""
    text = response_text.strip()
    if text.startswith("```"):
        first_nl = text.index("\n")
        text = text[first_nl + 1:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    json_match = re.search(r"\{[\s\S]*\}", text)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    cleaned = re.sub(r",\s*([}\]])", r"\1", text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    logger.error(f"Could not parse JSON (first 200 chars): {text[:200]}")
    return None


def _has_complete_event_coverage(result, expected_n: int) -> bool:
    """Return True when a parsed cross-dedup result covers all indices exactly once."""
    if not result or "events" not in result or not isinstance(result["events"], list):
        return False
    seen = []
    for ev in result["events"]:
        if not isinstance(ev, dict):
            return False
        indices = ev.get("indices", [])
        if not isinstance(indices, list):
            return False
        for idx in indices:
            if not isinstance(idx, int):
                return False
            if idx < 0 or idx >= expected_n:
                return False
            seen.append(idx)
    return len(seen) == expected_n and set(seen) == set(range(expected_n))


class PanelBuilder:
    """Builds panel datasets from extracted reform data."""

    def __init__(self, config):
        self.config = config
        self.reforms_dir = Path(
            config.get("paths", {}).get("reforms_json", "data/reforms_json")
        )
        self.output_dir = Path(
            config.get("paths", {}).get("output", "data/output")
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)

        year_range = config.get("year_range", {})
        self.start_year = year_range.get("start", 1995)
        self.end_year = year_range.get("end", 2025)

        # Themes to build panel columns for — defaults to all themes
        self.themes = config.get("themes") or THEME_LIST

        # Panel mode: "strict" uses only explicit years,
        # "inclusive" uses all years but flags imputed ones
        self.panel_mode = config.get("panel", {}).get("mode", "inclusive")

        # Year assignment: which date to use for the panel year
        # "implementation" (default), "legislation", "announcement"
        self.year_assignment = config.get("panel", {}).get(
            "year_assignment", "implementation"
        )

        # Major reform indicator is now driven directly by is_major_reform;
        # keep major_threshold for importance_bucket-based fallbacks only
        self.major_threshold = config.get("panel", {}).get(
            "major_reform_threshold", 3
        )

        # Cross-survey dedup similarity threshold
        self.cross_dedup_threshold = config.get("panel", {}).get(
            "cross_survey_dedup_threshold", 0.55
        )

        # Whether to use LLM for cross-survey dedup
        self.use_llm_cross_dedup = config.get("panel", {}).get(
            "llm_cross_survey_dedup", True
        )

        # Loose-year-window parameters for candidate formation.
        # When two mentions' names/descriptions are very similar (≥
        # name_match_threshold) but their imputed years are farther apart than
        # 1 year, they can still enter the same candidate component if year
        # distance ≤ loose_year_window.  This handles the case where one
        # mention has a good explicit year and another has a noisy survey-year
        # proxy.  Matches made by this looser rule still pass through
        # _cluster_by_similarity and the LLM dedup, so false positives have
        # two further gates.
        panel_cfg = config.get("panel", {})
        self.loose_year_window = panel_cfg.get("loose_year_window", 4)
        self.name_match_threshold = panel_cfg.get("name_match_threshold", 0.75)

        self._llm = None

    def _get_llm(self):
        """Lazy-init LLM client (only needed for cross-survey dedup)."""
        if self._llm is None:
            self._llm = LLMClient(self.config)
        return self._llm

    # ──────────────────────────────────────────────────────────
    # Loading
    # ──────────────────────────────────────────────────────────

    def load_all_reforms(self, countries=None):
        """Load all reform JSON files and compile into a single list.

        Returns:
            List of all reform dicts with country/year metadata.
        """
        all_reforms = []
        countries = set(countries or [])

        for json_file in sorted(self.reforms_dir.glob("*.json")):
            try:
                with open(json_file, encoding="utf-8") as f:
                    data = json.load(f)

                country_code = data.get("country_code", "")
                if countries and country_code not in countries:
                    continue
                country_name = data.get("country_name", "")
                survey_year = data.get("survey_year", 0)

                for reform in data.get("reforms", []):
                    reform["country_code"] = country_code
                    reform["country_name"] = country_name
                    reform["survey_year"] = survey_year
                    all_reforms.append(reform)

            except Exception as e:
                logger.error(f"Error loading {json_file.name}: {e}")

        logger.info(f"Loaded {len(all_reforms)} reforms from JSON files")
        return all_reforms

    # ──────────────────────────────────────────────────────────
    # Step 1: Mentions dataset (raw, one row per extraction)
    # ──────────────────────────────────────────────────────────

    def build_mentions_dataset(self, reforms=None, save=True):
        """Build the mentions-level dataset (one row per survey mention).

        Returns:
            pandas DataFrame with all mentions.
        """
        if reforms is None:
            reforms = self.load_all_reforms()

        if not reforms:
            logger.warning("No reforms to build dataset from")
            return pd.DataFrame()

        df = pd.DataFrame(reforms)

        core_columns = [
            "reform_id",
            "country_code",
            "country_name",
            "survey_year",
            "implementation_year",
            "announcement_year",
            "announcement_year_source",
            "announcement_year_confidence",
            "legislation_year",
            "legislation_year_source",
            "legislation_year_confidence",
            "implementation_year_end",
            "implementation_year_source",
            "implementation_year_confidence",
            "theme",
            "sub_theme",
            "secondary_type",
            "alternative_theme",
            "rd_actor",
            "rd_stage",
            "growth_orientation",
            "growth_orientation_rationale",
            "growth_orientation_confidence",
            "package_name",
            "component_name",
            "is_component",
            "status",
            "status_evidence",
            "status_confidence",
            "is_major_reform",
            "importance_bucket",
            "importance_rationale",
            "importance_confidence",
            "importance_rank",
            "importance_rank_rationale",
            "description",
            "source_quote",
            "source_page_start",
            "source_page_end",
        ]

        columns = [c for c in core_columns if c in df.columns]
        extra_cols = [c for c in df.columns if c not in core_columns]
        columns.extend(extra_cols)
        df = df[[c for c in columns if c in df.columns]]

        df = df.sort_values(
            ["country_code", "implementation_year", "survey_year"]
        ).reset_index(drop=True)

        if save:
            output_path = self.output_dir / "reforms_mentions.csv"
            df.to_csv(output_path, index=False, encoding="utf-8")
            print(f"  Mentions dataset: {len(df)} rows -> {output_path.name}")
        else:
            print(f"  Mentions dataset: {len(df)} rows")

        return df

    # ──────────────────────────────────────────────────────────
    # Step 2: Cross-survey event canonicalization
    # ──────────────────────────────────────────────────────────

    def canonicalize_events(self, mentions_df, save=True):
        """Deduplicate reform mentions across surveys to produce
        one row per real-world reform event.

        The same reform (e.g., France's 2023 pension reform) may appear
        in the 2023 and 2024 surveys. This step clusters those mentions
        into a single event.

        Strategy:
        1. Group by (country_code, theme, implementation_year).
        2. Within each group, use text similarity to find near-duplicates.
        3. For groups with ambiguous matches and LLM enabled, use one
           targeted LLM call to resolve.
        4. Assign a canonical event_id and keep the best metadata.

        Returns:
            Tuple of (events_df, mention_to_event mapping).
        """
        if mentions_df.empty:
            return pd.DataFrame(), {}

        # Only canonicalize implemented/legislated reforms
        actual = mentions_df[
            mentions_df["status"].isin(["implemented", "legislated"])
        ].copy()
        other = mentions_df[
            ~mentions_df["status"].isin(["implemented", "legislated"])
        ].copy()

        if actual.empty:
            return pd.DataFrame(), {}

        print(f"\n  [CROSS-SURVEY DEDUP] Canonicalizing {len(actual)} "
              f"reform mentions into events...")
        start_time = time.time()

        # ── Build candidate groups via connected components ──
        # Two mentions are candidates if: same country, year distance ≤ 1,
        # and their theme sets (primary + alternative + secondary) overlap.
        # This tolerates theme-assignment drift and ±1 year inconsistencies
        # across survey vintages; similarity clustering then resolves whether
        # candidates actually refer to the same event.
        from collections import Counter, defaultdict

        actual["_impl_year"] = actual["implementation_year"].fillna(
            actual["survey_year"]
        ).astype(int)

        idx_list = actual.index.tolist()

        def _theme_set(row):
            ts = {row["theme"]}
            alt = row.get("alternative_theme")
            if alt and pd.notna(alt):
                ts.add(alt)
            # support both old "secondary_theme" and new "secondary_type"
            for field in ("secondary_type", "secondary_theme"):
                sec = row.get(field)
                if sec and pd.notna(sec):
                    ts.add(sec)
            return ts

        country_of = {idx: actual.loc[idx, "country_code"] for idx in idx_list}
        year_of    = {idx: int(actual.loc[idx, "_impl_year"]) for idx in idx_list}
        themes_of  = {idx: _theme_set(actual.loc[idx]) for idx in idx_list}
        pkg_of = {
            idx: (actual.loc[idx, "package_name"]
                  if "package_name" in actual.columns else "")
            for idx in idx_list
        }
        desc_of = {
            idx: (actual.loc[idx, "description"]
                  if "description" in actual.columns else "")
            for idx in idx_list
        }

        # Union-Find for connected components
        parent = {idx: idx for idx in idx_list}

        def _find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def _union(x, y):
            px, py = _find(x), _find(y)
            if px != py:
                parent[px] = py

        by_country = defaultdict(list)
        for idx in idx_list:
            by_country[country_of[idx]].append(idx)

        loose_unions = 0
        for cidx_list in by_country.values():
            for i in range(len(cidx_list)):
                for j in range(i + 1, len(cidx_list)):
                    a, b = cidx_list[i], cidx_list[j]
                    year_dist = abs(year_of[a] - year_of[b])

                    # Tight rule (existing): theme overlap + year distance ≤ 1
                    if year_dist <= 1:
                        if themes_of[a] & themes_of[b]:
                            _union(a, b)
                        continue

                    # Loose rule (new): bypass hard year gate when the names
                    # or descriptions are very similar and year distance is
                    # within the configured window.  This catches cases where
                    # one mention has a good explicit year and another falls
                    # back to a survey-year proxy several vintages away.
                    if year_dist <= self.loose_year_window:
                        if themes_of[a] & themes_of[b]:
                            name_sim = max(
                                _similarity(pkg_of[a], pkg_of[b]),
                                _similarity(desc_of[a], desc_of[b]),
                            )
                            if name_sim >= self.name_match_threshold:
                                _union(a, b)
                                loose_unions += 1

        if loose_unions:
            logger.info(
                "[CROSS-SURVEY DEDUP] Loose-year rule fired %d time(s) "
                "(year_dist 2–%d, name_sim ≥ %.2f) — "
                "these pairs enter candidate components and will be "
                "re-evaluated by similarity clustering and the LLM.",
                loose_unions,
                self.loose_year_window,
                self.name_match_threshold,
            )

        comp_map = defaultdict(list)
        for idx in idx_list:
            comp_map[_find(idx)].append(idx)

        events = []
        mention_to_event = {}  # mention index -> event_id
        event_counter = 0
        llm_calls = 0

        multi_mention_groups = 0
        for comp_indices in comp_map.values():
            group_df = actual.loc[comp_indices]
            country = country_of[comp_indices[0]]

            # Canonical year = mode of impl_year across mentions in component
            comp_year = Counter(
                year_of[i] for i in comp_indices
            ).most_common(1)[0][0]
            # Primary theme = most common primary theme across mentions
            comp_theme = Counter(
                actual.loc[i, "theme"] for i in comp_indices
            ).most_common(1)[0][0]

            if len(comp_indices) == 1:
                # Single mention -> single event, no dedup needed
                event_counter += 1
                event_id = f"EVT_{country}_{comp_year}_{event_counter:04d}"
                row = group_df.iloc[0].to_dict()
                row["event_id"] = event_id
                row["n_mentions"] = 1
                row["mention_survey_years"] = str(row.get("survey_year", ""))
                row["mention_ids"] = row.get("reform_id", "")
                events.append(row)
                mention_to_event[comp_indices[0]] = event_id
                continue

            multi_mention_groups += 1

            # Multiple mentions: cluster by text similarity first
            descs = [
                group_df.loc[idx, "description"]
                if "description" in group_df.columns
                else ""
                for idx in comp_indices
            ]
            pkg_names = [
                group_df.loc[idx, "package_name"]
                if "package_name" in group_df.columns
                else ""
                for idx in comp_indices
            ]

            clusters = self._cluster_by_similarity(
                comp_indices, descs, pkg_names
            )

            # If LLM cross-dedup is enabled and there are many items,
            # use LLM to refine the clustering
            if (self.use_llm_cross_dedup
                    and len(comp_indices) > 2
                    and len(clusters) > 1):
                country_name = CODE_TO_NAME.get(country, country)
                clusters = self._llm_cross_dedup(
                    group_df, comp_indices, clusters,
                    country_name, comp_theme
                )
                llm_calls += 1

            # Build events from clusters
            for cluster_indices in clusters:
                event_counter += 1
                # Canonical year for this cluster = mode of impl_year
                cluster_year = Counter(
                    year_of[i] for i in cluster_indices
                ).most_common(1)[0][0]
                event_id = f"EVT_{country}_{cluster_year}_{event_counter:04d}"

                cluster_rows = group_df.loc[cluster_indices]
                best_idx = self._pick_best_event_representative(
                    group_df, cluster_indices, cluster_year
                )
                row = group_df.loc[best_idx].to_dict()
                row["event_id"] = event_id
                row["n_mentions"] = len(cluster_indices)
                survey_years = sorted(
                    cluster_rows["survey_year"].unique().tolist()
                )
                row["mention_survey_years"] = ",".join(
                    str(y) for y in survey_years
                )
                # Track which mention IDs contributed to this event
                mention_id_list = []
                for idx in cluster_indices:
                    mid = group_df.loc[idx].get("reform_id", "")
                    if mid:
                        mention_id_list.append(str(mid))
                row["mention_ids"] = ",".join(mention_id_list)

                # is_major_reform is canonical: True if any mention has it
                if "is_major_reform" in cluster_rows.columns and cluster_rows["is_major_reform"].any():
                    row["is_major_reform"] = True
                    row["importance_bucket"] = 3
                elif "importance_bucket" in cluster_rows.columns:
                    # Non-major: take highest bucket as soft supporting metadata
                    max_imp = cluster_rows["importance_bucket"].max()
                    if max_imp > row.get("importance_bucket", 0):
                        row["importance_bucket"] = int(max_imp)

                # Prefer explicit year source across all year fields
                for src_col, conf_col in (
                    ("implementation_year_source",
                     "implementation_year_confidence"),
                    ("announcement_year_source",
                     "announcement_year_confidence"),
                    ("legislation_year_source",
                     "legislation_year_confidence"),
                ):
                    if src_col in cluster_rows.columns:
                        sources = cluster_rows[src_col].dropna().tolist()
                        if "explicit" in sources:
                            row[src_col] = "explicit"
                            row[conf_col] = "high"

                # For growth orientation, take the most common non-neutral value
                if "growth_orientation" in cluster_rows.columns:
                    orientations = (
                        cluster_rows["growth_orientation"].dropna().tolist()
                    )
                    non_neutral = [
                        o for o in orientations
                        if o != "unclear_or_neutral"
                    ]
                    if non_neutral:
                        row["growth_orientation"] = Counter(
                            non_neutral
                        ).most_common(1)[0][0]

                events.append(row)
                for idx in cluster_indices:
                    mention_to_event[idx] = event_id

        # Also pass through announced as events (no dedup)
        for idx, row_data in other.iterrows():
            event_counter += 1
            row = row_data.to_dict()
            impl_year = row.get("implementation_year") or row.get(
                "survey_year", 0
            )
            country = row.get("country_code", "XXX")
            event_id = f"EVT_{country}_{impl_year}_{event_counter:04d}"
            row["event_id"] = event_id
            row["n_mentions"] = 1
            row["mention_survey_years"] = str(row.get("survey_year", ""))
            row["mention_ids"] = row.get("reform_id", "")
            events.append(row)
            mention_to_event[idx] = event_id

        events_df = pd.DataFrame(events)

        # Clean up temp column
        if "_impl_year" in events_df.columns:
            events_df.drop(columns=["_impl_year"], inplace=True)

        elapsed = time.time() - start_time
        n_actual_events = len(
            events_df[
                events_df["status"].isin(["implemented", "legislated"])
            ]
        )
        print(f"  [CROSS-SURVEY DEDUP] {len(actual)} mentions -> "
              f"{n_actual_events} events "
              f"({multi_mention_groups} groups checked, "
              f"{llm_calls} LLM calls, {elapsed:.1f}s)")

        # Save events dataset
        events_df = events_df.sort_values(
            ["country_code", "implementation_year", "survey_year"]
        ).reset_index(drop=True)

        if save:
            events_path = self.output_dir / "reforms_events.csv"
            events_df.to_csv(events_path, index=False, encoding="utf-8")
            print(f"  Events dataset: {len(events_df)} rows -> "
                  f"{events_path.name}")
        else:
            print(f"  Events dataset: {len(events_df)} rows")

        return events_df, mention_to_event

    def _cluster_by_similarity(self, indices, descriptions, package_names):
        """Cluster indices by text similarity on descriptions/package_names.

        Returns list of lists of indices (each inner list is a cluster).
        """
        n = len(indices)
        assigned = [False] * n
        clusters = []

        for i in range(n):
            if assigned[i]:
                continue
            cluster = [indices[i]]
            assigned[i] = True

            for j in range(i + 1, n):
                if assigned[j]:
                    continue

                desc_sim = _similarity(descriptions[i], descriptions[j])
                pkg_sim = _similarity(package_names[i], package_names[j])
                best = max(desc_sim, pkg_sim)

                if best >= self.cross_dedup_threshold:
                    cluster.append(indices[j])
                    assigned[j] = True

            clusters.append(cluster)

        return clusters

    def _pick_best_event_representative(self, group_df, cluster_indices, cluster_year):
        """Choose the best mention row to represent an event cluster."""
        status_rank = {"implemented": 2, "legislated": 1, "announced": 0}

        def _score(idx):
            row = group_df.loc[idx]
            survey_year = row.get("survey_year")
            year_distance = (
                abs(float(survey_year) - float(cluster_year))
                if pd.notna(survey_year) else 999
            )
            impl_src = str(row.get("implementation_year_source") or "")
            impl_src_rank = 1 if impl_src == "explicit" else 0
            status = str(row.get("status") or "")
            status_score = status_rank.get(status, -1)
            quote_len = len(str(row.get("source_quote") or "").strip())
            desc_len = len(str(row.get("description") or "").strip())
            return (
                year_distance,
                -impl_src_rank,
                -status_score,
                -quote_len,
                -desc_len,
                idx,
            )

        return min(cluster_indices, key=_score)

    def _llm_cross_dedup(self, group_df, group_indices, initial_clusters,
                         country_name, theme):
        """Use one LLM call to refine cross-survey clustering."""
        desc_lines = []
        idx_map = {}  # prompt index -> dataframe index
        for prompt_i, df_idx in enumerate(group_indices):
            row = group_df.loc[df_idx]
            desc = row.get("description", "")
            sy = row.get("survey_year", "?")
            status = row.get("status", "?")
            pkg = row.get("package_name", "")
            desc_lines.append(
                f"[{prompt_i}] (survey={sy}, status={status}, "
                f"package=\"{pkg}\") {desc}"
            )
            idx_map[prompt_i] = df_idx

        prompt = CROSS_SURVEY_DEDUP_PROMPT.format(
            country=country_name,
            theme=theme,
            descriptions="\n".join(desc_lines),
            max_index=len(group_indices) - 1,
        )

        try:
            llm = self._get_llm()
            max_tokens = min(5000, max(600, len(group_indices) * 12))
            response = llm.call(
                _CROSS_SURVEY_SYSTEM_PROMPT,
                prompt,
                max_tokens=max_tokens,
                operation=LLMClient.OP_CROSS_SURVEY_DEDUP,
                json_mode=(llm.provider == "openai"),
            )
            result = _parse_json_response(response)

            if not _has_complete_event_coverage(result, len(group_indices)):
                repair_prompt = (
                    "Repair the following malformed or schema-invalid JSON so it becomes a valid JSON object "
                    "with a top-level key `events`. Preserve the intended grouping if possible. "
                    "Each event must contain only an `indices` array of integers. "
                    f"Every index from 0 to {len(group_indices) - 1} must appear exactly once in the repaired output. "
                    "Return valid JSON only.\n\n"
                    f"JSON to repair:\n{response}"
                )
                repaired = llm.call(
                    _CROSS_SURVEY_SYSTEM_PROMPT,
                    repair_prompt,
                    max_tokens=max_tokens,
                    operation=LLMClient.OP_CROSS_SURVEY_DEDUP,
                    json_mode=(llm.provider == "openai"),
                )
                result = _parse_json_response(repaired)

            if not _has_complete_event_coverage(result, len(group_indices)):
                logger.warning(
                    "Cross-survey dedup JSON invalid after automatic repair; "
                    "falling back to initial clusters for %s / %s (%d mentions).",
                    country_name, theme, len(group_indices),
                )
                return initial_clusters

            # Convert prompt indices back to dataframe indices
            refined_clusters = []
            for ev in result["events"]:
                cluster = [idx_map[pi] for pi in ev["indices"]]
                refined_clusters.append(cluster)

            return refined_clusters

        except Exception as e:
            logger.error(f"LLM cross-survey dedup error: {e}")
            return initial_clusters

    # ──────────────────────────────────────────────────────────
    # Step 3: Panel construction
    # ──────────────────────────────────────────────────────────

    def _resolve_panel_year(self, row):
        """Determine which year to assign this event in the panel,
        based on the year_assignment config setting.

        Falls back to implementation_year, then survey_year.
        """
        if self.year_assignment == "announcement":
            year = row.get("announcement_year")
            if year and not pd.isna(year):
                return int(year)
        elif self.year_assignment == "legislation":
            year = row.get("legislation_year")
            if year and not pd.isna(year):
                return int(year)

        # Default: implementation year, then survey year
        year = row.get("implementation_year")
        if year and not pd.isna(year):
            return int(year)
        year = row.get("survey_year")
        if year and not pd.isna(year):
            return int(year)
        return 0

    def build_panel_dataset(self, events_df, countries=None, save=True):
        """Build the country×year panel dataset with reform indicators.

        Uses the events dataset (not mentions) to avoid double-counting
        the same reform across surveys.

        Produces indicators at both theme and sub_theme level.

        Args:
            events_df: DataFrame from canonicalize_events().

        Returns:
            pandas DataFrame with the panel.
        """
        if events_df.empty:
            logger.warning("No events data for panel construction")
            return pd.DataFrame()

        events_df = events_df.copy()

        # Resolve panel year based on year_assignment config
        events_df["panel_year"] = events_df.apply(
            self._resolve_panel_year, axis=1
        )

        # Filter to year range
        events_df = events_df[
            (events_df["panel_year"] >= self.start_year)
            & (events_df["panel_year"] <= self.end_year)
        ]

        # ── Split by status for different indicator sets ──
        # Core reforms: implemented + legislated (the main DV)
        actual = events_df[
            events_df["status"].isin(["implemented", "legislated"])
        ].copy()

        # Announced: separate indicators (government intent, not law yet)
        announced = events_df[
            events_df["status"] == "announced"
        ].copy()

        # In strict mode, exclude events whose panel year was imputed.
        # The source column checked depends on year_assignment: we evaluate
        # quality against the year actually used (legislation_year_source,
        # announcement_year_source, or implementation_year_source).
        # When the preferred year was unavailable and _resolve_panel_year fell
        # back to implementation_year, we check implementation_year_source.
        if self.panel_mode == "strict":
            _src_col = {
                "implementation": "implementation_year_source",
                "legislation":    "legislation_year_source",
                "announcement":   "announcement_year_source",
            }.get(self.year_assignment, "implementation_year_source")
            _year_col = {
                "implementation": "implementation_year",
                "legislation":    "legislation_year",
                "announcement":   "announcement_year",
            }.get(self.year_assignment, "implementation_year")

            before = len(actual)
            if _src_col in actual.columns and _year_col in actual.columns:
                # Rows where the preferred year was available: check its source.
                # Rows where it was missing (fallback): check impl source.
                used_primary = actual[_year_col].notna()
                active_source = pd.Series(index=actual.index, dtype=object)
                active_source[used_primary] = actual.loc[
                    used_primary, _src_col
                ]
                if "implementation_year_source" in actual.columns:
                    active_source[~used_primary] = actual.loc[
                        ~used_primary, "implementation_year_source"
                    ]
                actual = actual[active_source != "imputed_survey_year"]
            elif "implementation_year_source" in actual.columns:
                # Fallback: column for active assignment absent, use impl source
                actual = actual[
                    actual["implementation_year_source"] != "imputed_survey_year"
                ]
            dropped = before - len(actual)
            if dropped:
                print(f"  [PANEL] Strict mode: excluded {dropped} events "
                      f"with imputed {self.year_assignment} years")

        # Build panel skeleton
        countries = (
            [(code, name) for code, name in get_country_list()
             if not countries or code in set(countries)]
        )
        years = range(self.start_year, self.end_year + 1)

        panel_rows = []
        for code, name in countries:
            for year in years:
                panel_rows.append({
                    "country_code": code,
                    "country_name": name,
                    "year": year,
                })
        panel = pd.DataFrame(panel_rows)

        # ── Theme-level indicators ──
        panel = self._add_theme_indicators(panel, actual, "")

        # ── Major reform indicators (is_major_reform == True) ──
        if "is_major_reform" in actual.columns:
            major = actual[actual["is_major_reform"] == True].copy()  # noqa
        else:
            major = actual[
                actual["importance_bucket"] >= self.major_threshold
            ].copy()
        panel = self._add_theme_indicators(panel, major, "major_")

        # ── Growth-orientation-specific indicators ──
        if "growth_orientation" in actual.columns:
            growth_sup = actual[
                actual["growth_orientation"] == "growth_supporting"
            ]
            growth_hind = actual[
                actual["growth_orientation"] == "growth_hindering"
            ]
            panel = self._add_theme_indicators(
                panel, growth_sup, "growth_supporting_"
            )
            panel = self._add_theme_indicators(
                panel, growth_hind, "growth_hindering_"
            )

        # ── Announced reform indicators (separate set) ──
        if not announced.empty:
            ann_any = (
                announced.groupby(["country_code", "panel_year"])
                .size()
                .reset_index(name="announced_count")
            )
            ann_any.rename(columns={"panel_year": "year"}, inplace=True)
            panel = panel.merge(
                ann_any, on=["country_code", "year"], how="left"
            )
            panel["announced_count"] = (
                panel["announced_count"].fillna(0).astype(int)
            )

        # ── Sub-theme level indicators ──
        sub_theme_panel = self._build_sub_theme_panel(actual, countries, years)

        # ── rd_actor and rd_stage breakdown columns ──
        panel = self._add_dimension_indicators(panel, actual, "rd_actor",
            ["public", "private", "public_private"])
        panel = self._add_dimension_indicators(panel, actual, "rd_stage",
            ["basic", "applied", "commercialization", "adoption"])

        # ── Reform intensity score (composite, 0–1) ──
        # Combines: reform count (capped), share of growth-supporting reforms,
        # share of major reforms, and sub_theme diversity (breadth).
        # Useful as a single LHS / RHS variable in growth regressions.
        panel = self._add_intensity_score(panel, actual)

        # Sort and save
        panel = panel.sort_values(
            ["country_code", "year"]
        ).reset_index(drop=True)

        if save:
            panel_path = self.output_dir / "reform_panel.csv"
            panel.to_csv(panel_path, index=False, encoding="utf-8")
            print(f"  Panel dataset (theme-level): "
                  f"{len(panel)} rows -> {panel_path.name}")

            if not sub_theme_panel.empty:
                sub_path = self.output_dir / "reform_panel_subtheme.csv"
                sub_theme_panel.to_csv(sub_path, index=False, encoding="utf-8")
                print(f"  Panel dataset (sub-theme-level): "
                      f"{len(sub_theme_panel)} rows -> {sub_path.name}")

            # Try to save Excel versions
            try:
                xlsx_path = self.output_dir / "reform_panel.xlsx"
                panel.to_excel(xlsx_path, index=False)
            except Exception as e:
                logger.warning(f"Could not save Excel file: {e}")
        else:
            print(f"  Panel dataset (theme-level): {len(panel)} rows")
            if not sub_theme_panel.empty:
                print(f"  Panel dataset (sub-theme-level): {len(sub_theme_panel)} rows")

        return panel, sub_theme_panel

    def _add_theme_indicators(self, panel, events_subset, prefix):
        """Add count + binary indicators at the overall and per-theme level.

        Args:
            panel: The panel DataFrame to merge into.
            events_subset: Filtered events (e.g., all actual, or just major).
            prefix: Column name prefix (e.g., "major_", "lib_", "").
        """
        if events_subset.empty:
            # Add zero columns
            panel[f"{prefix}reform_count"] = 0
            panel[f"{prefix}has_reform"] = 0
            for theme in self.themes:
                panel[f"{prefix}{theme}_count"] = 0
                panel[f"{prefix}has_{theme}"] = 0
            return panel

        # Overall counts
        any_c = (
            events_subset.groupby(["country_code", "panel_year"])
            .size()
            .reset_index(name=f"{prefix}reform_count")
        )
        any_c.rename(columns={"panel_year": "year"}, inplace=True)
        panel = panel.merge(
            any_c, on=["country_code", "year"], how="left"
        )
        panel[f"{prefix}reform_count"] = (
            panel[f"{prefix}reform_count"].fillna(0).astype(int)
        )
        panel[f"{prefix}has_reform"] = (
            (panel[f"{prefix}reform_count"] > 0).astype(int)
        )

        # Per-theme counts
        for theme in self.themes:
            theme_df = events_subset[events_subset["theme"] == theme]
            if theme_df.empty:
                panel[f"{prefix}{theme}_count"] = 0
                panel[f"{prefix}has_{theme}"] = 0
                continue
            tc = (
                theme_df.groupby(["country_code", "panel_year"])
                .size()
                .reset_index(name=f"{prefix}{theme}_count")
            )
            tc.rename(columns={"panel_year": "year"}, inplace=True)
            panel = panel.merge(
                tc, on=["country_code", "year"], how="left"
            )
            panel[f"{prefix}{theme}_count"] = (
                panel[f"{prefix}{theme}_count"].fillna(0).astype(int)
            )
            panel[f"{prefix}has_{theme}"] = (
                (panel[f"{prefix}{theme}_count"] > 0).astype(int)
            )

        return panel

    def _add_dimension_indicators(self, panel, actual, dim_col, categories):
        """Add per-category count columns for an analytical dimension
        (rd_actor or rd_stage).
        """
        if actual.empty or dim_col not in actual.columns:
            for cat in categories:
                panel[f"{dim_col}_{cat}_count"] = 0
            return panel

        for cat in categories:
            cat_df = actual[actual[dim_col] == cat]
            if cat_df.empty:
                panel[f"{dim_col}_{cat}_count"] = 0
                continue
            counts = (
                cat_df.groupby(["country_code", "panel_year"])
                .size()
                .reset_index(name=f"{dim_col}_{cat}_count")
            )
            counts.rename(columns={"panel_year": "year"}, inplace=True)
            panel = panel.merge(counts, on=["country_code", "year"], how="left")
            panel[f"{dim_col}_{cat}_count"] = (
                panel[f"{dim_col}_{cat}_count"].fillna(0).astype(int)
            )
        return panel

    def _add_intensity_score(self, panel, actual):
        """Add a composite reform_intensity_score (0–1) per country-year.

        Methodology:
          Component 1 — volume:   log(1 + reform_count) / log(1 + cap)     [0-1]
          Component 2 — quality:  share of growth_supporting among all      [0-1]
          Component 3 — depth:    share of major (is_major_reform=True)     [0-1]
          Component 4 — breadth:  sub_theme diversity / total sub_themes    [0-1]

        Composite = mean of available components.
        Score is zero if no reforms present.
        Useful as a single indicator variable for regressions.
        """
        COUNT_CAP = 10          # log scale cap (counts above this give score ≈ 1)
        N_SUBTHEMES = 8         # total innovation sub_themes

        if actual.empty:
            panel["reform_intensity_score"] = 0.0
            panel["reform_intensity_components"] = ""
            return panel

        # Per country-year aggregations
        agg = (
            actual.groupby(["country_code", "panel_year"])
            .apply(lambda g: pd.Series({
                "n_reforms": len(g),
                "n_growth_supporting": (
                    (g.get("growth_orientation", pd.Series()) == "growth_supporting")
                    .sum() if "growth_orientation" in g.columns else 0
                ),
                "n_major": (
                    g["is_major_reform"].sum()
                    if "is_major_reform" in g.columns else 0
                ),
                "n_subtypes": (
                    g["sub_theme"].nunique()
                    if "sub_theme" in g.columns else 0
                ),
            }), include_groups=False)
            .reset_index()
        )
        agg.rename(columns={"panel_year": "year"}, inplace=True)

        agg["c_volume"]  = (
            (agg["n_reforms"].clip(upper=COUNT_CAP).apply(
                lambda x: __import__("math").log1p(x)
            )) / __import__("math").log1p(COUNT_CAP)
        )
        agg["c_quality"] = agg.apply(
            lambda r: r["n_growth_supporting"] / r["n_reforms"] if r["n_reforms"] > 0 else 0,
            axis=1,
        )
        agg["c_depth"]   = agg.apply(
            lambda r: min(r["n_major"] / r["n_reforms"], 1.0) if r["n_reforms"] > 0 else 0,
            axis=1,
        )
        agg["c_breadth"] = agg["n_subtypes"] / N_SUBTHEMES

        agg["reform_intensity_score"] = (
            (agg["c_volume"] + agg["c_quality"] + agg["c_depth"] + agg["c_breadth"]) / 4
        ).round(4)

        panel = panel.merge(
            agg[["country_code", "year", "reform_intensity_score"]],
            on=["country_code", "year"], how="left",
        )
        panel["reform_intensity_score"] = panel["reform_intensity_score"].fillna(0.0)
        return panel

    def _build_sub_theme_panel(self, actual_events, countries, years):
        """Build a country×year×sub_theme panel dataset.

        This is the panel that provides the most variation for regressions:
        rather than asking "did France have any product_market reform in 2019?"
        (almost always yes), it asks "did France have a competition_policy
        reform in 2019?" (much more selective).

        The skeleton is built from the full canonical taxonomy (THEMES_SUBTHEMES)
        so the panel structure is fixed and independent of what the LLM happened
        to extract. Events whose sub_theme is not in the taxonomy are excluded
        from counts and logged as warnings.

        Returns a long-format DataFrame:
            country_code, year, theme, sub_theme,
            has_reform, reform_count, has_major, major_count,
            has_growth_supporting, growth_supporting_count,
            has_growth_hindering, growth_hindering_count
        """
        if actual_events.empty:
            return pd.DataFrame()

        if "sub_theme" not in actual_events.columns:
            return pd.DataFrame()

        # ── Build canonical (theme, sub_theme) pairs from the fixed taxonomy ──
        # Filtered to only the themes configured for this run
        canonical_pairs = [
            (theme_key, st_key)
            for theme_key, theme_info in THEMES_SUBTHEMES.items()
            if theme_key in self.themes
            for st_key in theme_info["subthemes"]
        ]
        canonical_st_set = {st for _, st in canonical_pairs}

        # Warn about sub-themes that are not in the canonical vocabulary
        if actual_events["sub_theme"].notna().any():
            observed = set(
                actual_events["sub_theme"].dropna().unique()
            ) - {""}
            unknown = observed - canonical_st_set
            if unknown:
                logger.warning(
                    "Sub-theme label drift detected — the following "
                    "sub_theme values are not in the taxonomy and will be "
                    "excluded from the sub-theme panel: %s",
                    sorted(unknown),
                )

        # Build skeleton: all country × year × canonical-sub_theme combinations
        skeleton_rows = []
        for code, name in countries:
            for year in years:
                for theme_key, st_key in canonical_pairs:
                    skeleton_rows.append({
                        "country_code": code,
                        "country_name": name,
                        "year": year,
                        "theme": theme_key,
                        "sub_theme": st_key,
                    })
        skeleton = pd.DataFrame(skeleton_rows)

        # Pre-aggregate actual events by (country, year, sub_theme)
        has_orientation = "growth_orientation" in actual_events.columns
        group_key = ["country_code", "panel_year", "sub_theme"]

        # Total count
        counts = (
            actual_events.groupby(group_key)
            .size()
            .reset_index(name="reform_count")
        )
        counts.rename(columns={"panel_year": "year"}, inplace=True)

        # Major count (based on is_major_reform flag)
        if "is_major_reform" in actual_events.columns:
            major_events = actual_events[
                actual_events["is_major_reform"] == True  # noqa
            ]
        else:
            major_events = actual_events[
                actual_events["importance_bucket"] >= self.major_threshold
            ]
        major_counts = (
            major_events.groupby(group_key)
            .size()
            .reset_index(name="major_count")
        )
        major_counts.rename(columns={"panel_year": "year"}, inplace=True)

        # Growth-orientation counts
        if has_orientation:
            growth_sup_events = actual_events[
                actual_events["growth_orientation"] == "growth_supporting"
            ]
            growth_hind_events = actual_events[
                actual_events["growth_orientation"] == "growth_hindering"
            ]
            growth_sup_counts = (
                growth_sup_events.groupby(group_key)
                .size()
                .reset_index(name="growth_supporting_count")
            )
            growth_sup_counts.rename(
                columns={"panel_year": "year"}, inplace=True
            )
            growth_hind_counts = (
                growth_hind_events.groupby(group_key)
                .size()
                .reset_index(name="growth_hindering_count")
            )
            growth_hind_counts.rename(
                columns={"panel_year": "year"}, inplace=True
            )

        # Merge everything into the skeleton
        merge_on = ["country_code", "year", "sub_theme"]
        sub_panel = skeleton.merge(counts, on=merge_on, how="left")
        sub_panel = sub_panel.merge(major_counts, on=merge_on, how="left")

        if has_orientation:
            sub_panel = sub_panel.merge(
                growth_sup_counts, on=merge_on, how="left"
            )
            sub_panel = sub_panel.merge(
                growth_hind_counts, on=merge_on, how="left"
            )
        else:
            sub_panel["growth_supporting_count"] = 0
            sub_panel["growth_hindering_count"] = 0

        # Fill NaN with 0 and create binary indicators
        for col in ["reform_count", "major_count",
                    "growth_supporting_count", "growth_hindering_count"]:
            sub_panel[col] = sub_panel[col].fillna(0).astype(int)

        sub_panel["has_reform"] = (sub_panel["reform_count"] > 0).astype(int)
        sub_panel["has_major"] = (sub_panel["major_count"] > 0).astype(int)
        sub_panel["has_growth_supporting"] = (
            sub_panel["growth_supporting_count"] > 0
        ).astype(int)
        sub_panel["has_growth_hindering"] = (
            sub_panel["growth_hindering_count"] > 0
        ).astype(int)

        # Reorder columns
        col_order = [
            "country_code", "country_name", "year", "theme", "sub_theme",
            "has_reform", "reform_count", "has_major", "major_count",
            "has_growth_supporting", "growth_supporting_count",
            "has_growth_hindering", "growth_hindering_count",
        ]
        sub_panel = sub_panel[[c for c in col_order if c in sub_panel.columns]]

        sub_panel = sub_panel.sort_values(
            ["country_code", "year", "theme", "sub_theme"]
        ).reset_index(drop=True)

        return sub_panel

    # ──────────────────────────────────────────────────────────
    # Step 4: Recommendations
    # ──────────────────────────────────────────────────────────

    # ──────────────────────────────────────────────────────────
    # Main entry point
    # ──────────────────────────────────────────────────────────

    def build_all_datasets(self, countries=None, save=True):
        """Build all output datasets.

        Pipeline:
        1. Load all survey-level JSONs -> mentions
        2. Cross-survey dedup -> events
        3. Build panel from events

        Returns:
            Dict with DataFrames.
        """
        print("\n  Building output datasets...")
        print(f"  Panel mode: {self.panel_mode}, "
              f"year_assignment: {self.year_assignment}")

        # Step 1: Mentions
        mentions_df = self.build_mentions_dataset(
            reforms=self.load_all_reforms(countries=countries),
            save=save,
        )
        if mentions_df.empty:
            return {
                "mentions": mentions_df,
                "events": pd.DataFrame(),
                "panel": pd.DataFrame(),
                "subtheme_panel": pd.DataFrame(),
            }

        # Step 2: Cross-survey canonicalization
        events_df, mention_map = self.canonicalize_events(mentions_df, save=save)

        # Step 3: Panel
        panel_df, subtheme_panel_df = self.build_panel_dataset(
            events_df, countries=countries, save=save
        )

        # Summary
        if save:
            self._generate_summary(mentions_df, events_df, panel_df)

        return {
            "mentions": mentions_df,
            "events": events_df,
            "panel": panel_df,
            "subtheme_panel": subtheme_panel_df,
        }

    def _generate_summary(self, mentions_df, events_df, panel_df):
        """Generate and save summary statistics."""
        if events_df.empty:
            return

        actual_events = events_df[
            events_df["status"].isin(["implemented", "legislated"])
        ]

        summary_lines = [
            "OECD Reform Extraction - Summary Statistics",
            "=" * 50,
            "",
            f"Total mentions (across all surveys): {len(mentions_df)}",
            f"Unique reform events (after cross-survey dedup): "
            f"{len(actual_events)}",
            f"Countries covered: {events_df['country_code'].nunique()}",
            "",
            "Events by theme:",
        ]

        if "theme" in actual_events.columns:
            theme_counts = actual_events["theme"].value_counts()
            for theme, count in theme_counts.items():
                summary_lines.append(f"  {theme}: {count}")

        # Sub-theme breakdown
        if "sub_theme" in actual_events.columns:
            summary_lines.extend(["", "Top sub-themes:"])
            st_counts = (
                actual_events["sub_theme"]
                .fillna("")
                .value_counts()
                .head(15)
            )
            for st, count in st_counts.items():
                if st:
                    summary_lines.append(f"  {st}: {count}")

        summary_lines.extend(["", "Events by status:"])
        if "status" in events_df.columns:
            status_counts = events_df["status"].value_counts()
            for status, count in status_counts.items():
                summary_lines.append(f"  {status}: {count}")

        # Growth orientation breakdown
        if "growth_orientation" in actual_events.columns:
            summary_lines.extend(["", "Events by growth orientation:"])
            orient_counts = (
                actual_events["growth_orientation"].value_counts()
            )
            for orientation, count in orient_counts.items():
                summary_lines.append(f"  {orientation}: {count}")

        if "is_major_reform" in actual_events.columns:
            big = actual_events[
                actual_events["is_major_reform"] == True  # noqa
            ]
            summary_lines.extend([
                "",
                f"Major reform events (is_major_reform=True): {len(big)}",
            ])

        if "importance_bucket" in actual_events.columns:
            summary_lines.append("")
            summary_lines.append("Events by importance bucket:")
            imp_counts = (
                actual_events["importance_bucket"].value_counts().sort_index()
            )
            for level, count in imp_counts.items():
                summary_lines.append(f"  Bucket {level}: {count}")

        if "implementation_year_source" in actual_events.columns:
            summary_lines.append("")
            summary_lines.append("Year source breakdown:")
            src_counts = (
                actual_events["implementation_year_source"]
                .fillna("unknown")
                .value_counts()
            )
            for src, count in src_counts.items():
                summary_lines.append(f"  {src}: {count}")

        if not panel_df.empty:
            reform_years = panel_df[panel_df["has_reform"] == 1]
            total_cy = len(panel_df)
            reform_cy = len(reform_years)
            summary_lines.extend([
                "",
                f"Panel: {total_cy} country-year observations",
                f"Country-years with at least one reform: {reform_cy} "
                f"({100 * reform_cy / total_cy:.1f}%)",
                f"Panel mode: {self.panel_mode}",
                f"Year assignment: {self.year_assignment}",
            ])

        summary_text = "\n".join(summary_lines)
        summary_path = self.output_dir / "summary_statistics.txt"
        with open(summary_path, "w") as f:
            f.write(summary_text)

        logger.info(f"Summary saved: {summary_path}")
        print("\n" + summary_text)
