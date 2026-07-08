"""
Apply hand-verified `action == "reclassify"` gap-report matches for France,
Portugal, and Luxembourg into their canonical detail series.

Context
-------
gap_detector.py's `search_raw_rows_for_gaps` (and its docx_results.csv
fallback, `_results_gap_diagnosis_from_country_results`) already identifies
missing agency-years where a plausible matching row exists somewhere in the
pipeline's own extracted output (raw_rows.csv or <country>_docx_results.csv),
tagging the gap-report row `action="reclassify"`. But nothing ever *applies*
that match — `gap_review_apply.py` only consumes `<country>_gap_review.csv`,
which is produced by an LLM pass (`gap_review.py`) that only reviews
`action=="verify"` rows, never `action=="reclassify"` ones. So these
already-identified matches sit flagged forever with no path into the series.

This script closes that gap for France/Portugal/Luxembourg specifically,
using a hand-vetted accept list (not a blind bulk-apply of every
`action=="reclassify"` row) — manual auditing of every current
reclassify/verify candidate in these three countries turned up genuine
false positives that a naive apply would have inserted as data errors:

  - Portugal FCT 2021/2022: the matched raw row is a transfer *from* FCT's
    budget to another fund ("Transferência de verbas ... da FCT" /
    "... para o Fundo de Contragarantia Mútuo ... provenientes do orçamento
    da FCT"), not FCT's own appropriation. Two orders of magnitude smaller
    than neighbouring years — a real tell.
  - Portugal LNEC 2000: matched row is a one-off consulting/procurement line
    ("AQUISIÇÃO DE SERVIÇOS DESTINADA À REFORMULAÇÃO INSTITUCIONAL DO LNEC"),
    not LNEC's institutional appropriation.
  - France 2005 "Universities and Higher Education (Pre-LOLF Chapter)": the
    matched docx_results.csv rows are actually Research-chapter lines
    ("Recherche"/"Research") that canonical_series.py's own real matcher
    would exclude via that canonical's `exclude_match_groups` rule — a false
    positive from gap_detector.py's simpler diagnostic matcher, now fixed
    upstream in gap_detector.py itself (see accompanying diff) so it won't
    recur, but excluded here too for safety.
  - Portugal FCT 2004 and ANI 2024: downgraded to "needs verification" rather
    than applied — FCT 2004's raw amount is duplicated byte-for-byte against
    an unrelated transfer line to a different institution (Instituto
    Politécnico de Bragança) in the same document, suggesting a possible
    table-parsing artifact rather than two genuinely equal figures; ANI 2024
    shows an 11-20x jump versus the last known year with the matched amount
    coincidentally equal to a same-document "ministry total" line, which is
    internally inconsistent (an agency total can't equal its parent
    ministry's total when other agencies/programmes also draw on it).

Where accepted, amounts pulled from docx_results.csv are unit-normalized
(unit=="thousand" -> x1000) before insertion, matching the fix now also
applied to gap_detector.py's own diagnosis text.

Usage:
  python reclassify_apply_fpl.py            # apply + rebuild
  python reclassify_apply_fpl.py --dry-run  # show what would change, no writes
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from budget import config as cfg
from budget.canonical_series import build_totals_series
from budget.compile import build_combined_database
from budget.manual_curation import is_locked_observation

OUTPUT_DIR = cfg.OUTPUT_DIR

# (country, year, canonical_name, amount_local, unit, currency, source_file,
#  page_number, line_description_en, note)
ACCEPTED: list[tuple] = [
    # --- France: LOLF-era programme/mission totals genuinely present in
    # france_docx_results.csv (decision="review", but confirmed via magnitude
    # continuity with neighbouring years after unit normalization: thousand -> unit) ---
    ("France", 2014, "Research and Higher Education in Economic and Industrial Matters",
     984169961.0, "euro", "EUR", "JORF_2014.pdf", "110",
     "Recherche et enseignement supérieur en matière économique et industrielle",
     "reclassify: docx_results row, unit-normalized x1000, confirmed vs. neighbours (2013 ~1.09B / 2015 ~878M)"),
    ("France", 2015, "Research and Higher Education in Economic and Industrial Matters",
     877712013.0, "euro", "EUR", "JORF_2015.pdf", "88",
     "Recherche et enseignement supérieur en matière économique et industrielle",
     "reclassify: docx_results row, unit-normalized x1000, confirmed vs. neighbours (2014 ~984M)"),
    ("France", 2017, "Research and Higher Education in Economic and Industrial Matters",
     794609301.0, "euro", "EUR", "JORF_2017.pdf", "109",
     "Recherche et enseignement supérieur en matière économique et industrielle",
     "reclassify: docx_results row, unit-normalized x1000, confirmed vs. neighbours (2015 ~878M)"),
    ("France", 2009, "Higher Education and Agricultural Research",
     296732542.0, "euro", "EUR", "JORF_2009.pdf", "117",
     "Enseignement supérieur et recherche agricoles",
     "reclassify: docx_results row, unit-normalized x1000, confirmed vs. neighbours (2014 ~312M)"),
    ("France", 2014, "Higher Education and Agricultural Research",
     312006931.0, "euro", "EUR", "JORF_2014.pdf", "110",
     "Enseignement supérieur et recherche agricoles",
     "reclassify: docx_results row, unit-normalized x1000, confirmed vs. neighbours (2009 ~297M / 2015 ~329M)"),
    ("France", 2015, "Higher Education and Agricultural Research",
     329442176.0, "euro", "EUR", "JORF_2015.pdf", "88",
     "Enseignement supérieur et recherche agricoles",
     "reclassify: docx_results row, unit-normalized x1000, confirmed vs. neighbours (2014 ~312M)"),
    ("France", 2017, "Higher Education and Agricultural Research",
     339670121.0, "euro", "EUR", "JORF_2017.pdf", "109",
     "Enseignement supérieur et recherche agricoles",
     "reclassify: docx_results row, unit-normalized x1000, confirmed vs. neighbours (2015 ~329M)"),

    # --- Portugal: raw_rows.csv matches, entity_raw checked directly ---
    ("Portugal", 1997, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
     13495419.0, "escudo", "PTE", "Lei orcamento para 1997.pdf", "",
     "FCT - Fundação para a Ciência e Tecnologia",
     "reclassify: raw_rows.csv entity_raw is literally 'FCT - Fundação para a Ciência e Tecnologia'"),
    ("Portugal", 2001, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
     20000000.0, "euro", "EUR", "2001 8e5ae31a-7057-4541-a78a-5e464e0fe30b.pdf", "",
     "FCT - Fundação para a Ciência e a Tecnologia",
     "reclassify: raw_rows.csv, corroborated by a second 'Cap. 50 - FN' line with the identical amount in the same document"),
    ("Portugal", 2002, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
     47048341.0, "euro", "EUR", "Lei orcamento para 2002.pdf", "",
     "FCT - Fundação para a Ciência e a Tecnologia",
     "reclassify: raw_rows.csv, clean single entity match"),
    ("Portugal", 2003, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
     634907638.0, "euro", "EUR", "Lei orcamento para 2003.pdf", "",
     "FCT - Fundação para a Ciência e a Tecnologia",
     "reclassify: raw_rows.csv; NOTE same amount also tagged 'Total for Capítulo 50' and "
     "'Programa Investigação Científica e Tecnológica e Inovação' in the same document — "
     "plausible because Capítulo 50 is FCT's own budget chapter, but flagged here for visibility"),
    ("Portugal", 2005, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
     40000000.0, "euro", "EUR", "Lei orcamento para 2005.pdf", "",
     "FCT - Fundação para a Ciência e a Tecnologia",
     "reclassify: raw_rows.csv, clean single entity match"),
    ("Portugal", 2024, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
     300000000.0, "euro", "EUR", "Lei orcamento para 2024.pdf", "",
     "FCT - Fundação para a Ciência e Tecnologia",
     "reclassify: raw_rows.csv, clean single entity match under Programa Investigação Científica e Tecnológica e Inovação"),
    ("Portugal", 1991, "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)",
     497900.0, "escudo", "PTE", "Lei orcamento para 1991.pdf", "",
     "Junta Nacional de Investigação Científica e Tecnológica",
     "reclassify: raw_rows.csv, clean single entity match"),
    ("Portugal", 1993, "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)",
     200000.0, "escudo", "PTE", "Lei orcamento para 1993.pdf", "",
     "Junta Nacional de Investigação Científica e Tecnológica",
     "reclassify: raw_rows.csv, clean single entity match"),
    ("Portugal", 1995, "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)",
     9373000.0, "escudo", "PTE", "Lei orcamento para 1995.pdf", "",
     "JNICT funding for various projects",
     "reclassify: raw_rows.csv, clean single entity match"),
    ("Portugal", 2009, "ANI — Agência Nacional de Inovação (Portugal)",
     10000000.0, "euro", "EUR", "Lei orcamento para 2009.pdf", "",
     "Agência Nacional de Inovação",
     "reclassify: raw_rows.csv, clean single entity match, consistent with 2016 (~9.4M)"),
    ("Portugal", 2002, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)",
     2819507.0, "euro", "EUR", "Lei orcamento para 2002.pdf", "",
     "LNEC - Laboratório Nacional de Engenharia Civil",
     "reclassify: raw_rows.csv, clean single entity match"),
    ("Portugal", 2003, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)",
     1352770.0, "euro", "EUR", "Lei orcamento para 2003.pdf", "",
     "LNEC",
     "reclassify: raw_rows.csv, clean single entity match"),

    # --- Luxembourg: docx_results.csv matches, decision="include", entity checked directly ---
    ("Luxembourg", 2003, "LIST / CRP Henri Tudor (Luxembourg)",
     750000.0, "euro", "EUR", "2003 eli-etat-leg-memorial-2002-a143-fr-pdf.pdf", "",
     "Transfer to LIST for research projects",
     "reclassify: docx_results.csv row, decision=include, clean single entity match"),
    ("Luxembourg", 2006, "LIST / CRP Henri Tudor (Luxembourg)",
     15000000.0, "euro", "EUR", "2006 eli-etat-leg-memorial-2005-a217-fr-pdf.pdf", "",
     "Allocation to CRP Henri Tudor for research",
     "reclassify: docx_results.csv row, decision=include, clean single entity match"),
    ("Luxembourg", 2006, "CRP Gabriel Lippmann (Luxembourg)",
     10000000.0, "euro", "EUR", "2006 eli-etat-leg-memorial-2005-a217-fr-pdf.pdf", "",
     "Allocation to CRP Gabriel Lippmann for research",
     "reclassify: docx_results.csv row, decision=include, clean single entity match"),
    ("Luxembourg", 2003, "Université du Luxembourg",
     500000.0, "euro", "EUR", "2003 eli-etat-leg-memorial-2002-a143-fr-pdf.pdf", "",
     "Allocation to the University of Luxembourg for research",
     "reclassify: docx_results.csv row, decision=include, clean single entity match"),
]

# Rows explicitly reviewed and REJECTED (left as-is / re-diagnosed), kept here
# only as a documented audit trail — not applied.
REJECTED_FOR_RECORD = [
    ("Portugal", 2021, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
     "matched row is a transfer FROM FCT's budget to elsewhere, not FCT's own appropriation (2 orders of magnitude too small)"),
    ("Portugal", 2022, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
     "matched row is a transfer TO Fundo de Contragarantia Mútuo sourced from FCT's budget, not FCT's own appropriation"),
    ("Portugal", 2000, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)",
     "matched row is a one-off institutional-restructuring procurement line, not LNEC's budget"),
    ("France", 2005, "Universities and Higher Education (Pre-LOLF Chapter)",
     "matched rows are Research-chapter lines excluded by this canonical's own exclude_match_groups rule (fixed upstream in gap_detector.py)"),
]

NEEDS_VERIFY_FOR_RECORD = [
    ("Portugal", 2004, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
     "raw amount (228,427,206) is duplicated byte-for-byte against an unrelated transfer to Instituto Politécnico de Bragança in the same document — possible table-parsing artifact"),
    ("Portugal", 2024, "ANI — Agência Nacional de Inovação (Portugal)",
     "11-20x jump vs. last known year (2009/2020), and the matched amount coincidentally equals a same-document 'Total para o Ministério' line — internally inconsistent"),
    ("Luxembourg", 2007, "LIST / CRP Henri Tudor (Luxembourg)",
     "two candidate rows in the same document: 'Allocation to the CRP Henri Tudor' = 10,000,000 vs. 'Transfer to the Luxembourg Institute of Science and Technology' (LIST's post-2015 name, anachronistic in a 2007 document) = 15,000,000 — unclear if these are the same line double-labeled or genuinely additive"),
]


def apply_country(country: str, rows: list[tuple], dry_run: bool) -> pd.DataFrame:
    cname = country.lower().replace(" ", "_")
    series_path = OUTPUT_DIR / country / f"{cname}_docx_series.csv"
    totals_path = OUTPUT_DIR / country / f"{cname}_docx_totals.csv"
    audit_path = OUTPUT_DIR / country / f"{cname}_reclassify_apply_audit.csv"

    series_df = pd.read_csv(series_path)
    audit_rows = []

    for (c, year, canonical_name, amount, unit, currency, source_file, page, line_desc, note) in rows:
        if c != country:
            continue

        if is_locked_observation(country, canonical_name, year):
            audit_rows.append({"year": year, "canonical_name": canonical_name, "applied": False,
                                "skip_reason": "locked_manual_curation"})
            continue

        mask = (series_df["year"] == year) & (series_df["canonical_name"].astype(str) == canonical_name)
        existing = series_df.loc[mask].copy()
        existing_amounts = pd.to_numeric(existing.get("amount_local"), errors="coerce").dropna()

        if not existing_amounts.empty:
            audit_rows.append({"year": year, "canonical_name": canonical_name, "applied": False,
                                "skip_reason": f"already_has_value({existing_amounts.tolist()})"})
            continue

        new_row = {
            "country": country,
            "year": year,
            "canonical_name": canonical_name,
            "category": existing.iloc[0].get("category") if not existing.empty else "",
            "amount_local": amount,
            "unit": unit,
            "currency": currency,
            "item_type": "section_total",
            "line_description_en": line_desc,
            "source_file": source_file,
            "page_number": page,
            "series_notes": note,
        }

        if not dry_run:
            series_df = series_df.loc[~mask].copy()
            series_df = pd.concat([series_df, pd.DataFrame([new_row])], ignore_index=True)

        audit_rows.append({"year": year, "canonical_name": canonical_name, "applied": True,
                            "amount_local": amount, "unit": unit, "currency": currency,
                            "source_file": source_file, "note": note})

    audit_df = pd.DataFrame(audit_rows)

    if dry_run:
        print(f"\n=== {country} (dry-run) ===")
        print(audit_df.to_string(index=False))
        return audit_df

    series_df = series_df.sort_values(["country", "canonical_name", "year", "source_file"]).reset_index(drop=True)
    series_df.to_csv(series_path, index=False)
    print(f"Updated series -> {series_path}")

    totals_df = build_totals_series(series_df, country=country)
    if not totals_df.empty:
        totals_df.to_csv(totals_path, index=False)
        print(f"Updated totals -> {totals_path}")

    audit_df.to_csv(audit_path, index=False)
    print(f"Apply audit -> {audit_path}")
    print(audit_df.to_string(index=False))

    return audit_df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    for country in ["France", "Portugal", "Luxembourg"]:
        apply_country(country, ACCEPTED, dry_run=args.dry_run)

    if not args.dry_run:
        build_combined_database(output_dir=OUTPUT_DIR)
        print("\nRebuilt combined database.")


if __name__ == "__main__":
    main()
