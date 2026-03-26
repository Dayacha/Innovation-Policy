"""
Post-processing cleaning script for results.csv
Expert-reviewed corrections for Denmark Finance Bill R&D budget data.
Run: python3 budget/clean_results.py
"""
import os
import pandas as pd
import numpy as np
import shutil
from datetime import datetime

RESULTS_PATH = "data/output/budget/results.csv"
REVIEW_STATUS_PATH = "data/output/budget/results_review_status.csv"
AI_VERIFIED_PATH = "data/output/budget/results_ai_verified.csv"

# ── helpers ─────────────────────────────────────────────────────────────────

def load():
    df = pd.read_csv(RESULTS_PATH)
    rs = pd.read_csv(REVIEW_STATUS_PATH)
    ai = pd.read_csv(AI_VERIFIED_PATH) if os.path.exists(AI_VERIFIED_PATH) else pd.DataFrame()
    return df, rs, ai

def backup(df, rs, ai):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    pairs = [(RESULTS_PATH, df), (REVIEW_STATUS_PATH, rs)]
    if not ai.empty:
        pairs.append((AI_VERIFIED_PATH, ai))
    for path, data in pairs:
        bak = path.replace(".csv", f"_backup_{ts}.csv")
        data.to_csv(bak, index=False)
        print(f"  Backed up → {bak}")

def match_key(df):
    """Composite matching key: year + program_code + line_description (first 60 chars)"""
    return (
        df["year"].astype(str)
        + "|" + df["program_code"].fillna("").str[:20]
        + "|" + df["line_description"].fillna("").str[:60]
    )

# ── removal rules ────────────────────────────────────────────────────────────

def build_removal_mask(df):
    """Returns boolean mask of rows to REMOVE (change to 'excluded')."""
    mask = pd.Series(False, index=df.index)
    removed = {}

    # 1. Pension transfers: §26 "Tilskud under [Ministry]"
    m1 = (
        (df["section_code"] == "§26") &
        (
            df["program_description"].str.contains(r"tilskud under", case=False, na=False) |
            df["line_description"].str.contains(r"tilskud under", case=False, na=False)
        )
    )
    removed["pension_transfers"] = df[m1][["year","program_code","amount_local"]].to_dict("records")
    mask |= m1

    # 2. Uddannelsestaxametre (per-student teaching payments, NOT R&D)
    m2 = (
        df["program_code"].fillna("").str.contains(r"20\.61\.01", regex=True) &
        (df["amount_local"] > 1_000_000)  # keep the trivial 47K item
    )
    removed["uddannelsestaxametre"] = df[m2][["year","program_code","amount_local"]].to_dict("records")
    mask |= m2

    # 3. Oil/gas extraction operations (large items only — small amounts may be energy R&D overhead)
    m3 = (
        df["program_code"].fillna("").str.contains(r"29\.23", regex=True) &
        (df["amount_local"] > 500_000_000)
    )
    removed["oil_gas_large"] = df[m3][["year","program_code","amount_local"]].to_dict("records")
    mask |= m3

    # 4. Innovation program reservation authorizations (one-time commitment totals, not annual R&D spend)
    m4 = (
        df["program_code"].fillna("").str.contains(r"19\.74\.03", regex=True) &
        (df["amount_local"] > 5_000_000_000)
    )
    removed["innovation_reservations"] = df[m4][["year","program_code","amount_local"]].to_dict("records")
    mask |= m4

    # 5. Welfare/unemployment program (not R&D)
    m5 = df["program_code"].fillna("").str.contains(r"17\.49\.24", regex=True)
    removed["welfare_program"] = df[m5][["year","program_code","amount_local"]].to_dict("records")
    mask |= m5

    # 6. Foreign environmental aid to developing countries (§6 6.34.01)
    m6 = (
        (df["section_code"] == "§6") &
        df["program_code"].fillna("").str.contains(r"6\.34\.01", regex=True)
    )
    removed["foreign_env_aid"] = df[m6][["year","program_code","amount_local"]].to_dict("records")
    mask |= m6

    # 7. Multilateral regional/transition aid (§6 6.36.02) — foreign aid, not R&D
    m7 = (
        (df["section_code"] == "§6") &
        df["program_code"].fillna("").str.contains(r"6\.36\.02", regex=True)
    )
    removed["multilateral_aid"] = df[m7][["year","program_code","amount_local"]].to_dict("records")
    mask |= m7

    # 8. Education expenditure for special programs (teaching cost, not R&D)
    m8 = df["program_code"].fillna("").str.contains(r"17\.42\.28", regex=True)
    removed["special_education"] = df[m8][["year","program_code","amount_local"]].to_dict("records")
    mask |= m8

    # 9. Ministry of Education administrative department budget (admin overhead, not R&D)
    m9 = (
        df["program_code"].fillna("").str.contains(r"20\.11\.01", regex=True) &
        (df["amount_local"] > 100_000_000)
    )
    removed["ministry_admin"] = df[m9][["year","program_code","amount_local"]].to_dict("records")
    mask |= m9

    # 10. State Car Inspection investment program 1997 (not R&D)
    m10 = df["program_code"].fillna("").str.contains(r"28\.22\.29", regex=True)
    removed["car_inspection"] = df[m10][["year","program_code","amount_local"]].to_dict("records")
    mask |= m10

    return mask, removed


def build_downgrade_mask(df):
    """Returns boolean mask of rows to DOWNGRADE from include → review."""
    mask = pd.Series(False, index=df.index)
    downgraded = {}

    # 1. Education & research buildings capital budget (mix of R&D and teaching facilities)
    m1 = (
        (
            df["program_code"].fillna("").str.contains(r"28\.73|29\.53", regex=True) |
            df["line_description"].fillna("").str.contains("forskningsbygninger", case=False, na=False)
        ) &
        (df["amount_local"] > 500_000_000)
    )
    downgraded["education_buildings"] = df[m1][["year","program_code","amount_local"]].to_dict("records")
    mask |= m1

    # 2. University capital expenditure block (mix of R&D equipment and general infra)
    m2 = (
        df["program_code"].fillna("").str.contains(r"20\.61\.03", regex=True) &
        (df["amount_local"] > 100_000_000)
    )
    downgraded["university_capital"] = df[m2][["year","program_code","amount_local"]].to_dict("records")
    mask |= m2

    # 3. Joint building program (university construction — not dedicated R&D)
    m3 = (
        df["program_code"].fillna("").str.contains(r"20\.61\.71", regex=True) &
        (df["amount_local"] > 50_000_000)
    )
    downgraded["joint_building"] = df[m3][["year","program_code","amount_local"]].to_dict("records")
    mask |= m3

    return mask, downgraded


# ── manual inserts ───────────────────────────────────────────────────────────

def build_manual_rows():
    """Manually verified R&D items missing from extraction (from direct PDF reading)."""
    rows = []
    base = dict(
        country="Denmark", currency="DKK", rd_category="direct_rd",
        pillar="Direct R&D", taxonomy_score=5.0, decision="include",
        confidence=0.99, budget_type=None,
        section_name_en="Ministry of Science / Education",
        program_description_en=None, line_description_en=None,
    )

    # ── 2006: §19.22 Universiteter (missed entirely by extractor) ────────────
    # Source: 2006 20051_L2_som_vedtaget.pdf page 81 — "10.493,5" Mio. kr.
    rows.append({**base,
        "year": 2006, "section_code": "§19",
        "section_name": "MinisterietforVidenskab,TeknologiogUdvikling",
        "program_code": "19.22",
        "program_description": "Universiteter (tekstanm. 7)",
        "line_description": "19.22 Universiteter (tekstanm. 7)",
        "amount_local": 10_493_500_000,
        "rationale": "MANUAL INSERT: PDF p.81 (2006 20051_L2_som_vedtaget.pdf) — 10.493,5 Mio.kr.; missed by extractor",
        "source_file": "2006 20051_L2_som_vedtaget.pdf", "page_number": 81,
    })

    # ── 2008: Individual universities under §19.22 (Aarhus was extracted, rest missed) ──
    # Source: 2008 A20080000130.pdf pp.88-89
    unis_2008 = [
        ("19.22.01", "Københavns Universitet (Reservationsbev.)",         4_251_600_000, 88),
        ("19.22.11", "Syddansk Universitet (Reservationsbev.)",            1_296_500_000, 89),
        ("19.22.15", "Roskilde Universitetscenter (Reservationsbev.)",       534_500_000, 89),
        ("19.22.17", "Aalborg Universitet (Reservationsbev.)",             1_219_000_000, 89),
        ("19.22.21", "Handelshøjskolen i København (Reservationsbev.)",      712_500_000, 89),
        ("19.22.37", "Danmarks Tekniske Universitet (Reservationsbev.)",   1_669_300_000, 89),
        ("19.22.45", "IT-Universitetet i København (Reservationsbev.)",      133_400_000, 89),
        ("19.22.49", "IT-Vest (Reservationsbev.)",                            21_000_000, 89),
    ]
    for code, desc, amt, pg in unis_2008:
        rows.append({**base,
            "year": 2008, "section_code": "§19",
            "section_name": "MinisterietforVidenskab,TeknologiogUdvikling",
            "program_code": code,
            "program_description": desc,
            "line_description": f"{code} {desc}",
            "amount_local": amt,
            "rationale": f"MANUAL INSERT: PDF p.{pg} (2008 A20080000130.pdf) — missing university sub-item",
            "source_file": "2008 A20080000130.pdf", "page_number": pg,
        })

    # ── 1997: 20.61.02 Forskningsaktiviteter missed by extractor ────────────────
    # Source: 1997 19961_L1_som_vedtaget.pdf page 92
    # Also 20.61.03 Kapitaludgifter (624M) was missed (all other years were extracted)
    rows.append({**base,
        "year": 1997, "section_code": "§20",
        "section_name": "Undervisningsministeriet",
        "section_name_en": "Ministry of Education",
        "program_code": "20.61.02",
        "program_description": "Forskningsaktiviteter m.v. (Driftsbev.)",
        "line_description": "20.61.02 Forskningsaktiviteter m.v. (Driftsbev.)",
        "amount_local": 3_152_900_000,
        "rationale": "MANUAL INSERT: PDF p.92 (1997 19961_L1_som_vedtaget.pdf) — missed by extractor; research activities block grant",
        "source_file": "1997 19961_L1_som_vedtaget.pdf", "page_number": 92,
    })
    rows.append({**base,
        "year": 1997, "section_code": "§20",
        "section_name": "Undervisningsministeriet",
        "section_name_en": "Ministry of Education",
        "program_code": "20.61.03",
        "program_description": "Kapitaludgifter (Driftsbev.)",
        "line_description": "20.61.03 Kapitaludgifter (Driftsbev.)",
        "amount_local": 624_000_000,
        "decision": "review",  # capital expenditure — mix of R&D and non-R&D
        "rationale": "MANUAL INSERT: PDF p.92 (1997 19961_L1_som_vedtaget.pdf) — missed by extractor; university capital budget (review: mix R&D/non-R&D)",
        "source_file": "1997 19961_L1_som_vedtaget.pdf", "page_number": 92,
    })

    # ── 2003: §19.22 Universiteter (missed by extractor) ─────────────────────
    # Source: 2003 20021_L1_som_vedtaget.pdf page 86 — "6.366,4" Mio. kr.
    # Note: 2003 is the first year universities moved from §20.61 to §19.22
    rows.append({**base,
        "year": 2003, "section_code": "§19",
        "section_name": "MinisterietforVidenskab,TeknologiogUdvikling",
        "section_name_en": "Ministry of Science, Technology and Innovation",
        "program_code": "19.22",
        "program_description": "Universiteter (tekstanm. 7)",
        "line_description": "19.22 Universiteter (tekstanm. 7)",
        "amount_local": 6_366_400_000,
        "rationale": "MANUAL INSERT: PDF p.86 (2003 20021_L1_som_vedtaget.pdf) — 6.366,4 Mio.kr.; missed by extractor",
        "source_file": "2003 20021_L1_som_vedtaget.pdf", "page_number": 86,
    })

    # ── 2004: §19.22 Universiteter (missed by extractor) ─────────────────────
    # Source: 2004 20031_L1_som_vedtaget.pdf page 83 — "10.047,4" Mio. kr.
    rows.append({**base,
        "year": 2004, "section_code": "§19",
        "section_name": "MinisterietforVidenskab,TeknologiogUdvikling",
        "section_name_en": "Ministry of Science, Technology and Innovation",
        "program_code": "19.22",
        "program_description": "Universiteter (tekstanm. 7)",
        "line_description": "19.22 Universiteter (tekstanm. 7)",
        "amount_local": 10_047_400_000,
        "rationale": "MANUAL INSERT: PDF p.83 (2004 20031_L1_som_vedtaget.pdf) — 10.047,4 Mio.kr.; missed by extractor",
        "source_file": "2004 20031_L1_som_vedtaget.pdf", "page_number": 83,
    })

    # ── 2014: Complete extraction failure (two-column PDF layout) ─────────────
    # Source: 2014 A20130000230.pdf — amounts in Mio. kr.
    items_2014 = [
        ("§19", "Uddannelses- og Forskningsministeriet", "19.22", "Universiteter",
         "19.22 Universiteter", 16_643_400_000, "direct_rd", "Direct R&D", 104),
        ("§19", "Uddannelses- og Forskningsministeriet", "19.41.11", "Det Strategiske Forskningsråd (Reservationsbev.)",
         "19.41.11 Det Strategiske Forskningsråd (Reservationsbev.)", 868_000_000, "direct_rd", "Direct R&D", 106),
        ("§19", "Uddannelses- og Forskningsministeriet", "19.41.12", "Det Frie Forskningsråd (Reservationsbev.)",
         "19.41.12 Det Frie Forskningsråd (Reservationsbev.)", 1_252_400_000, "direct_rd", "Direct R&D", 106),
        ("§19", "Uddannelses- og Forskningsministeriet", "19.41.15", "Infrastrukturmidler (Reservationsbev.)",
         "19.41.15 Infrastrukturmidler (Reservationsbev.)", 66_500_000, "direct_rd", "Direct R&D", 106),
        ("§19", "Uddannelses- og Forskningsministeriet", "19.74", "Kompetence og teknologi m.v.",
         "19.74 Kompetence og teknologi m.v.", 988_100_000, "direct_rd", "Direct R&D", 107),
        ("§19", "Uddannelses- og Forskningsministeriet", "19.15", "Internationalt forskningssamarbejde",
         "19.15 Internationalt forskningssamarbejde", 865_900_000, "direct_rd", "Direct R&D", 105),
        ("§19", "Uddannelses- og Forskningsministeriet", "19.17", "Nye forskningsprogrammer",
         "19.17 Nye forskningsprogrammer", 144_200_000, "direct_rd", "Direct R&D", 105),
        ("§19", "Uddannelses- og Forskningsministeriet", "19.55", "Særlige forskningsinstitutioner",
         "19.55 Særlige forskningsinstitutioner", 418_000_000, "direct_rd", "Direct R&D", 107),
        ("§16", "Ministeriet for Sundhed og Forebyggelse", "16.35", "Forskning og forebyggelse af smitsomme sygdomme mv.",
         "16.35 Forskning og forebyggelse af smitsomme sygdomme mv.", 1_533_300_000, "direct_rd", "Applied R&D", 85),
        ("§29", "Klima-, Energi- og Bygningsministeriet", "29.22", "Forskning og udvikling",
         "29.22 Forskning og udvikling", 382_100_000, "direct_rd", "Applied R&D", 163),
        ("§29", "Klima-, Energi- og Bygningsministeriet", "29.41", "Geologisk forskning og undersøgelser",
         "29.41 Geologisk forskning og undersøgelser", 320_600_000, "direct_rd", "Applied R&D", 164),
        ("§24", "Fødevareministeriet", "24.33.02", "Tilskud til udvikling og demonstration",
         "24.33.02 Tilskud til udvikling og demonstration", 259_400_000, "direct_rd", "Applied R&D", 150),
        ("§24", "Fødevareministeriet", "24.33.03", "Forskningsbaseret myndighedsbetjening",
         "24.33.03 Forskningsbaseret myndighedsbetjening", 581_900_000, "direct_rd", "Applied R&D", 150),
    ]
    for (sec_code, sec_name, prog_code, prog_desc, line_desc, amt, rd_cat, pillar, pg) in items_2014:
        rows.append({**base,
            "year": 2014, "section_code": sec_code,
            "section_name": sec_name,
            "section_name_en": sec_name,
            "program_code": prog_code,
            "program_description": prog_desc,
            "line_description": line_desc,
            "amount_local": amt,
            "rd_category": rd_cat, "pillar": pillar,
            "rationale": f"MANUAL INSERT: PDF p.{pg} (2014 A20130000230.pdf) — two-column layout missed by extractor",
            "source_file": "2014 A20130000230.pdf", "page_number": pg,
        })

    return pd.DataFrame(rows)


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    print("Loading data...")
    df, rs, ai = load()

    print("Backing up originals...")
    backup(df, rs, ai)

    orig_include = (df["decision"] == "include").sum()
    orig_total = len(df)
    print(f"\nOriginal: {orig_total} rows, {orig_include} include")

    # ── Apply removes ────────────────────────────────────────────────────────
    print("\n── REMOVING false positives ─────────────────────────────────")
    remove_mask, removed_info = build_removal_mask(df)
    inc_mask = df["decision"] == "include"
    remove_include_mask = remove_mask & inc_mask

    for label, items in removed_info.items():
        if items:
            total = sum(i["amount_local"] for i in items) / 1e9
            print(f"  [{label}] {len(items)} rows  {total:.2f}B DKK")

    df.loc[remove_mask, "decision"] = "excluded"
    df.loc[remove_mask, "rationale"] = (
        "EXPERT REVIEW: " + df.loc[remove_mask, "rationale"].fillna("") +
        " | Removed: not R&D expenditure (see clean_results.py)"
    )

    # ── Apply downgrades ─────────────────────────────────────────────────────
    print("\n── DOWNGRADING ambiguous items ──────────────────────────────")
    down_mask, down_info = build_downgrade_mask(df)
    # Only downgrade items currently 'include'
    down_mask = down_mask & (df["decision"] == "include")

    for label, items in down_info.items():
        if items:
            total = sum(i["amount_local"] for i in items) / 1e9
            print(f"  [{label}] {len(items)} rows  {total:.2f}B DKK")

    df.loc[down_mask, "decision"] = "review"
    df.loc[down_mask, "rationale"] = (
        "EXPERT REVIEW: " + df.loc[down_mask, "rationale"].fillna("") +
        " | Downgraded: ambiguous (see clean_results.py)"
    )

    # ── Add manual rows ──────────────────────────────────────────────────────
    print("\n── ADDING manually verified rows ────────────────────────────")
    manual = build_manual_rows()
    print(f"  Adding {len(manual)} rows for years: {sorted(manual['year'].unique())}")
    year_totals = manual.groupby("year")["amount_local"].sum() / 1e9
    for yr, tot in year_totals.items():
        print(f"    {yr}: {tot:.1f}B DKK added")

    df = pd.concat([df, manual], ignore_index=True)
    df = df.sort_values(["year", "section_code", "program_code"]).reset_index(drop=True)

    # ── Year summary ─────────────────────────────────────────────────────────
    print("\n── YEAR TOTALS (include only) ───────────────────────────────")
    inc = df[df["decision"] == "include"].groupby("year")["amount_local"].sum() / 1e6
    for yr, amt in inc.items():
        print(f"  {yr}: {amt:.0f}M DKK")

    # ── Save results.csv ─────────────────────────────────────────────────────
    df.to_csv(RESULTS_PATH, index=False)
    print(f"\nSaved results.csv ({len(df)} rows)")

    # ── Update results_review_status.csv ────────────────────────────────────
    # Rebuild from scratch: merge review_status from old rs by key
    old_key = match_key(rs)
    old_status = dict(zip(old_key.tolist(), rs["review_status"].tolist()))

    new_key = match_key(df)
    df_with_status = df.copy()
    df_with_status["review_status"] = [old_status.get(k, "pending_ai_review") for k in new_key.tolist()]

    df_with_status.to_csv(REVIEW_STATUS_PATH, index=False)
    reviewed = (df_with_status["review_status"] == "reviewed").sum()
    pending = (df_with_status["review_status"] == "pending_ai_review").sum()
    print(f"Saved results_review_status.csv (reviewed={reviewed}, pending={pending})")

    # ── Validate ai_verified: report on consistency ──────────────────────────
    print("\n── VALIDATING ai_verified consistency ───────────────────────")
    if ai.empty:
        print("  ai_verified is empty (fresh run) — skipping validation")
        print("\nDone.")
        return
    ai.to_csv(AI_VERIFIED_PATH, index=False)

    # Build set of (year, amount) for items we REMOVED from include
    removed_keys = set()
    for items in removed_info.values():
        for r in items:
            removed_keys.add((r["year"], r["amount_local"]))

    # Check if any removed items appear in ai_verified
    ai_keys_set = set(
        zip(ai["year"].tolist(), ai["amount_local"].tolist())
    )
    overlap = removed_keys & ai_keys_set

    if overlap:
        print(f"  WARNING: {len(overlap)} removed items found in ai_verified:")
        for yr, amt in sorted(overlap):
            print(f"    year={yr}  amount={amt/1e6:.1f}M")
        # These should be removed from ai_verified
        ai_clean = ai[~ai.apply(lambda r: (r["year"], r["amount_local"]) in overlap, axis=1)]
        ai_clean.to_csv(AI_VERIFIED_PATH, index=False)
        print(f"  Cleaned ai_verified: {len(ai_clean)} rows (removed {len(ai) - len(ai_clean)})")
    else:
        print(f"  No removed items found in ai_verified ✓ ({len(ai)} rows unchanged)")

    print(f"\nFinal ai_verified: {len(pd.read_csv(AI_VERIFIED_PATH))} rows")

    print("\nDone.")


if __name__ == "__main__":
    main()
