"""
Turkey-specific post-extraction cleaner.

Documents: Bütçe Kanunu / Merkezi Yönetim Bütçe Kanunu, published in Resmî Gazete.

SOURCE GUIDE:
  - ALL YEARS: period is thousands separator (1.234.567 = 1,234,567).
  - 1975-2004: TRL (old Turkish lira). HYPERINFLATION — amounts in trillions.
               1 YTL = 1,000,000 old TRL (redenomination 1 January 2005).
  - 2005-2008: YTL (Yeni Türk Lirası). Unit='unit' (full lira).
  - 2009+:     TL (Turkish Lira, same as YTL — 'Yeni' dropped). Unit='unit'.

CRITICAL STRUCTURE:
  (I)  SAYILI CETVEL — GENEL BÜTÇELİ İDARELER (General Budget).
       Ministries and attached units. Sanayi ve Teknoloji Bakanlığı appears here.
  (II) SAYILI CETVEL — ÖZEL BÜTÇELİ İDARELER (Special/Autonomous Budget).
       TÜBİTAK, TÜBA, TAEK, YÖK, KOSGEB, universities appear HERE.
       Files labelled '2-a' contain ONLY (I) — TÜBİTAK is ABSENT.

DUPLICATES:
  1990 file == 1991 file (byte-for-byte identical).
  2001 UUID scanned file — use only the Kanun text-layer file.
  2002 '(1)' copy — process only one.
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

_RESEARCH_RE = re.compile(
    r"tübitak\b|tubitak\b|"
    r"türkiye\s+bilimsel\s+ve\s+teknolojik|turkiye\s+bilimsel\s+ve\s+teknolojik|"
    r"tüba\b|tuba\b|türkiye\s+bilimler\s+akademisi|"
    r"taek\b|atom\s+enerjisi\s+kurumu|"
    r"\bar-ge\b|araştırma.geliştirme|arastirma.gelistirme|"
    r"bilimsel\s+araştırma|bilimsel\s+arastirma|"
    r"bilimsel\s+araştırma\s+projeleri|bap\b|"
    r"teknoloji\s+geliştirme|teknoloji\s+gelistirme|"
    r"inovasyon\b|"
    r"sanayi\s+ve\s+teknoloji\s+bakanlığı|sanayi\s+ve\s+teknoloji\s+bakanligi|"
    r"bilim.*sanayi.*teknoloji|"
    r"kosgeb\b|"
    r"türkiye\s+uzay\s+ajansı|turkiye\s+uzay\s+ajansi|\btua\b|"
    r"yükseköğretim\s+kurulu|\byök\b",
    re.IGNORECASE,
)

_CRIMINAL_INVEST_RE = re.compile(
    r"emniyet\s+genel\s+müdürlüğü|emniyet\s+genel\s+mudurlugu|"
    r"jandarma\s+genel\s+komutanlığı|jandarma\s+genel\s+komutanligi|"
    r"iç\s+işleri\s+bakanlığı|ic\s+isleri\s+bakanligi|"
    r"cumhuriyet\s+başsavcılığı|cumhuriyet\s+bassavciligi|"
    r"kriminal\s+araştırma|kriminal\s+arastirma|"
    r"adli\s+araştırma|adli\s+arastirma",
    re.IGNORECASE,
)

_DEBT_RE = re.compile(
    r"kamu\s+borç\s+yönetimi|kamu\s+borc\s+yonetimi|"
    r"borç\s+servisine|borc\s+servisine|"
    r"hazine.*borç|hazine.*borc|"
    r"faiz\s+giderleri|"
    r"iç\s+borçlanma|dis\s+borclanma|dış\s+borçlanma|"
    r"kamu\s+dış\s+borcu|kamu\s+ic\s+borcu",
    re.IGNORECASE,
)

_SOCIAL_RE = re.compile(
    r"sosyal\s+güvenlik\s+kurumu|\bsgk\b|"
    r"emeklilik\s+ödemeleri|emeklilik\s+odemeleri|"
    r"bağ.kur\b|bag.kur\b|\bssk\b|"
    r"sosyal\s+güvenlik\s+transferleri|sosyal\s+guvenlik\s+transferleri|"
    r"işsizlik\s+sigortası|issizlik\s+sigortasi",
    re.IGNORECASE,
)

_DEFENCE_RE = re.compile(
    r"millî\s+savunma\s+bakanlığı|milli\s+savunma\s+bakanligi|\bmsb\b|"
    r"kara\s+kuvvetleri\s+komutanlığı|kara\s+kuvvetleri\s+komutanligi|"
    r"deniz\s+kuvvetleri\s+komutanlığı|deniz\s+kuvvetleri\s+komutanligi|"
    r"hava\s+kuvvetleri\s+komutanlığı|hava\s+kuvvetleri\s+komutanligi|"
    r"savunma\s+sanayii\s+başkanlığı|savunma\s+sanayii\s+baskanligi|\bssb\b",
    re.IGNORECASE,
)

_TRANSPORT_RE = re.compile(
    r"karayolları\s+genel\s+müdürlüğü|karayollari\s+genel\s+mudurlugu|"
    r"devlet\s+demiryolları|\btcdd\b|"
    r"ulaştırma.*bakanlığı|ulastirma.*bakanligi|"
    r"havalimanı\s+inşaatı|havalimanı\s+insaati|"
    r"otoban\s+yapımı|otoyol\s+yapimi",
    re.IGNORECASE,
)

_EDUCATION_BROAD_RE = re.compile(
    r"ilköğretim\s+genel\b|ilkogretim\s+genel\b|"
    r"ortaöğretim\s+genel\b|ortaogretim\s+genel\b|"
    r"milli\s+eğitim\s+bakanlığı\s+genel|milli\s+egitim\s+bakanligi\s+genel|"
    r"\bmeb\b(?!.*ar.ge)(?!.*araştırma)(?!.*arastirma)",
    re.IGNORECASE,
)

_MACRO_TOTAL_RE = re.compile(
    r"genel\s+toplam\s+ödenek|genel\s+toplam\s+odenek|"
    r"toplam\s+bütçe\s+giderleri|toplam\s+butce\s+giderleri|"
    r"merkezi\s+yönetim\s+bütçesi?\s+toplamı|"
    r"bütçe\s+genel\s+toplamı|butce\s+genel\s+toplami",
    re.IGNORECASE,
)

_DUPLICATE_1990_RE = re.compile(
    r"1990\s*bütçe\s*kanunu|1990\s*butce\s*kanunu|"
    r"1990.*bütçe.*kanunu|1990.*butce.*kanunu",
    re.IGNORECASE,
)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Turkey-specific post-extraction corrections."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    desc_col = "line_description_en" if "line_description_en" in df.columns else "line_description"
    raw_col = "line_description" if "line_description" in df.columns else desc_col
    descs = df[desc_col].fillna("").astype(str)
    raw_descs = df[raw_col].fillna("").astype(str)
    sections = df.get("section_name", pd.Series("", index=df.index)).fillna("").astype(str)
    sections_en = df.get("section_name_en", pd.Series("", index=df.index)).fillna("").astype(str)
    source_files = df.get("source_file", pd.Series("", index=df.index)).fillna("").astype(str)
    combined = descs + " " + raw_descs + " " + sections + " " + sections_en

    year_num = pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce")
    has_research = combined.str.contains(_RESEARCH_RE, regex=True)

    # Duplicate 1990 file (identical to 1991) — exclude entirely
    dup_1990_mask = (year_num == 1990) | source_files.str.contains(_DUPLICATE_1990_RE, regex=True, na=False)
    if dup_1990_mask.any():
        df.loc[dup_1990_mask, "decision"] = "exclude"
        _note(df, dup_1990_mask,
              "[turkey_duplicate_1990: file is identical to the 1991 budget — excluded] ")

    # Criminal investigation — 'araştırma' in interior ministry = criminal, not scientific
    criminal_mask = combined.str.contains(_CRIMINAL_INVEST_RE, regex=True) & ~has_research
    if criminal_mask.any():
        df.loc[criminal_mask, "aggregation_role"] = "non_rd"
        df.loc[criminal_mask, "decision"] = "review"
        _note(df, criminal_mask,
              "[turkey_criminal_investigation: araştırma = criminal investigation, not scientific R&D] ")

    # Social security / pensions — non-R&D
    social_mask = combined.str.contains(_SOCIAL_RE, regex=True) & ~has_research
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[turkey_social_transfer: non-R&D] ")

    # Debt service — always non-R&D
    debt_mask = combined.str.contains(_DEBT_RE, regex=True)
    if debt_mask.any():
        df.loc[debt_mask, "aggregation_role"] = "non_rd"
        df.loc[debt_mask, "decision"] = "review"
        _note(df, debt_mask, "[turkey_debt_service: non-R&D] ")

    # Defence without research signal
    defence_mask = combined.str.contains(_DEFENCE_RE, regex=True) & ~has_research
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = "review"
        _note(df, defence_mask, "[turkey_defence_non_rd] ")

    # Transport infrastructure without research signal
    transport_mask = combined.str.contains(_TRANSPORT_RE, regex=True) & ~has_research
    if transport_mask.any():
        df.loc[transport_mask, "aggregation_role"] = "non_rd"
        df.loc[transport_mask, "decision"] = "review"
        _note(df, transport_mask, "[turkey_transport_non_rd] ")

    # Broad primary/secondary education without research signal
    educ_mask = combined.str.contains(_EDUCATION_BROAD_RE, regex=True) & ~has_research
    if educ_mask.any():
        df.loc[educ_mask, "aggregation_role"] = "non_rd"
        df.loc[educ_mask, "decision"] = "review"
        _note(df, educ_mask, "[turkey_broad_education: non-R&D] ")

    # Macro totals
    macro_mask = combined.str.contains(_MACRO_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        macro_mask = macro_mask | (df["item_type"] == "section_total")
    if macro_mask.any():
        df.loc[macro_mask, "aggregation_role"] = df.loc[macro_mask, "aggregation_role"].replace("", "section")
        _note(df, macro_mask, "[turkey_macro_total: section aggregate] ")

    # Pre-YTL old lira era note (hyperinflation amounts)
    pre_ytl = year_num < 2005
    if pre_ytl.any():
        _note(df, pre_ytl,
              "[turkey_old_lira_era: amounts in TRL (old Turkish lira, hyperinflation); "
              "1 YTL = 1,000,000 TRL (redenomination 1 January 2005).] ")

    # YTL era note
    ytl_era = (year_num >= 2005) & (year_num <= 2008)
    if ytl_era.any():
        _note(df, ytl_era,
              "[turkey_ytl_era: amounts in Yeni Türk Lirası (YTL); "
              "1 YTL = 1,000,000 old TRL.] ")

    # Implausible single line post-2005 (> 500B TL for a single R&D line)
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col:
        unit_s = df.get("unit", pd.Series("", index=df.index)).fillna("").str.lower()
        implausible = (
            (unit_s == "unit")
            & (pd.to_numeric(df[amount_col], errors="coerce") > 500_000_000_000)
            & (year_num >= 2005)
            & (df["decision"] == "include")
        )
        if implausible.any():
            df.loc[implausible, "decision"] = "review"
            _note(df, implausible,
                  "[turkey_implausible_amount: >500B TL single R&D line post-2005 — "
                  "likely budget total or wrong unit] ")

    return df.reset_index(drop=True)
