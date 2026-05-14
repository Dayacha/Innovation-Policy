"""
Portugal-specific post-extraction cleaner.

Documents: Lei do Orçamento de Estado (OE), published in Diário da República 1.ª série.

SOURCE GUIDE:
  - ALL YEARS: space is thousands separator (281 634 915 = 281,634,915).
  - 1977-2001: PTE (escudo). 1 EUR = 200.482 PTE.
               Units: may be FULL ESCUDOS (unit='unit') or CONTOS (1 conto = 1,000 escudos,
               unit='thousand'). Check table header.
  - 2002+:     EUR, full euros (unit='unit'). FCT ~€200-400M/year.
  - TEXT:      1977-2000 are scanned (OCR). 2001+ have text layer.
  - DUPLICATES: 'Lei orcamento para 1985.pdf' == 'Lei orcamento para 1986.pdf' (identical files).
                '1997 02040557.pdf' == 'Lei orcamento para 1997.pdf'.

CRITICAL WARNING:
  'investigação' (investigation) is used in BOTH scientific R&D context (FCT, JNICT,
  investigação científica) AND criminal/police context (Polícia Judiciária, PGR, PSP).
  Criminal investigation lines are NOT R&D.

KEY R&D STRUCTURE:
  - FCT (from 1997): Capítulo 50 under science ministry, or P002 programme
  - JNICT (pre-1997): predecessor to FCT
  - P002: Programa Investigação Científica e Tecnológica e Inovação
  - Ministério da Ciência (various names) — primary R&D ministry
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

_RESEARCH_RE = re.compile(
    r"investiga[cç][aã]o\s+cient[ií]fica|"
    r"fct\b|fundação\s+para\s+a\s+ciência|fundacao\s+para\s+a\s+ciencia|"
    r"jnict\b|inic\b|"
    r"ani\b|agência\s+nacional\s+de\s+inova[cç][aã]o|"
    r"p002\b|p-002\b|"
    r"ciência\s+e\s+tecnologia|ciencia\s+e\s+tecnologia|"
    r"i&d\b|investigação\s+e\s+desenvolvimento|"
    r"bolsas\s+de\s+(?:doutoramento|investigação)|"
    r"centros?\s+de\s+investigação|"
    r"laboratório\s+(?:nacional|de\s+estado)|laboratorio\s+(?:nacional|de\s+estado)|"
    r"lnec\b|ineti\b|iniav\b|inrb\b|",
    re.IGNORECASE,
)

_CRIMINAL_INVEST_RE = re.compile(
    r"pol[ií]cia\s+judici[aá]ria\b|\bpj\b|"
    r"procuradoria.geral\s+da\s+rep[uú]blica|\bpgr\b|"
    r"pol[ií]cia\s+de\s+seguran[cç]a\s+p[uú]blica|\bpsp\b|"
    r"\bgnr\b|\bsef\b|servi[cç]os?\s+de\s+estrangeiros|"
    r"investiga[cç][aã]o\s+criminal|investiga[cç][aã]o\s+judici[aá]ria|"
    r"investiga[cç][aã]o\s+e\s+persegui[cç][aã]o|"
    r"sistema\s+de\s+investiga[cç][aã]o\s+criminal|"
    r"m002\s*[-–]\s*sistema\s+de\s+investiga[cç][aã]o",
    re.IGNORECASE,
)

_DEBT_RE = re.compile(
    r"servi[cç]o\s+da\s+d[ií]vida|encargos\s+com\s+a\s+d[ií]vida|"
    r"d[ií]vida\s+p[uú]blica|amortiza[cç][aã]o\s+da\s+d[ií]vida|"
    r"juros\s+da\s+d[ií]vida|refinanciamento\s+da\s+d[ií]vida",
    re.IGNORECASE,
)

_SOCIAL_RE = re.compile(
    r"seguran[cç]a\s+social\b|"
    r"presta[cç][oõ]es\s+sociais|transferências\s+para\s+a\s+seguran[cç]a|"
    r"pens[oõ]es\b|reform(?:as|ados)\b|"
    r"adse\b|cga\b|caixa\s+geral\s+de\s+aposenta[cç][oõ]es|"
    r"subsidio\s+de\s+desemprego|prest[aá][cç][aã]o\s+social",
    re.IGNORECASE,
)

_DEFENCE_RE = re.compile(
    r"for[cç]as\s+armadas|minist[eé]rio\s+da\s+defesa\s+nacional|"
    r"exercito\b|marinha\b|for[cç]a\s+a[eé]rea\b|"
    r"defesa\s+nacional\b",
    re.IGNORECASE,
)

_TRANSPORT_RE = re.compile(
    r"infraestruturas\s+de\s+portugal|"
    r"\bep\b.*estradas|estradas\s+de\s+portugal|"
    r"refer\b|infraestrutura\s+ferrovi[aá]ria|"
    r"constru[cç][aã]o\s+de\s+(?:estradas|autoestradas|pontes)|"
    r"rede\s+rodovi[aá]ria",
    re.IGNORECASE,
)

_EDUCATION_BROAD_RE = re.compile(
    r"ensino\s+b[aá]sico\b|ensino\s+secund[aá]rio\b|"
    r"ensino\s+pr[eé]-escolar\b|"
    r"minist[eé]rio\s+da\s+educa[cç][aã]o\b",
    re.IGNORECASE,
)

_MACRO_TOTAL_RE = re.compile(
    r"total\s+do\s+or[cç]amento|"
    r"despesa\s+total\b|receita\s+total\b|"
    r"mapa\s+\d+\b|"
    r"s[aá]ldo\s+or[cç]amental|"
    r"resumo\s+geral\s+das\s+despesas",
    re.IGNORECASE,
)

_DUPLICATE_1985_RE = re.compile(
    r"lei\s+or[cç]amento\s+para\s+1985|orcamento\s+para\s+1985",
    re.IGNORECASE,
)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Portugal-specific post-extraction corrections."""
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

    # Duplicate 1985 file (identical to 1986) — exclude entirely
    dup_1985_mask = (year_num == 1985) | source_files.str.contains(_DUPLICATE_1985_RE, regex=True, na=False)
    if dup_1985_mask.any():
        df.loc[dup_1985_mask, "decision"] = "exclude"
        _note(df, dup_1985_mask,
              "[portugal_duplicate_1985: file is identical to the 1986 budget — excluded] ")

    # Criminal investigation — 'investigação' = criminal, not scientific
    criminal_mask = combined.str.contains(_CRIMINAL_INVEST_RE, regex=True) & ~has_research
    if criminal_mask.any():
        df.loc[criminal_mask, "aggregation_role"] = "non_rd"
        df.loc[criminal_mask, "decision"] = "review"
        _note(df, criminal_mask,
              "[portugal_criminal_investigation: investigação = criminal, not scientific R&D] ")

    # Social insurance / pensions — non-R&D
    social_mask = combined.str.contains(_SOCIAL_RE, regex=True) & ~has_research
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[portugal_social_transfer: non-R&D] ")

    # Debt service — always non-R&D
    debt_mask = combined.str.contains(_DEBT_RE, regex=True)
    if debt_mask.any():
        df.loc[debt_mask, "aggregation_role"] = "non_rd"
        df.loc[debt_mask, "decision"] = "review"
        _note(df, debt_mask, "[portugal_debt_service: non-R&D] ")

    # Defence without research signal
    defence_mask = combined.str.contains(_DEFENCE_RE, regex=True) & ~has_research
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = "review"
        _note(df, defence_mask, "[portugal_defence_non_rd] ")

    # Transport infrastructure without research signal
    transport_mask = combined.str.contains(_TRANSPORT_RE, regex=True) & ~has_research
    if transport_mask.any():
        df.loc[transport_mask, "aggregation_role"] = "non_rd"
        df.loc[transport_mask, "decision"] = "review"
        _note(df, transport_mask, "[portugal_transport_non_rd] ")

    # Broad primary/secondary education without research signal
    educ_mask = combined.str.contains(_EDUCATION_BROAD_RE, regex=True) & ~has_research
    if educ_mask.any():
        df.loc[educ_mask, "aggregation_role"] = "non_rd"
        df.loc[educ_mask, "decision"] = "review"
        _note(df, educ_mask, "[portugal_broad_education: non-R&D] ")

    # Macro totals
    macro_mask = combined.str.contains(_MACRO_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        macro_mask = macro_mask | (df["item_type"] == "section_total")
    if macro_mask.any():
        df.loc[macro_mask, "aggregation_role"] = df.loc[macro_mask, "aggregation_role"].replace("", "section")
        _note(df, macro_mask, "[portugal_macro_total: section aggregate] ")

    # Pre-EUR escudo era note
    pre_eur = year_num < 2002
    if pre_eur.any():
        _note(df, pre_eur,
              "[portugal_escudo_era: amounts in PTE (escudo); "
              "1 EUR = 200.482 PTE. Unit may be 'unit' (escudos) or 'thousand' (contos).] ")

    # Implausible single R&D line > 2B EUR (post-2002 only)
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col:
        unit_s = df.get("unit", pd.Series("", index=df.index)).fillna("").str.lower()
        implausible = (
            (unit_s == "unit")
            & (pd.to_numeric(df[amount_col], errors="coerce") > 2_000_000_000)
            & (year_num >= 2002)
            & (df["decision"] == "include")
        )
        if implausible.any():
            df.loc[implausible, "decision"] = "review"
            _note(df, implausible,
                  "[portugal_implausible_amount: >2B EUR single R&D line — "
                  "likely budget total or wrong unit] ")

    return df.reset_index(drop=True)
