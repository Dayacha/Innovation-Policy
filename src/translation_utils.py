"""Simple glossary-based translation helpers."""

import re


def translate_to_english_glossary(text: str) -> str:
    """Translate common budget terms to English using deterministic replacements."""
    translated = text or ""
    glossary_patterns = [
        (r"\bUndervisningsministeriet\b", "Ministry of Education"),
        (r"\bSocialministeriet\b", "Ministry of Social Affairs"),
        (r"\bSocialforskningsinstituttet\b", "Social Research Institute"),
        (r"\bSocialforskningsrådet\b", "Social Research Council"),
        (r"\bSocialforskningsradet\b", "Social Research Council"),
        (r"\bSocialforskningsrÃ¥det\b", "Social Research Council"),
        (r"\bNettotal\b", "Net total"),
        (r"\bDriftsudgifter\b", "Operating expenditures"),
        (r"\bTilskud\b", "Grants"),
        (r"\bIndtægter\b", "Revenues"),
        (r"\bIndtaegter\b", "Revenues"),
        (r"\bIndtÃ¦gter\b", "Revenues"),
        (r"\bAnlægsudgifter\b", "Capital expenditures"),
        (r"\bAnlaegsudgifter\b", "Capital expenditures"),
        (r"\bAnlÃ¦gsudgifter\b", "Capital expenditures"),
        (r"\bAnlægstilskud\b", "Capital grants"),
        (r"\bAnlaegstilskud\b", "Capital grants"),
        (r"\bAnlÃ¦gstilskud\b", "Capital grants"),
        (r"\bEjendomserhvervelser\b", "Property acquisitions"),
        (r"\bUdlån\b", "Lending"),
        (r"\bUdlan\b", "Lending"),
        (r"\bUdlÃ¥n\b", "Lending"),
        (r"\bforskning\b", "research"),
        (r"\bteknologi\b", "technology"),
        (r"\buniversitet\b", "university"),
        (r"\buddannelse\b", "education"),
        (r"\bvidenskab\b", "science"),
        (r"\brecherche\b", "research"),
        (r"\btechnologie\b", "technology"),
        (r"\buniversité\b", "university"),
        (r"\benseignement supérieur\b", "higher education"),
        (r"\bministère\b", "ministry"),
        (r"\bm\. v\.\b", "etc."),
    ]

    for pattern, replacement in glossary_patterns:
        translated = re.sub(pattern, replacement, translated, flags=re.IGNORECASE)
    return translated

