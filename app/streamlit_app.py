"""
Innovation Policy Dataset — Research Dashboard
Run:  streamlit run app/streamlit_app.py
"""

import io
import sys
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.data_loader import (
    ACTOR_LABELS, ORIENTATION_COLORS, ORIENTATION_LABELS,
    RD_CATEGORY_COLORS, RD_CATEGORY_LABELS,
    REFORM_PANEL, STAGE_LABELS, STAGE_PATHS, STATUS_LABELS,
    SUBTHEME_COLORS, SUBTHEME_LABELS, SUBTHEME_SHORT,
    budget_available, get_app_password, load_budget, load_korea_theme_panel,
    load_budget_country_gap_report, load_budget_country_gap_review_table,
    load_budget_country_notes, load_budget_run_log,
    load_budget_gap_deepdive_detail, load_budget_gap_deepdive_summary,
    load_reform_panel, load_reforms,
    load_reform_mentions, load_reform_panel_subtheme,
    reforms_available,
    available_reform_stages, load_reforms_stage, load_reform_panel_stage,
)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Innovation Policy Dataset",
    layout="wide",
    initial_sidebar_state="expanded",
)


def require_password() -> None:
    expected_password = get_app_password()
    if not expected_password:
        return

    if st.session_state.get("app_authenticated") is True:
        return

    st.title("Protected Access")
    st.caption("Enter the application password to continue.")
    password = st.text_input("Password", type="password")
    submitted = st.button("Enter")

    if submitted:
        if password == expected_password:
            st.session_state["app_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()


require_password()

# ─────────────────────────────────────────────────────────────────────────────
# CSS  — pure white everywhere, no dark surfaces
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* ── Reset dark Streamlit chrome ── */
#root > div, .main, .block-container,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewBlockContainer"],
[data-testid="stHeader"],
header[data-testid="stHeader"],
[data-testid="stDecoration"] {
    background-color: #ffffff !important;
    color: #1a1a1a !important;
}
/* Kill the coloured top decoration bar */
[data-testid="stDecoration"] { display: none !important; }
/* Keep the toolbar so the sidebar can always be reopened */
[data-testid="stMainMenuPopover"],
footer { display: none !important; }

/* ── Sidebar ── */
[data-testid="stSidebar"],
[data-testid="stSidebar"] > div:first-child {
    background-color: #F5F7FA !important;
    border-right: 1px solid #DDE1E7 !important;
}
[data-testid="stSidebar"] * { color: #1a1a1a !important; }

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] {
    border-bottom: 2px solid #DDE1E7;
    gap: 0;
}
[data-testid="stTabs"] button[role="tab"] {
    font-size: 0.82rem;
    font-weight: 600;
    color: #555 !important;
    padding: 0.5rem 1.1rem;
    border-radius: 0;
    background: transparent !important;
    border: none !important;
}
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    color: #003189 !important;
    border-bottom: 2px solid #003189 !important;
}

/* ── Buttons ── */
[data-testid="stBaseButton-secondary"],
[data-testid="stDownloadButton"] button,
.stDownloadButton > button {
    background-color: #ffffff !important;
    color: #003189 !important;
    border: 1.5px solid #003189 !important;
    border-radius: 3px !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    padding: 0.35rem 0.9rem !important;
}
[data-testid="stBaseButton-secondary"]:hover,
[data-testid="stDownloadButton"] button:hover {
    background-color: #003189 !important;
    color: #ffffff !important;
}

/* ── Widget labels (uppercase caption style) ── */
[data-testid="stSelectbox"] label,
[data-testid="stMultiSelect"] label,
[data-testid="stSlider"] label,
[data-testid="stRadio"] label,
[data-testid="stCheckbox"] label,
[data-testid="stTextInput"] label {
    font-size: 0.72rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.06em !important;
    color: #444 !important;
}

/* ── Selectbox & Multiselect — white box, dark text ── */
[data-baseweb="select"] > div:first-child,
[data-baseweb="select"] > div {
    background-color: #ffffff !important;
    border-color: #C0C4CC !important;
}
/* Selected value text */
[data-baseweb="select"] [class*="ValueContainer"] *,
[data-baseweb="select"] [class*="singleValue"],
[data-baseweb="select"] [class*="placeholder"],
[data-baseweb="select"] input {
    color: #1a1a1a !important;
    background-color: transparent !important;
}
/* Dropdown menu */
[data-baseweb="menu"],
[data-baseweb="popover"] {
    background-color: #ffffff !important;
    border: 1px solid #C0C4CC !important;
    box-shadow: 0 4px 12px rgba(0,0,0,0.10) !important;
}
[data-baseweb="menu"] li,
[data-baseweb="option"] {
    background-color: #ffffff !important;
    color: #1a1a1a !important;
}
[data-baseweb="menu"] li:hover,
[data-baseweb="option"]:hover {
    background-color: #EEF3FB !important;
    color: #003189 !important;
}

/* ── Multiselect tag pills ── */
[data-baseweb="tag"] {
    background-color: #E8EEF9 !important;
    color: #003189 !important;
    border: 1px solid #C0CFEE !important;
    border-radius: 3px !important;
}
[data-baseweb="tag"] span,
[data-baseweb="tag"] * {
    color: #003189 !important;
    background-color: transparent !important;
}

/* ── Dataframe / table ── */
[data-testid="stDataFrame"],
[data-testid="stDataFrame"] iframe,
.stDataFrame {
    background-color: #ffffff !important;
    border: 1px solid #DDE1E7 !important;
    border-radius: 4px !important;
}
/* Text input (search box in table expanders) */
[data-testid="stTextInput"] input {
    background-color: #ffffff !important;
    color: #1a1a1a !important;
    border: 1px solid #C0C4CC !important;
    border-radius: 3px !important;
}

/* ── Expander ── */
[data-testid="stExpander"] summary {
    font-size: 0.8rem !important;
    font-weight: 600 !important;
    color: #1a1a1a !important;
    background-color: #F5F7FA !important;
    border: 1px solid #DDE1E7 !important;
    border-radius: 3px !important;
    padding: 0.45rem 0.8rem !important;
}
[data-testid="stExpander"] summary:hover {
    background-color: #E8EEF9 !important;
}
[data-testid="stExpander"] details[open] summary {
    border-bottom: 1px solid #DDE1E7 !important;
    border-radius: 3px 3px 0 0 !important;
}

/* ── Typography ── */
body, p, li, td, th {
    font-family: "Source Sans Pro", "Helvetica Neue", Arial, sans-serif !important;
}
/* Do NOT include span/div here — Streamlit renders expander arrows as spans with
   a generated class; overriding font-family breaks the Material Icons ligature rendering */
[data-testid="stIconMaterial"] {
    font-family: "Material Symbols Rounded", "Material Icons", serif !important;
}
h1, h2, h3 { color: #003189 !important; }

/* ── Section dividers ── */
hr { border-color: #DDE1E7 !important; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

NAVY   = "#003189"
BLUE   = "#009FDA"
TEAL   = "#00A389"
GREEN  = "#3D9349"
ORANGE = "#E86B33"
GREY   = "#9B9B9B"
LGREY  = "#F5F7FA"
BORDER = "#DDE1E7"
TEXT   = "#1a1a1a"

BUDGET_CATEGORY_COLORS = {
    "science_agency":          NAVY,
    "direct_rd":               NAVY,
    "research_infrastructure": BLUE,
    "innovation_instruments":  GREEN,
    "higher_education":        TEAL,
    "unclear":                 GREY,
    "other":                   GREY,
}

PLOTLY_BASE = dict(
    template="plotly_white",
    font=dict(family="Source Sans Pro, Helvetica Neue, Arial", size=11.5, color=TEXT),
    plot_bgcolor="#ffffff",
    paper_bgcolor="#ffffff",
)


def _format_korea_source_amount(amount_thousand_krw):
    """Render Korea amounts in reader-friendly source-style units."""
    try:
        amt = float(amount_thousand_krw)
    except Exception:
        return "—"
    if pd.isna(amt):
        return "—"

    jo = amt / 1_000_000_000.0
    eok = amt / 100_000.0

    if jo >= 1:
        if abs(jo - round(jo)) < 1e-9:
            return f"{int(round(jo))}조원"
        if abs(jo * 10 - round(jo * 10)) < 1e-8:
            return f"{jo:.1f}조원"
        return f"{jo:.2f}조원"

    if eok >= 1:
        if abs(eok - round(eok)) < 1e-6:
            return f"{int(round(eok)):,}억원"
        return f"{eok:,.1f}억원"

    return f"{amt * 1000:,.0f}원"


def render_table(df, col_labels=None, max_rows=500, num_cols=None, bool_cols=None, wide_cols=None):
    """Render a styled HTML table — bypasses st.dataframe iframe limitations.

    col_labels : dict mapping raw col name → display header
    num_cols   : list of cols to right-align and format with thousands separator
    bool_cols  : list of boolean cols (renders ✓ / —)
    wide_cols  : list of cols that get extra width (long text)
    """
    import html as _html
    col_labels = col_labels or {}
    num_cols   = set(num_cols or [])
    bool_cols  = set(bool_cols or [])
    wide_cols  = set(wide_cols or [])

    df = df.head(max_rows)

    # Build header
    ths = ""
    for c in df.columns:
        lbl = col_labels.get(c, c.replace("_", " ").title())
        w   = "min-width:220px" if c in wide_cols else ("min-width:80px" if c in num_cols else "min-width:100px")
        ths += (f'<th style="padding:.45rem .7rem;text-align:{"right" if c in num_cols else "left"};'
                f'font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;'
                f'color:{NAVY};white-space:nowrap;{w};">{_html.escape(lbl)}</th>')

    # Build rows
    rows_html = ""
    for i, (_, row) in enumerate(df.iterrows()):
        bg = "#F8F9FC" if i % 2 == 0 else "#ffffff"
        tds = ""
        for c in df.columns:
            val = row[c]
            if c in bool_cols:
                cell = '<span style="color:#3D9349;font-weight:700;">✓</span>' if val else '<span style="color:#aaa;">—</span>'
                align = "center"
            elif c in num_cols:
                try:
                    if "confidence" in c:
                        cell = _html.escape(f"{float(val):.2f}") if pd.notna(val) else "—"
                    else:
                        cell = _html.escape(f"{float(val):,.0f}") if pd.notna(val) else "—"
                except Exception:
                    cell = _html.escape(str(val)) if pd.notna(val) else "—"
                align = "right"
            else:
                raw = str(val) if pd.notna(val) else "—"
                # Truncate very long text with tooltip
                if len(raw) > 120 and c in wide_cols:
                    cell = f'<span title="{_html.escape(raw)}">{_html.escape(raw[:120])}…</span>'
                else:
                    cell = _html.escape(raw)
                align = "left"
            tds += (f'<td style="padding:.38rem .7rem;font-size:.8rem;color:#1a1a1a;'
                    f'vertical-align:top;text-align:{align};border-bottom:1px solid #EEF0F4;">'
                    f'{cell}</td>')
        rows_html += f'<tr style="background:{bg};">{tds}</tr>'

    table_html = f"""
    <div style="overflow-x:auto;overflow-y:auto;max-height:460px;
                border:1px solid #DDE1E7;border-radius:4px;margin-top:.5rem;">
      <table style="width:100%;border-collapse:collapse;font-family:'Source Sans Pro',Arial,sans-serif;">
        <thead style="position:sticky;top:0;background:{LGREY};z-index:1;">
          <tr>{ths}</tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """
    st.markdown(table_html, unsafe_allow_html=True)


def apply_style(fig, height=340, title="", xtitle="", ytitle="",
                legend_bottom=True, xangle=0):
    """Apply uniform OECD-style formatting to a Plotly figure."""
    fig.update_layout(
        height=height,
        title=dict(text=title, font=dict(size=12, color=NAVY, family="Source Sans Pro, Arial"),
                   x=0, pad=dict(b=8)),
        xaxis=dict(title=dict(text=xtitle, font=dict(size=11, color="#444")),
                   tickangle=xangle, showgrid=False,
                   linecolor="#AAAAAA", linewidth=1,
                   tickcolor="#AAAAAA", tickfont=dict(size=10.5, color="#333")),
        yaxis=dict(title=dict(text=ytitle, font=dict(size=11, color="#444")),
                   gridcolor="#E0E0E0", gridwidth=0.8,
                   linecolor="#AAAAAA", linewidth=1,
                   tickfont=dict(size=10.5, color="#333")),
        margin=dict(t=44, b=36, l=8, r=8),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="left", x=0,
            font=dict(size=10.5),
            bgcolor="rgba(0,0,0,0)",
            borderwidth=0,
        ) if legend_bottom else dict(font=dict(size=10.5)),
        **PLOTLY_BASE,
    )
    return fig


def stat_row(items):
    """Render a horizontal KPI strip. items = [(value, label), ...]"""
    cols = st.columns(len(items))
    for col, (val, lbl) in zip(cols, items):
        with col:
            st.markdown(
                f"""<div style="border:1px solid {BORDER}; border-top:3px solid {NAVY};
                    background:#fff; padding:.6rem .9rem; border-radius:0 0 4px 4px;">
                    <div style="font-size:1.5rem;font-weight:800;color:{NAVY};line-height:1.1;">{val}</div>
                    <div style="font-size:.68rem;font-weight:700;text-transform:uppercase;
                         letter-spacing:.06em;color:#777;margin-top:3px;">{lbl}</div>
                </div>""",
                unsafe_allow_html=True,
            )


def section_header(text):
    st.markdown(
        f'<div style="font-size:.72rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.07em;color:#777;border-bottom:1px solid {BORDER};'
        f'padding-bottom:.3rem;margin:1.4rem 0 .7rem;">{text}</div>',
        unsafe_allow_html=True,
    )


def caption_note(text):
    st.markdown(
        f'<div style="font-size:.7rem;color:#888;margin-top:.2rem;">{text}</div>',
        unsafe_allow_html=True,
    )


def _df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()


def _clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _split_pipe_values(value) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    return [part.strip() for part in text.split("|") if part.strip()]


def _uniq_keep_order(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        token = _clean_text(value)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _preview_list(values: list[str], limit: int = 5) -> str:
    values = _uniq_keep_order(values)
    if not values:
        return ""
    if len(values) <= limit:
        return ", ".join(values)
    return ", ".join(values[:limit]) + f", +{len(values) - limit} more"


def _smart_gap_note(
    country: str,
    year: int,
    year_gaps: pd.DataFrame,
    year_review: pd.DataFrame,
    country_summary_row: pd.Series | None,
    country_notes: dict[str, str],
) -> str:
    bullets: list[str] = []

    flagged = year_gaps[year_gaps["gap_type"].fillna("ok") != "ok"].copy()
    affected = _uniq_keep_order(flagged.get("canonical_name", pd.Series(dtype=str)).astype(str).tolist()) if not flagged.empty else []
    source_files = []
    if not year_review.empty and "source_file" in year_review.columns:
        source_files.extend(year_review["source_file"].dropna().astype(str).tolist())
    if not flagged.empty and "raw_row_file" in flagged.columns:
        source_files.extend(flagged["raw_row_file"].dropna().astype(str).tolist())
    source_files = _uniq_keep_order(source_files)

    if flagged.empty:
        bullets.append(f"No flagged `missing` or `outlier` rows are recorded for {country} {year} in the current gap report.")
    else:
        issue_mix = ", ".join(sorted(flagged["gap_type"].dropna().astype(str).unique()))
        bullets.append(
            f"{country} {year} has {len(flagged)} flagged series-year issue(s) across {len(affected)} series: {_preview_list(affected, limit=4)}."
        )
        bullets.append(f"Issue mix: {issue_mix}.")

    if source_files:
        bullets.append(f"Source document(s) checked for this year: {_preview_list(source_files, limit=3)}.")

    if not year_review.empty:
        review = year_review.iloc[0]
        extracted = review.get("run_log_rows_extracted")
        docx_rows = review.get("docx_results_rows")
        audited = review.get("docx_audit_in_series_rows")
        if pd.notna(extracted) or pd.notna(docx_rows) or pd.notna(audited):
            bullets.append(
                "Pipeline evidence: "
                f"run log extracted {int(extracted) if pd.notna(extracted) else 0} rows, "
                f"country results kept {int(docx_rows) if pd.notna(docx_rows) else 0}, "
                f"final audited series kept {int(audited) if pd.notna(audited) else 0}."
            )

        issue_label = _clean_text(review.get("year_issue_label"))
        if issue_label:
            bullets.append(f"Year-level review: {issue_label}.")

        extracted_entities = _split_pipe_values(review.get("extracted_entities"))
        if extracted_entities:
            bullets.append(
                "The document did contain research-like rows, but they look to be sub-lines or non-comparable fragments rather than a defendable final series total: "
                f"{_preview_list(extracted_entities, limit=5)}."
            )

        missing_queue = _split_pipe_values(review.get("missing_agencies_from_queue"))
        if missing_queue:
            bullets.append(f"Still missing from the queue after review: {_preview_list(missing_queue, limit=4)}.")

        diagnosis_excerpt = _clean_text(review.get("diagnosis_excerpt"))
        if diagnosis_excerpt:
            bullets.append(textwrap.shorten(diagnosis_excerpt, width=220, placeholder="…"))

        document_change_note = _clean_text(review.get("document_change_note"))
        if document_change_note:
            bullets.append(textwrap.shorten(document_change_note, width=220, placeholder="…"))

        recommended_action = _clean_text(review.get("recommended_action"))
        if recommended_action:
            bullets.append(f"Recommended next step: {textwrap.shorten(recommended_action, width=220, placeholder='…')}")
    else:
        if not flagged.empty:
            actions = _uniq_keep_order(flagged["action"].dropna().astype(str).tolist()) if "action" in flagged.columns else []
            if actions:
                bullets.append(f"Current pipeline action tag: {', '.join(actions)}.")

        diagnoses = _uniq_keep_order(flagged["diagnosis"].dropna().astype(str).tolist()) if "diagnosis" in flagged.columns else []
        if diagnoses:
            bullets.append(textwrap.shorten(diagnoses[0], width=220, placeholder="…"))

    if country_summary_row is not None:
        summary_excerpt = _clean_text(country_summary_row.get("note_excerpt"))
        if summary_excerpt:
            bullets.append(f"Country note context: {textwrap.shorten(summary_excerpt, width=220, placeholder='…')}")

    source_notes = _clean_text(country_notes.get("source_notes", ""))
    quality_note = _clean_text(country_notes.get("quality_note", ""))
    notes_text = source_notes or quality_note
    if notes_text:
        matching_line = ""
        for line in notes_text.splitlines():
            raw = line.strip().lstrip("-").strip()
            if str(year) in raw or any(name in raw for name in affected[:3]):
                matching_line = raw
                break
        if matching_line:
            bullets.append(f"Source note cross-check: {textwrap.shorten(matching_line, width=220, placeholder='…')}")

    return "\n".join(f"- {bullet}" for bullet in bullets if bullet)


def _gap_issue_label_for_ui(label: str) -> str:
    label = _clean_text(label)
    mapping = {
        "Needs targeted re-extraction": "Processed source needs deeper manual review",
        "Raw rows exist but need reclassification": "Research-like rows found but not retained as final series",
        "Document ran but returned zero rows": "Processed file yielded no usable R&D rows",
        "Agency not named in parsed source": "Research references found, but no defendable target institution line",
        "Aggregate-only OCW Art. 16 for NWO/KNAW": "Only an aggregate research-policy total is visible",
        "extracted rows exist but are not making it into the final series": "Research-like rows found but not retained as final series",
        "document changed structure": "Document structure or comparability break",
        "unsupported format": "Misfiled or non-comparable source file",
    }
    return mapping.get(label, label)


def _short_series_name(label: str) -> str:
    label = _clean_text(label)
    if not label:
        return ""
    return label.split(" — ")[0].strip()


def _diagnosis_claims_unparsed(text: str) -> bool:
    text = _clean_text(text).lower()
    if not text:
        return False
    needles = [
        "year not in raw_rows",
        "not in raw_rows",
        "not parsed yet",
        "may not be parsed yet",
        "documents may not be parsed yet",
        "probably not parsed yet",
    ]
    return any(needle in text for needle in needles)


@st.cache_data
def _load_budget_country_docx_series(country: str) -> pd.DataFrame:
    root = Path(__file__).resolve().parent.parent / "Data" / "output" / "budget" / str(country)
    path = root / f"{str(country).lower()}_docx_series.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _year_docx_series_fragments(country: str, year: int) -> list[str]:
    df = _load_budget_country_docx_series(country)
    if df.empty or "year" not in df.columns:
        return []
    year_df = df[df["year"] == year].copy()
    if year_df.empty:
        return []

    fragments: list[str] = []
    for _, row in year_df.iterrows():
        note = _clean_text(row.get("series_notes"))
        if not note:
            continue
        short_name = _short_series_name(row.get("canonical_name"))
        tail = [part.strip() for part in note.split(";") if part.strip()]
        detail = tail[-1] if tail else note
        if detail.lower().startswith("gap:"):
            detail = detail[4:].strip()
        if short_name:
            fragments.append(f"{short_name}: {detail}.")
        else:
            fragments.append(f"{detail}.")
    return _uniq_keep_order(fragments)


# ── Gap investigation knowledge base ─────────────────────────────────────────
# For each (country, year) calendar gap: what was tried and what the conclusion is.
_GAP_KB: dict[tuple[str, int], dict] = {}

def _build_gap_kb() -> dict[tuple[str, int], dict]:
    """Build the manual gap investigation knowledge base."""
    def _expand(country, years_spec, entry):
        out = {}
        if isinstance(years_spec, range):
            for y in years_spec:
                out[(country, y)] = entry
        elif isinstance(years_spec, (list, tuple)):
            for y in years_spec:
                out[(country, y)] = entry
        else:
            out[(country, years_spec)] = entry
        return out

    kb: dict[tuple[str, int], dict] = {}

    def add(country, years_spec, tried, conclusion, fixable):
        kb.update(_expand(country, years_spec, {"tried": tried, "conclusion": conclusion, "fixable": fixable}))

    add("Italy", range(1998, 2009),
        "Previous LLM extraction covered only 1986–1997 and 2009–2024. Source PDFs for 1998–2008 exist in corpus (Italian Finance Bills — Gazzetta Ufficiale, e.g. '1998 19971230_302_SO_255.pdf'). These years have not been attempted yet.",
        "POTENTIALLY FIXABLE — Source PDFs exist for all years 1998–2008. Needs targeted re-extraction with prompts focusing on 'dotazione ordinaria' or annual institutional appropriation lines (not extraordinary contributions or multiyear authorizations).",
        "Potentially")

    add("Italy", range(2009, 2026),
        "LLM extraction ran but amounts are 12–25× too large (e.g. CNR 2013 = 16.1B EUR vs. expected ~1B). Root cause: LLM reads extraordinary contributions, multiyear authorization totals, or mission-level aggregates instead of annual institutional appropriation lines. 'modern_caps' filter in canonical_series.py blocks these inflated values from rd_database.csv.",
        "POTENTIALLY FIXABLE — Re-extract with prompt targeting annual 'dotazione ordinaria' or 'fondo ordinario' line items. Clean-factor scaling is not possible since the error ratio varies by year and agency.",
        "Potentially")

    add("Germany", range(2008, 2010),
        "_GERMANY_MANUAL_DROP_ROWS in canonical_series.py drops DFG, HGF, Fraunhofer, MPG, WGL, DLR for 2003–2009 because the LLM misassigned the BMBF chapter total (~1.93B EUR constant) to every individual agency. All agencies show the same value confirming a single chapter-total repeated.",
        "POTENTIALLY FIXABLE via re-extraction — Would require targeted re-extraction with prompts identifying individual Titel/Kapitel lines within Einzelplan 30 (BMBF chapter). Current extraction is correctly blocked.",
        "Potentially")

    add("Germany", range(2010, 2022),
        "Bundesgesetzblatt PDFs exist for 2010–2020 but contain only the enacted budget law (article-level totals). The R&D agency-level breakdown requires Einzelplan 30 (BMBF Haushaltsplan) which is a separate document NOT in our corpus. VA-Band3 supplementary detail only available for 2021.",
        "NOT FIXABLE without Einzelplan 30 PDFs — Bundesgesetzblatt files in corpus contain only enacted totals, not agency-level chapter breakdowns. Would need BMBF Haushaltsplan PDFs for 2010–2020 added to corpus.",
        "No")

    add("Germany", [2022, 2023, 2024],
        "Same structural problem as 2010–2020: Bundesgesetzblatt contains only enacted totals. BMBF Haushaltsplan Einzelplan 30 PDFs needed for agency-level breakdowns.",
        "NOT FIXABLE without Einzelplan 30 PDFs for 2022–2024.",
        "No")

    add("Portugal", range(1988, 1998),
        "Portuguese Finance Bills (Lei do Orçamento) for 1988–1997 exist as PDFs in corpus (e.g. 'Lei orcamento para 1988.pdf'). Pipeline has not run extraction on these years. JNICT (Junta Nacional de Investigação Científica e Tecnológica) was main R&D funding agency 1967–1997.",
        "POTENTIALLY FIXABLE — PDFs exist for all years. Could re-extract targeting JNICT annual appropriation lines. Note: 1987, 1992, 1994 already have verified overrides from known JNICT data.",
        "Potentially")

    add("Portugal", [2000, 2001],
        "PDFs for 2000 ('Lei orcamento para 2000.pdf') and 2001 ('2001 8e5ae31a...pdf') exist in corpus. FCT was created in 1997. Pipeline may not have cleanly extracted MAPA VII annual appropriation values for these transition years.",
        "POTENTIALLY FIXABLE — PDFs exist. Re-extract targeting FCT annual appropriation in MAPA VII autonomous services table.",
        "Potentially")

    add("Portugal", [2002, 2003, 2004, 2005],
        "Results.csv has data for 2002–2005 but portugal_manual_drops blocks them. Extracted amounts (352–634M EUR, labeled 'unit EUR') come from plurianual commitment authorization tables rather than annual appropriations. MAPA VII methodology requires only annual dotation lines.",
        "NOT FIXABLE from current source — extracted amounts represent cumulative authorizations not annual appropriations. A different table within the same PDF could theoretically have annual figures but was not identified.",
        "No")

    add("Portugal", [2008],
        "No PDF for Portugal 2008 Finance Bill found in our corpus.",
        "NOT FIXABLE — source document not available in corpus.",
        "No")

    add("Turkey", [1979, 1980, 1981],
        "Turkey 1979–1981 rows in results.csv are transfer amounts paid to TÜBİTAK by other ministries ('tübitaka ödenecektir') — not TÜBİTAK's own institutional budget appropriation. Correctly excluded by turkey_drop_pairs in canonical_series.py.",
        "POTENTIALLY FIXABLE via re-extraction — the correct TÜBİTAK budget line needs to be found in TÜBİTAK's own chapter, not as a transfer line in another ministry's section.",
        "Potentially")

    add("Turkey", [1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2001, 2002, 2003, 2004],
        "Source PDFs (Turkish Budget Law Resmi Gazete) exist for these years (e.g. '1983 17895.pdf' through '1990 20388.pdf'). Turkey data successfully extracted for 1975–1983 and 2005–2009. The 1983–2004 period appears to have extraction gaps.",
        "POTENTIALLY FIXABLE — PDFs exist for 1983–1990 at minimum. Turkish budget shows TÜBİTAK as a line item in Ministry of Education. Requires re-extraction with Turkish-language prompts targeting TÜBİTAK appropriation lines.",
        "Potentially")

    add("Turkey", [2000],
        "Turkey 2000 appears in results.csv but was excluded via turkey_drop_pairs. Source PDF exists in corpus.",
        "POTENTIALLY FIXABLE — Needs investigation of what the 2000 extraction found and why it was excluded. May be a transfer amount issue similar to 1979–1981.",
        "Potentially")

    add("Turkey", [2005],
        "Turkey 2005 blocked by turkey_drop_pairs for TÜBİTAK and TAEK. The 2005 extraction was identified as problematic during audit (likely transfer amount or scale issue).",
        "POTENTIALLY FIXABLE — if correct annual institutional budget lines can be identified in the 2005 PDF.",
        "Potentially")

    add("France", range(1966, 2025),
        "France has 21 gaps at years 1971, 1975, 1976, 1980, 1989, 1991–1993, 1995–2004, 2006, 2018–2019. French Finance Bills (PLF/LFI) are well-structured with MIRES mission chapters. Source docs exist for most gap years (56 files in corpus). Some gaps reflect parsing failures or years where agency-level data wasn't cleanly separated.",
        "POTENTIALLY FIXABLE — French budget structure is well-organized. Source docs available. Re-extraction for specific gap years (especially recent ones 2018–2019) should recover missing data.",
        "Potentially")

    add("Poland", range(1992, 2025),
        "Poland has 18 gaps at years 1994–2009, 2012, 2018, 2021–2022. Polish Finance Bills (Ustawa budzetowa) available (37 files in corpus). KBN (1992–2005) and NCN/NCBiR (2011+) key agencies. Some gap years not attempted.",
        "POTENTIALLY FIXABLE — Source docs likely available for most gaps. Systematic re-extraction for gap years should recover data.",
        "Potentially")

    add("UK", range(1998, 2025),
        "UK has 12 gaps (2000–2003, 2008–2009, 2011–2012, 2014, 2016, 2019, 2022). UK Supply Estimates don't present R&D as institutional block grants in same way as continental systems. RCUK/UKRI receive block grants visible in BEIS/DfE Supply Estimates but structure is complex. 53 files in corpus.",
        "HARD TO FIX — UK Supply Estimates have complex multi-document structure. Missing years reflect both extraction failures and structural format differences. UK R&D budget is distributed across multiple departmental estimates, not a single chapter.",
        "Uncertain")

    add("Estonia", range(1992, 2025),
        "Estonia has 13 gaps (1993, 2012–2021, 2023–2024). 35 files in corpus. ETF (Eesti Teadusfond) and MKM (Ministry of Economic Affairs) key R&D agencies. Gap years not yet attempted for extraction.",
        "POTENTIALLY FIXABLE — Source docs likely available for most gaps. Systematic re-extraction needed.",
        "Potentially")

    add("Latvia", range(1993, 2025),
        "Latvia has 13 gaps (1994, 2002, 2004–2005, 2007–2008, 2010–2014, 2016–2017). 38 files in corpus. LZP (Latvian Council of Science) key R&D agency. Gap years appear not to have been attempted.",
        "POTENTIALLY FIXABLE — Source docs likely available.",
        "Potentially")

    add("Sweden", range(1975, 2025),
        "Sweden has 9 gaps (1976–1979, 1996, 2006, 2008, 2012, 2015). Swedish Finance Bills (Budgetproposition) are very large multi-volume PDFs. 49 files in corpus. Previous session noted files may be too large for LLM extraction context window.",
        "UNCERTAIN — Large PDF file size may prevent extraction. Some gaps may be recoverable if specific VINNOVA/VR chapters can be isolated from multi-volume PDF.",
        "Uncertain")

    add("New Zealand", range(1970, 2010),
        "New Zealand has 9 gaps (1977, 1984, 1988, 1991–1995, 2004). 52 files in corpus. FRST (Foundation for Research, Science and Technology) and MoRST key agencies. Source docs likely in corpus for covered period.",
        "POTENTIALLY FIXABLE — Source docs likely available. NZ budget structure clearly identifies science funding agencies.",
        "Potentially")

    add("Colombia", range(2000, 2025),
        "Colombia has 8 gaps (2003, 2006–2011, 2015). 41 files in corpus. Colciencias (now MinCiencias) key R&D agency. Source docs likely exist for gap years.",
        "POTENTIALLY FIXABLE — Source docs likely available.",
        "Potentially")

    add("Costa Rica", range(2010, 2025),
        "Costa Rica has 8 gaps (2012, 2014–2016, 2018–2019, 2022–2023). 25 files in corpus. CONICIT/MICITT key R&D agencies. Source docs likely available.",
        "POTENTIALLY FIXABLE — Source docs likely available.",
        "Potentially")

    add("Switzerland", range(1975, 2025),
        "Switzerland has 8 gaps (1979–1982, 1990, 1992–1993, 2002). 56 files in corpus. SNF (Swiss National Science Foundation) and ETH Domain key R&D agencies. Source docs likely available.",
        "POTENTIALLY FIXABLE — Source docs likely available.",
        "Potentially")

    add("Hungary", range(1998, 2012),
        "Hungary has 7 gaps (2000, 2002–2004, 2006, 2008–2009). 34 files in corpus. OTKA (Hungarian Scientific Research Fund) and NKTH key agencies. Source docs likely exist.",
        "POTENTIALLY FIXABLE — Source docs likely available.",
        "Potentially")

    add("Spain", range(1988, 2025),
        "Spain has 6 gaps (1991, 1994–1995, 1997, 2003, 2019). 45 files in corpus. CSIC and AEI (Agencia Estatal de Investigación) key R&D agencies. Source docs likely available.",
        "POTENTIALLY FIXABLE — Source docs likely available.",
        "Potentially")

    add("Mexico", range(2000, 2025),
        "Mexico has 5 gaps (2004–2005, 2019, 2022–2023). 68 files in corpus. CONACYT (now CONAHCYT) key R&D agency. Source docs likely exist.",
        "POTENTIALLY FIXABLE — Source docs likely available.",
        "Potentially")

    add("Finland", [1990, 1991, 1994, 1995],
        "Finland has 4 gaps. 39 files in corpus but no documents matching 1990, 1991, 1994, or 1995. Finnish Academy and TEKES (now Business Finland) key agencies.",
        "NOT FIXABLE without source documents — files not in corpus for these years.",
        "No")

    add("Israel", [1993, 2003, 2006, 2020],
        "Israel has 4 gaps. 57 files in corpus but no documents found matching 1993, 2003, 2006, or 2020.",
        "NOT FIXABLE without source documents — files not in corpus for these specific years.",
        "No")

    add("Czech Republic", [1995, 2010, 2011],
        "Czech Republic has 3 gaps. 33 files in corpus. GACR (Czech Science Foundation) key agency. Source docs likely available for 2010–2011.",
        "POTENTIALLY FIXABLE — Source docs likely exist.",
        "Potentially")

    add("Australia", [2000, 2001],
        "Australia has 2 gaps. 368 files in corpus. ARC (Australian Research Council) and NHMRC key agencies. Source docs exist in corpus.",
        "POTENTIALLY FIXABLE — Source docs exist in corpus.",
        "Potentially")

    add("Canada", [2004, 2005],
        "Canada has 2 gaps. 134 files in corpus but no documents found matching 2004 or 2005.",
        "NOT FIXABLE without source documents.",
        "No")

    add("Lithuania", [1995, 2000],
        "Lithuania has 2 gaps. 35 files in corpus. LMT (Research Council of Lithuania) key agency.",
        "POTENTIALLY FIXABLE.",
        "Potentially")

    add("Slovakia", [2023, 2024],
        "Slovakia has 2 recent gaps. 38 files in corpus. APVV (Slovak Research and Development Agency) key agency. Recent years — source docs may be available.",
        "POTENTIALLY FIXABLE — Recent years, source docs may be available.",
        "Potentially")

    add("Austria", [1987],
        "Austria has 1 gap (1987). 94 files in corpus. FWF and FFG key agencies. Source doc likely in corpus.",
        "POTENTIALLY FIXABLE — Source doc likely in corpus, needs re-extraction.",
        "Potentially")

    add("Belgium", [2003],
        "Belgium has 1 gap (2003). 34 files in corpus but no document found for 2003 specifically. Note: Belgium 2008 had separate issue where all extracted amounts were empty.",
        "NOT FIXABLE — source document for 2003 not in corpus.",
        "No")

    add("Denmark", [2002],
        "Denmark has 1 gap (2002). 50 files in corpus. Forskningsministeriet key agency. No 2002 source document found in corpus.",
        "NOT FIXABLE — source document for 2002 not in corpus.",
        "No")

    add("Korea", [2020],
        "Korea has 1 gap (2020). 21 files in corpus. NRF (National Research Foundation) key agency. No source document found matching 2020.",
        "NOT FIXABLE — source document for 2020 not in corpus.",
        "No")

    add("Netherlands", [1996],
        "Netherlands has 1 gap (1996). 170 files in corpus. NWO (Dutch Research Council) key agency. Source doc likely in corpus.",
        "POTENTIALLY FIXABLE — Source doc likely available.",
        "Potentially")

    add("Slovenia", [2014],
        "Slovenia has 1 gap (2014). 53 files in corpus. ARRS (Slovenian Research Agency) key agency. Source doc likely available.",
        "POTENTIALLY FIXABLE — Source doc likely available.",
        "Potentially")

    return kb


_GAP_KB = _build_gap_kb()


def _gap_investigation_lookup(country: str, year: int) -> tuple[str, str, str]:
    """Return (tried, conclusion, fixable) from knowledge base for a gap."""
    entry = _GAP_KB.get((country, year))
    if entry:
        return entry["tried"], entry["conclusion"], entry["fixable"]
    return "", "", ""


def _budget_gap_explorer_detail(
    country: str,
    year: int,
    year_review: pd.DataFrame,
    year_gap_report: pd.DataFrame,
    year_run_log: pd.DataFrame,
) -> tuple[str, str]:
    if not year_review.empty:
        _labels = _uniq_keep_order(
            [_gap_issue_label_for_ui(v) for v in year_review.get("year_issue_label", pd.Series(dtype=str)).astype(str).tolist()]
        )
        _issue = _labels[0] if _labels else "Reviewed source gap"

        _fragments: list[str] = []
        _diag = _uniq_keep_order(year_review.get("diagnosis_excerpt", pd.Series(dtype=str)).astype(str).tolist())
        _doc_notes = _uniq_keep_order(year_review.get("document_change_note", pd.Series(dtype=str)).astype(str).tolist())
        _entities = _uniq_keep_order(
            sum([_split_pipe_values(v) for v in year_review.get("extracted_entities", pd.Series(dtype=str)).tolist()], [])
        )
        _missing = _uniq_keep_order(
            sum([_split_pipe_values(v) for v in year_review.get("missing_agencies_from_queue", pd.Series(dtype=str)).tolist()], [])
        )

        if _diag:
            _fragments.append(textwrap.shorten(_diag[0], width=220, placeholder="…"))
        if _doc_notes:
            _fragments.append(textwrap.shorten(_doc_notes[0], width=220, placeholder="…"))
        if _entities:
            _fragments.append(
                f"Processed source contains research-related references such as {_preview_list(_entities, limit=4)}."
            )
        if _missing:
            _fragments.append(
                f"The expected final series line is still not clearly recoverable for {_preview_list(_missing, limit=3)}."
            )

        _analysis = " ".join([frag for frag in _fragments if _clean_text(frag)]).strip()
        if _analysis:
            return _issue, _analysis
        return _issue, "The source was reviewed manually, but no defendable final R&D line could be retained for this year."

    _docx_fragments = _year_docx_series_fragments(country, year)
    _ok_runs = (
        year_run_log[year_run_log["status"].astype(str) == "ok"].copy()
        if not year_run_log.empty and "status" in year_run_log.columns else pd.DataFrame()
    )
    _ok_rows = (
        pd.to_numeric(_ok_runs.get("rows_extracted", pd.Series(dtype=float)), errors="coerce").fillna(0)
        if not _ok_runs.empty else pd.Series(dtype=float)
    )
    _ok_rows_total = int(_ok_rows.sum()) if not _ok_rows.empty else 0
    _ok_docs = (
        _uniq_keep_order(_ok_runs.get("source_file", pd.Series(dtype=str)).dropna().astype(str).tolist())
        if not _ok_runs.empty else []
    )

    if not year_gap_report.empty:
        _diagnoses = _uniq_keep_order(year_gap_report.get("diagnosis", pd.Series(dtype=str)).astype(str).tolist())
        _actions = _uniq_keep_order(year_gap_report.get("action", pd.Series(dtype=str)).astype(str).tolist())
        _issue = "Gap under review"
        if "reclassify" in _actions:
            _issue = "Research-like rows found but not retained as final series"
        elif "reextract" in _actions:
            _issue = "Processed source needs deeper manual review"

        _primary_diag = _diagnoses[0] if _diagnoses else ""
        _stale_unparsed = _diagnosis_claims_unparsed(_primary_diag)
        if _stale_unparsed and (not _ok_runs.empty or _docx_fragments):
            if not _ok_runs.empty and _ok_rows_total > 0:
                _issue = "Processed source yielded no retained R&D line"
                _analysis = (
                    f"Run log evidence shows {country} {year} was processed successfully and extracted {_ok_rows_total} row(s)"
                    f"{' from ' + _preview_list(_ok_docs, limit=2) if _ok_docs else ''}, so this is not an unparsed year."
                )
                if _docx_fragments:
                    _analysis += " Country-series evidence: " + " ".join(
                        textwrap.shorten(fragment, width=140, placeholder="…") for fragment in _docx_fragments[:3]
                    )
                else:
                    _analysis += " The current gap means no defendable final R&D appropriation line survived into the audited panel."
                return _issue, _analysis

            if not _ok_runs.empty and _ok_rows_total == 0:
                _issue = "Processed file yielded no usable R&D rows"
                _analysis = (
                    f"Run log evidence shows {country} {year} did run successfully"
                    f"{' on ' + _preview_list(_ok_docs, limit=2) if _ok_docs else ''}, but it returned zero extracted rows."
                )
                if _docx_fragments:
                    _analysis += " Country-series evidence: " + " ".join(
                        textwrap.shorten(fragment, width=140, placeholder="…") for fragment in _docx_fragments[:3]
                    )
                return _issue, _analysis

            _issue = "Processed source yielded no retained R&D line"
            return (
                _issue,
                "Country-series evidence indicates the year was already processed in downstream budget outputs, but no defendable final R&D line survived into the audited panel. "
                + " ".join(textwrap.shorten(fragment, width=140, placeholder="…") for fragment in _docx_fragments[:3]),
            )

        if _diagnoses:
            return _issue, textwrap.shorten(_primary_diag, width=240, placeholder="…")

    if not _ok_runs.empty:
        if _ok_rows_total > 0:
            _analysis = (
                f"Run log evidence shows {country} {year} was processed successfully and extracted {_ok_rows_total} row(s)"
                f"{' from ' + _preview_list(_ok_docs, limit=2) if _ok_docs else ''}, but no final audited R&D line appears in the current panel."
            )
            if _docx_fragments:
                _analysis += " Country-series evidence: " + " ".join(
                    textwrap.shorten(fragment, width=140, placeholder="…") for fragment in _docx_fragments[:3]
                )
            return "Processed source yielded no retained R&D line", _analysis

        _analysis = (
            f"Run log evidence shows {country} {year} did run successfully"
            f"{' on ' + _preview_list(_ok_docs, limit=2) if _ok_docs else ''}, but it returned zero extracted rows."
        )
        if _docx_fragments:
            _analysis += " Country-series evidence: " + " ".join(
                textwrap.shorten(fragment, width=140, placeholder="…") for fragment in _docx_fragments[:3]
            )
        return "Processed file yielded no usable R&D rows", _analysis

    if not year_run_log.empty and "status" in year_run_log.columns:
        _statuses = year_run_log["status"].dropna().astype(str).unique().tolist()
        if len(_statuses) > 0 and all(s == "error" for s in _statuses):
            return (
                "Pipeline processing error",
                f"All logged document runs for {country} {year} ended in error; the gap is not being driven by a clean no-R&D result.",
            )
        if "error" in _statuses:
            return (
                "Mixed processing outcomes",
                "Some files ran and others errored, so the gap reflects mixed document outcomes rather than a clean absence of R&D lines.",
            )

    _direct_files = _inventory_docs_for_year(country, year)
    if not _direct_files:
        _nearby = _nearby_inventory_docs(country, year, window=1)
        if _nearby:
            return (
                "No year-specific source confirmed",
                f"No reviewed run or gap record is linked directly to {country} {year}. The source folder does not contain a file whose name clearly matches {year}; nearest visible files are {_preview_list(_nearby, limit=3)}.",
            )
        return (
            "No year-specific source confirmed",
            f"No reviewed run or gap record is linked directly to {country} {year}, and no source file in the current folder names that year explicitly.",
        )

    return (
        "Source file exists but no reviewed evidence is attached yet",
        f"The source folder contains {_preview_list(_direct_files, limit=3)} for {country} {year}, but this year has not yet been tied to a country review row or explicit gap-report diagnosis.",
    )


def _country_source_files(country: str) -> list[str]:
    root = Path(__file__).resolve().parent.parent / "Data" / "input" / "finance_bills" / str(country)
    if not root.exists():
        return []
    return sorted([p.name for p in root.iterdir() if p.is_file()])


def _inventory_docs_for_year(country: str, year: int) -> list[str]:
    year_token = str(year)
    files = _country_source_files(country)
    return [name for name in files if year_token in name]


def _nearby_inventory_docs(country: str, year: int, window: int = 1) -> list[str]:
    files = _country_source_files(country)
    out: list[str] = []
    for candidate_year in range(year - window, year + window + 1):
        token = str(candidate_year)
        out.extend([name for name in files if token in name])
    return _uniq_keep_order(out)


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — all filters
# ─────────────────────────────────────────────────────────────────────────────

# Defaults (used if data not available or no selection made)
yr_b       = (1975, 1984)
cat_b      = "All"
conf_b     = (0.0, 1.0)
dec_b      = ["include"]
sel_bud_ctry = []
sel_ctry   = []
sel_st     = []
sel_stat   = ["implemented", "legislated"]
sel_svy    = []
only_major = False

def _sidebar_label(text):
    st.markdown(
        f'<div style="font-size:.68rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.07em;color:#777;margin:.6rem 0 .3rem;">{text}</div>',
        unsafe_allow_html=True,
    )

with st.sidebar:
    st.markdown(
        f'<div style="font-size:1.05rem;font-weight:800;color:{NAVY};'
        f'border-bottom:2px solid {NAVY};padding-bottom:.5rem;margin-bottom:1rem;">'
        f'Innovation Policy Dataset</div>',
        unsafe_allow_html=True,
    )

    # ── Stream 1 filters ──
    _sidebar_label("Stream 1 — R&D Budget")
    dec_b = []
    conf_b = None
    if budget_available():
        _db = load_budget()
        _yrs = sorted(_db["year"].unique())
        if not _yrs:
            st.caption("No budget data available.")
        else:
            # Migrate old session state that was pinned to the historical 1975-1984 range.
            _yr_default = (min(_yrs), max(_yrs))
            _yr_state = st.session_state.get("yr_b")
            if _yr_state == (1975, 1984) and _yr_default != (1975, 1984):
                st.session_state["yr_b"] = _yr_default
            yr_b = st.select_slider(
                "Year range", options=_yrs,
                value=(min(_yrs), max(_yrs)), key="yr_b",
                label_visibility="collapsed",
            )
            _bud_ctry_opts = sorted(_db["country"].dropna().unique()) \
                if "country" in _db.columns else ["Denmark"]
            _bud_ctry_default = _bud_ctry_opts[:1] if _bud_ctry_opts else []
            sel_bud_ctry = st.multiselect(
                "Country", _bud_ctry_opts, default=_bud_ctry_default, key="bud_ctry",
            )
            if not sel_bud_ctry:
                st.caption("Select at least one country.")
            _cats = ["All"] + sorted(_db["budget_category"].dropna().astype(str).unique())
            cat_b = st.selectbox(
                "R&D category", _cats, key="cat_b",
                format_func=lambda x: x if x != "All" else "All categories",
            )
            _decisions = sorted(_db["decision"].dropna().astype(str).unique()) if "decision" in _db.columns else []
            if _decisions:
                _default_decisions = ["include"] if "include" in _decisions else _decisions
                dec_b = st.multiselect(
                    "Decision",
                    _decisions,
                    default=_default_decisions,
                    key="dec_b",
                )
            _conf_vals = pd.to_numeric(_db["confidence"], errors="coerce").dropna()
            if not _conf_vals.empty:
                _conf_min = float(_conf_vals.min())
                _conf_max = float(_conf_vals.max())
                conf_b = st.slider(
                    "Confidence",
                    min_value=round(_conf_min, 2),
                    max_value=round(_conf_max, 2),
                    value=(round(_conf_min, 2), round(_conf_max, 2)),
                    step=0.05,
                    key="conf_b",
                )
    else:
        st.caption("No data — run `python main.py --budget-only`")

    st.markdown("<hr style='margin:.7rem 0;'>", unsafe_allow_html=True)

    # ── Stream 2 filters ──
    _sidebar_label("Stream 2 — Reforms")
    _avail_stages = available_reform_stages()

    if _avail_stages:
        # Stage selector — only show stages that have data on disk
        _stage_keys   = list(_avail_stages.keys())
        _stage_labels = list(_avail_stages.values())
        _default_stage = "merged" if "merged" in _avail_stages else _stage_keys[-1]
        _sel_stage_label = st.radio(
            "Extraction stage",
            _stage_labels,
            index=_stage_labels.index(_avail_stages[_default_stage]),
            key="reform_stage",
        )
        sel_stage = _stage_keys[_stage_labels.index(_sel_stage_label)]

        # For merged stage: option to show excluded reforms too
        show_excluded = False
        if sel_stage == "merged":
            show_excluded = st.checkbox("Include LLM-excluded reforms", key="show_excl")

        _dr = load_reforms_stage(sel_stage, included_only=not show_excluded)

        if _dr.empty:
            st.caption("No data for this stage yet.")
            sel_ctry = []; sel_st = []; sel_stat = []; sel_svy = []
            sel_verif = []
            only_major = False; yr_r = (1995, 2025)
        else:
            _ctry = sorted(_dr["country_name"].dropna().unique()) if "country_name" in _dr.columns else []
            sel_ctry = st.multiselect("Country", _ctry, default=_ctry, key="ctry")

            _st_opts = sorted(_dr["sub_theme"].dropna().unique()) if "sub_theme" in _dr.columns else []
            sel_st = st.multiselect(
                "Innovation type", _st_opts, default=_st_opts, key="st_filt",
                format_func=lambda x: SUBTHEME_LABELS.get(x, x),
            )
            _stat_opts = sorted(_dr["status"].dropna().unique()) if "status" in _dr.columns else []
            sel_stat = st.multiselect(
                "Status", _stat_opts, default=_stat_opts, key="stat_filt",
                format_func=lambda x: STATUS_LABELS.get(x, x),
            )
            _svy_opts = sorted(_dr["survey_year"].dropna().astype(int).unique()) if "survey_year" in _dr.columns else []
            sel_svy = st.multiselect("Survey year", _svy_opts, default=_svy_opts, key="svy_filt")
            if sel_stage == "merged" and "verification_bucket" in _dr.columns:
                _verif_opts = [v for v in _dr["verification_bucket"].dropna().unique() if str(v).strip()]
                _verif_order = [
                    "All 3 models agreed",
                    "2 of 3 models",
                    "Both models agreed",
                    "1 model only",
                    "Excluded — all 3 models agreed",
                    "Excluded — 2 of 3 models",
                    "Excluded — both models agreed",
                    "Excluded — 1 model only",
                ]
                _verif_opts = [v for v in _verif_order if v in _verif_opts]
                sel_verif = st.multiselect(
                    "Verification",
                    _verif_opts,
                    default=_verif_opts,
                    key="verif_filt",
                )
            else:
                sel_verif = []
            only_major = st.checkbox("Major reforms only", key="maj_filt")

            _ref_yrs = (
                sorted(_dr["display_year"].dropna().astype(int).unique())
                if "display_year" in _dr.columns else []
            )
            if len(_ref_yrs) > 1:
                yr_r = st.select_slider(
                    "Year range", options=_ref_yrs,
                    value=(min(_ref_yrs), max(_ref_yrs)), key="yr_r",
                    label_visibility="collapsed",
                )
            else:
                yr_r = (_ref_yrs[0], _ref_yrs[0]) if _ref_yrs else (1995, 2025)

            # Coverage note
            _panel_df_sb = load_reform_panel_stage(sel_stage)
            if not _panel_df_sb.empty and "reform_count" in _panel_df_sb.columns:
                _countries_with_data = sorted(
                    _panel_df_sb[_panel_df_sb["reform_count"] > 0]["country_code"].unique()
                )
                _total_panel = _panel_df_sb["country_code"].nunique()
                st.markdown(
                    f'<div style="font-size:.67rem;color:#888;margin-top:.4rem;">'
                    f'<b style="color:#555;">{len(_countries_with_data)}</b> of {_total_panel} countries '
                    f'have data in this stage.</div>',
                    unsafe_allow_html=True,
                )
    else:
        _dr = pd.DataFrame()
        sel_stage = "stage1"
        show_excluded = False
        sel_ctry = []; sel_st = []; sel_stat = []; sel_svy = []
        sel_verif = []
        only_major = False; yr_r = (1995, 2025)
        st.caption("No data — run `python main.py --reforms-only`")

    st.markdown("<hr style='margin:.7rem 0;'>", unsafe_allow_html=True)
    st.markdown(
        f'<div style="font-size:.68rem;color:#aaa;">OECD Innovation Policy Pipeline</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE HEADER
# ─────────────────────────────────────────────────────────────────────────────

# Dynamic subtitle based on what data is actually loaded
_hdr_parts = []
if budget_available():
    _db_hdr = load_budget()
    _bud_countries = sorted(_db_hdr["country"].dropna().unique()) if "country" in _db_hdr.columns else ["Denmark"]
    _bud_yrs = sorted(_db_hdr["year"].dropna().unique())
    _bud_span = f"{min(_bud_yrs)}–{max(_bud_yrs)}" if _bud_yrs else ""
    _hdr_parts.append(f"Finance Bills: {', '.join(_bud_countries)} {_bud_span}")
if not _dr.empty:
    _dr_hdr = _dr
    _ref_countries = sorted(_dr_hdr["country_name"].dropna().unique()) if "country_name" in _dr_hdr.columns else []
    _ref_surv = sorted(_dr_hdr["survey_year"].dropna().astype(int).unique()) if "survey_year" in _dr_hdr.columns else []
    _ref_span = f"{min(_ref_surv)}–{max(_ref_surv)}" if _ref_surv else ""
    if _ref_countries:
        _hdr_parts.append(f"OECD Surveys: {', '.join(_ref_countries[:5])}{'…' if len(_ref_countries) > 5 else ''} {_ref_span}")
_subtitle = "  ·  ".join(_hdr_parts) if _hdr_parts else "R&D budget allocation &amp; structural reform tracking"

st.markdown(
    f'<h1 style="font-size:1.4rem;font-weight:800;color:{NAVY};'
    f'border-bottom:3px solid {NAVY};padding-bottom:.5rem;margin-bottom:1.2rem;">'
    f'Innovation Policy Dataset'
    f'<span style="font-size:.85rem;font-weight:400;color:#777;margin-left:.8rem;">'
    f'{_subtitle}</span>'
    f'</h1>',
    unsafe_allow_html=True,
)

# ── Pre-compute filtered reforms dataframe (shared across all tabs) ──
if not _dr.empty:
    _dr_all = _dr
    dr_f = _dr_all.copy()
    if sel_ctry:  dr_f = dr_f[dr_f["country_name"].isin(sel_ctry)]
    if sel_st:    dr_f = dr_f[dr_f["sub_theme"].isin(sel_st)]
    if sel_stat:  dr_f = dr_f[dr_f["status"].isin(sel_stat)]
    if sel_svy and "survey_year" in dr_f.columns:
        dr_f = dr_f[dr_f["survey_year"].isin(sel_svy)]
    if sel_verif and "verification_bucket" in dr_f.columns:
        dr_f = dr_f[dr_f["verification_bucket"].isin(sel_verif)]
    if only_major and "is_major_reform" in dr_f.columns:
        dr_f = dr_f[dr_f["is_major_reform"] == True]  # noqa: E712
    if "display_year" in dr_f.columns:
        dr_f = dr_f[
            dr_f["display_year"].isna() |
            ((dr_f["display_year"] >= yr_r[0]) & (dr_f["display_year"] <= yr_r[1]))
        ]
else:
    dr_f = pd.DataFrame()


def _filtered_budget_df() -> pd.DataFrame:
    if not budget_available():
        return pd.DataFrame()
    _db = load_budget()
    if _db.empty:
        return _db
    _m = (_db["year"] >= yr_b[0]) & (_db["year"] <= yr_b[1])
    if dec_b and "decision" in _db.columns:
        _m &= _db["decision"].isin(dec_b)
    if cat_b != "All":
        _m &= _db["budget_category"] == cat_b
    if sel_bud_ctry and "country" in _db.columns:
        _m &= _db["country"].isin(sel_bud_ctry)
    if conf_b is not None and "confidence" in _db.columns:
        _conf = pd.to_numeric(_db["confidence"], errors="coerce")
        _m &= _conf.between(conf_b[0], conf_b[1], inclusive="both")
    return _db[_m].copy()


def _budget_currency_color_map(values: list[str]) -> dict[str, str]:
    _fallback = [NAVY, ORANGE, TEAL, GREEN, BLUE, "#9B59B6", "#E74C3C", GREY]
    _base = {
        "EUR": ORANGE,
        "SKK": NAVY,
        "SIT": NAVY,
        "LTL": "#1F4E79",
        "TAL": "#C0392B",
        "FIM": "#006D77",
        "FRF": TEAL,
        "DEM": "#9B59B6",
        "NLG": GREEN,
        "DKK": BLUE,
        "NOK": "#E74C3C",
        "CHF": "#F39C12",
        "SEK": "#27AE60",
        "GBP": "#1ABC9C",
        "JPY": "#8E44AD",
        "CAD": "#E67E22",
        "AUD": "#16A085",
    }
    out = {}
    for i, v in enumerate(values):
        key = str(v)
        code = key
        if "(" in key and ")" in key:
            inner = key.rsplit("(", 1)[-1].split(")", 1)[0].strip()
            if inner:
                code = inner
        out[v] = _base.get(code, _base.get(key, _fallback[i % len(_fallback)]))
    return out


def _budget_currency_label(value: str) -> str:
    code = str(value or "").strip().upper()
    labels = {
        "EUR": "EUR — euro",
        "SKK": "SKK — Slovak koruna",
        "FIM": "FIM — Finnish markka",
        "FRF": "FRF — French franc",
        "DEM": "DEM — Deutsche Mark",
        "NLG": "NLG — Dutch guilder",
        "DKK": "DKK — Danish krone",
        "NOK": "NOK — Norwegian krone",
        "SEK": "SEK — Swedish krona",
        "CHF": "CHF — Swiss franc",
        "GBP": "GBP — pound sterling",
        "JPY": "JPY — yen",
        "CAD": "CAD — Canadian dollar",
        "AUD": "AUD — Australian dollar",
        "LTL": "LTL — Lithuanian litas",
        "SIT": "SIT — Slovenian tolar",
        "TAL": "TAL — Lithuanian talonas",
    }
    return labels.get(code, code or "Unknown currency")

TAB_BUDGET, TAB_REFORMS, TAB_COMBINED, TAB_TABLE, TAB_METHODS = st.tabs([
    "R&D Budget",
    "Innovation Reforms",
    "Combined View",
    "Data Table",
    "Methodology",
])


# ═════════════════════════════════════════════════════════════════════════════
# TAB 1 — R&D BUDGET
# ═════════════════════════════════════════════════════════════════════════════

with TAB_BUDGET:
    if not budget_available():
        st.info("Run `python main.py --budget-only` to generate budget data.")
        st.stop()

    st.caption("R&D budget module: work in progress. Coverage and classifications may still change as extractors are refined.")

    # ── Budget data quality notes (shown before charts) ──
    _BUD_DOC_QUALITY = {
        "Australia": {
            "years": "1975–2026",
            "source": "Commonwealth Budget Papers and federal portfolio budget documents",
            "gaps": (
                "Long-run coverage exists in the panel, but quality is uneven across eras. Older scanned "
                "documents are OCR-sensitive, and portfolio restructurings can break continuity between "
                "department-level and agency-level series. Smaller sub-lines should be treated cautiously."
            ),
            "fit": "Moderate — useful for broad trends, strongest where named science agencies are explicit.",
            "rating": "moderate",
        },
        "Austria": {
            "years": "1976–2026",
            "source": (
                "Austrian federal budget laws and annex tables (Bundesvoranschlag / Bundesfinanzgesetz), "
                "with compile-side cleaning and a small set of manual verified overrides traced to original budget pages"
            ),
            "gaps": (
                "Austria is intentionally presented as a conservative, traceability-first institutional panel. "
                "The final app-ready series keeps only 162 audited observations across 10 active series, all stored in full "
                "currency units (`schilling` before the euro era and `euro` afterward) rather than `thousand`. "
                "Several tempting rows in the source family were deliberately excluded because they came from chapter summaries, "
                "performance/KPI pages, or generic funding buckets rather than clean institution-level appropriations. "
                "The strongest verified overrides are the ÖAW rows for 1996–1999 and 2012. Remaining gaps are mostly structural: "
                "many older PDFs expose only scan-heavy annexes such as staffing or systemisation tables, while several modern "
                "pages mention FWF, ÖAW, FFG, ISTA, CERN, or ESA only narratively rather than as explicit budget lines."
            ),
            "fit": (
                "Moderate — strong for a small audited institutional backbone and transparent source tracing; "
                "not suitable as a dense continuous ledger of all Austrian public R&D appropriations."
            ),
            "rating": "moderate",
        },
        "Belgium": {
            "years": "1994–2025",
            "source": "Federal finance laws and science-policy budget annexes in French and Dutch",
            "gaps": (
                "The current Belgian panel is usable but still narrow. It captures a small federal science-policy "
                "core and a few named institutions, while many years in the broader corpus still sit outside the "
                "final canonical panel. Several late-year BELSPO files in the current source set behave like legal "
                "text or narrative programme pages rather than clean numeric annexes, so 2008 and 2022–2024 remain "
                "genuine source/extraction gaps. The panel should be treated as federal-only and not as a complete "
                "view of Belgian public R&D across regional and community systems."
            ),
            "fit": "Limited to moderate — strongest for audited federal science-policy lines and a small set of named federal institutions.",
            "rating": "limited",
        },
        "Canada": {
            "years": "1987–2024",
            "source": "Appropriation Acts, Main Estimates, and federal budget tables",
            "gaps": (
                "This is one of the strongest country panels, but older years still require care. "
                "Pre-2000 and some mid-2000s documents can contain OCR/column-bleed errors, bilingual "
                "duplicates, and generic programme rows. Major anomalies were manually reviewed and many "
                "were corrected, but a few historical programme-style series remain less secure than the "
                "core agencies."
            ),
            "fit": "Good — strongest for named federal science agencies and the 2000s onward.",
            "rating": "good",
        },
        "Colombia": {
            "years": "2002–2025 (usable years are discontinuous)",
            "source": (
                "Budget laws (Ley), plus decree/annex budget tables for modern years; "
                "amounts stored in full Colombian pesos (COP)"
            ),
            "gaps": (
                "The current Colombia panel is usable as a traced institutional series, but not as a continuous "
                "annual budget history. Several early files are legal-wrapper texts rather than full budget tables, "
                "and some historical years only mention SENA-to-COLCIENCIAS transfers without a recoverable "
                "institutional appropriation amount. Modern coverage improves substantially when decree/annex files "
                "are used: 2019–2025 depends heavily on those sources rather than on the Ley text alone. The main "
                "documented non-recoverable years in the current file inventory are 2007, 2009, 2010, and 2015; "
                "1996, 1997, 1998, and 2003 also behave like weak wrapper-only sources."
            ),
            "fit": (
                "Moderate — strongest for COLCIENCIAS/MinCiencias and named science institutions; "
                "long-run trends should be read as a documented but discontinuous institutional panel."
            ),
            "rating": "moderate",
        },
        "Czech Republic": {
            "years": "1994–2024, with audited institutional gaps",
            "source": "State budget acts, annex budget tables, and audited text-cache reconstructions from Czech budget files",
            "gaps": (
                "The Czech panel is intentionally conservative and fully traceable to individual source files and page "
                "numbers. The current app-ready series keeps only audited institutional lines: the Czech Academy of "
                "Sciences (AV ČR), the Czech Science Foundation (GA ČR), and, where the chapter totals are explicit, "
                "the Technology Agency (TA ČR). Several modern observations were manually recovered from chapter "
                "summary blocks that state an explicit 'Vydaje celkem' total. Generic R&D/innovation totals, zero-value "
                "placeholders, legal-structure rows, misfiled 1993 material, and ambiguous scale/OCR candidates remain "
                "excluded from the final panel. Coverage is now strong through the audited early series, the recovered "
                "2012–2015 chapter totals, and the audited modern block from 2016–2024. The remaining gaps are "
                "concentrated in misfiled 1993 material, unresolved GA ČR early anomalies, TA ČR pre-stabilization "
                "years, and still-ambiguous 2025 summary-table rows."
            ),
            "fit": "Moderate to strong for audited institutional trends; limited for system-wide totals and for agency-years that still lack an explicit recoverable chapter total.",
            "rating": "moderate",
        },
        "Costa Rica": {
            "years": "1989, 2010–2025 (the audited comparable backbone is much narrower)",
            "source": "Annual budget laws, including several multi-volume tomos and annex-style tables",
            "gaps": (
                "Costa Rica should be read as a conservative, traceability-first panel rather than as a dense "
                "continuous annual series. The original files change the object being measured across years, "
                "mixing annual international quotas (CATIE), institutional transfer bundles (CONICIT, PCII), "
                "programme totals (MICITT, some INTA, modern INCIENSA), and narrow university sub-lines (UCR). "
                "A high-recall extraction therefore overstates comparability. The audited panel intentionally "
                "keeps only the short backbone that can be defended from the original documents and drops FEES, "
                "ITCR/TEC, UCR proxy sub-lines, ING-page fragments, and third-party transfers that are not a "
                "clean institutional R&D appropriation."
            ),
            "fit": (
                "Limited to moderate — useful as a short audited institutional backbone, but not as a dense "
                "long-run annual R&D budget series."
            ),
            "rating": "limited",
        },
        "Chile": {
            "years": "1999–2025",
            "source": "National budget law tables (Ley de Presupuestos), usually reported in thousands of Chilean pesos",
            "gaps": (
                "This series is usable for the main public science and innovation institutions, but some gaps remain "
                "for smaller programme-style lines and some values still rely on fragile budget formatting. For years "
                "before 1999, the currently held source files are often not the full detailed budget volume: many are "
                "short LeyChile/BCN legal texts or laws that point readers to the Diario Oficial instead of reproducing "
                "the budget tables. That means some older gaps reflect source limitations, not just missing extraction."
            ),
            "fit": "Moderate — strongest for major science and innovation agencies; smaller programme-level series should be treated cautiously.",
            "rating": "moderate",
        },
        "France": {
            "years": "1970–2025",
            "source": "Loi de Finances / JORF budget tables; LOLF MIRES programmes after 2006",
            "gaps": (
                "France is now much broader historically, but pre-2006 remains a hybrid panel. Early years "
                "mix ministry chapters and explicit organisms, and older JORF tables required manual unit "
                "corrections from inflated pre-euro rows. Pre-2002 values are in French francs (FRF), while "
                "2002 onward is in euros (EUR), so long-run charts must not be read as a single-currency series. "
                "Post-2006 MIRES programme data is much cleaner than the historical section."
            ),
            "fit": "Moderate — strong for 2006+ programme data; historical pre-2006 data is usable but more heterogeneous.",
            "rating": "moderate",
        },
        "Germany": {
            "years": "1955–2025 (sparse pre-2003, stronger 2003–2009 and 2021+)",
            "source": "Bundeshaushalt — BMBF (Epl. 30); institutional grants to DFG, Helmholtz, Fraunhofer, MPG, Leibniz, DLR",
            "gaps": (
                "Coverage is episodic before 2003 and in the 2010–2020 window: source documents for those years "
                "contained ministry-level aggregates rather than per-institution grant tables, so most agencies "
                "cannot be matched. The 2003–2009 and 2021+ panels are substantially stronger, with individual "
                "institutional grant rows for the Big 5 science organisations (DFG, Helmholtz, Fraunhofer, MPG, "
                "Leibniz) plus DLR, DESY, and Jülich. Any long-run trend analysis must treat the series as "
                "discontinuous across the gap windows."
            ),
            "fit": "Limited to moderate — strong for 2003–2009 and 2021+; thin or absent for 2010–2020 and most pre-2003 years.",
            "rating": "limited",
        },
        "Hungary": {
            "years": "1992–2025 with documented gaps in 1991, 2000, 2003, 2004, 2006, 2008, and 2009",
            "source": "Annual Hungarian budget laws / Magyar Közlöny budget texts, with audited overrides traced to original PDFs or stable parsed budget text",
            "gaps": (
                "Hungary is now usable as a traced institutional series, but not as a complete annual panel. "
                "The strongest rows are the audited MTA, library, fund, and research-network totals carried as verified overrides. "
                "The main remaining weakness is historical MTA coverage in a small set of years where the original chapter "
                "heading is visible but the chapter-total amount is truncated in the PDF text layer, so no defendable institutional "
                "total reaches raw_rows. 1991 is weaker still: the current cached source text does not expose a detectable MTA "
                "chapter heading at all. National Agricultural Research and Innovation Centre was intentionally removed from the "
                "audited panel because the current source inventory does not yield a stable institution-level total across reruns."
            ),
            "fit": "Moderate — strong for audited institutional totals and modern R&D funds; incomplete for a narrow set of historical MTA years.",
            "rating": "moderate",
        },
        "Iceland": {
            "years": "1975–2025",
            "source": "Icelandic budget bills and annex tables, often reported in m.kr. with OCR-sensitive historical scans",
            "gaps": (
                "Coverage is now much broader for named science agencies, universities, and sectoral research funds, "
                "and many fragile rows were manually rechecked against the original budget text. The main remaining "
                "weakness is 2017–2018: the currently held source files for those years mostly expose aggregate "
                "programme headings rather than detailed institution-level appropriations, so those years still have "
                "structural gaps that are unlikely to disappear without alternative source documents."
            ),
            "fit": "Moderate — useful for institutional analysis in most years, but incomplete in the aggregate-only 2017–2018 window.",
            "rating": "moderate",
        },
        "Israel": {
            "years": "1975–2025",
            "source": "Annual Israeli budget laws and budget tables, including scanned historical PDFs and several biannual / summary-style modern files",
            "gaps": (
                "Israel is now usable as a conservative, traceability-first institutional panel, but the source family is heterogeneous. "
                "Historical scans are OCR-sensitive, the monetary regime changes across ILP / old shekel / NIS eras, "
                "and some modern files behave more like summary-style budget pages than clean institutional ledgers. "
                "The audited panel therefore keeps only explicit council, ministry, authority, fund, and named-agency rows, "
                "and intentionally leaves several smaller institutions sparse rather than forcing continuity. "
                "For 2023–2025, the ministry row should be read as a broader Section 19 science-plus-culture bundle rather than as a pure ministry-of-science total."
            ),
            "fit": "Moderate — strongest for the audited National Council pre-1992 and Ministry of Science 1992+ backbone; weaker for sparse agencies and the modern Section 19 bundle years.",
            "rating": "moderate",
        },
        "Japan": {
            "years": "1975–2025",
            "source": "Budget of Japan science and technology tables, operating grants, and historical predecessor rows",
            "gaps": (
                "Modern years were manually audited and are much stronger. The early historical span "
                "(especially 1975–2000) relies partly on predecessor-institution rollups rather than "
                "perfectly comparable modern agency lines. That bridge is documented, but it is not the "
                "same evidentiary quality as post-2000 operating-grant rows."
            ),
            "fit": "Moderate to good — strongest from the 2000s onward; historical coverage is broad but partly bridged.",
            "rating": "moderate",
        },
        "Korea": {
            "years": "2018–2025 core audited panel, plus a thematic layer for 2019 and 2022–2025",
            "source": (
                "Korean budget briefs, 홍보자료, and fiscal-plan summary tables; "
                "the app separates a core audited panel from a Korea-only thematic panel that also preserves source-display units"
            ),
            "gaps": (
                "Korea should not be read as a clean ministry-by-ministry appropriations panel. The core layer keeps "
                "only conservative annual totals and audited subtotals, while the thematic layer captures explicit "
                "theme-based amounts such as AI, semiconductors, bio, space, quantum, and strategic technology when "
                "they appear clearly in the original page. Most supported files are summary-style PDFs rather than "
                "full ledgers, and some stronger source material may still sit in HWP files that are not yet ingested. "
                "The main remaining core-panel gaps in the current inventory are MSIT 2018, 2019, and 2021, plus "
                "Strategic Technology 2021."
            ),
            "fit": (
                "Limited to moderate — useful only when read as a two-layer source family: a conservative core audited "
                "series for annual totals, plus a thematic panel for policy-priority buckets. Not suitable as a complete "
                "institutional budget ledger."
            ),
            "rating": "limited",
        },
        "UK": {
            "years": "1993–2025 (episodic — not a continuous series)",
            "source": "HM Treasury Budget / Spending Review documents and DSIT/BEIS R&D funding announcements",
            "gaps": (
                "The UK panel is not a continuous institutional budget series. Source documents are mainly "
                "policy Budgets and Spending Reviews, which announce named funds and headline R&D commitments "
                "rather than reporting per-agency appropriations line by line. The result is coverage in years "
                "when major announcements were made, with gaps in quieter years. UKRI (post-2018) is tracked "
                "as a block grant. Pre-2018 individual research councils (MRC, EPSRC, BBSRC, NERC, ESRC) are "
                "included where explicitly stated. For a complete science-budget ledger, Main Supply Estimates "
                "would be required as an additional source."
            ),
            "fit": "Limited to moderate — informative for named R&D funds and UKRI block grants, but not a complete agency appropriations series.",
            "rating": "limited",
        },
        "Denmark": {
            "years": "1975–2025",
            "source": "Finanslov (Finance Bill) — scanned PDFs (1975–1999) and digital PDFs (2000–2025), Danish",
            "gaps": (
                "Full coverage 1975–2025 for all 7 universities and the main research councils, with three methodological eras: "
                "(1) 1975–1993: gross Driftsudgifter (operating appropriations) — amounts are systematically larger than "
                "post-reform values and not directly comparable. "
                "(2) 1994–2025: Selvejebevilling basistilskud (block grants to autonomous institutions) — the consistent "
                "institutional series. University amounts roughly halved at the 1994 reform as grants shifted to net framework. "
                "(3) 1996–1998 structural gap: universities became Nettostyrede virksomheder (net-managed enterprises) — "
                "no individual line items appear in the Finance Bill for those years, only a collective enterprise total (~5.4B DKK). "
                "Several years required verified manual overrides due to LLM extraction issues: "
                "unit bugs (2005, 2015), garbled font encoding (2018 recovered via OCR), "
                "missing extraction (2020–2022), and changed PDF column layout (2025). "
                "All overrides are document-verified from original Finanslov pages. "
                "Smaller programme lines (research councils, innovation funds) should be treated with more caution than universities."
            ),
            "fit": "Good for university series 1975–2025 (with noted methodology break at 1994 and 1996–1998 gap); moderate for smaller programmes.",
            "rating": "moderate",
        },
        "Estonia": {
            "years": "1991–2011 institutional series, with a sparse programme-based continuation in later budgets",
            "source": "Annual Estonian state budget laws and budget tables",
            "gaps": (
                "The Estonia panel is strongest from 1991 to 2011, where the budget more often names specific science bodies such as "
                "the science foundation, Archimedes, and selected universities. After 2011, the budget documents still contain research "
                "and innovation spending, but they are more often organised under broad programmes rather than the same named institutions. "
                "The app therefore treats the post-2011 continuation as a separate programme-based bridge where clear totals can be read from "
                "the budget. Later gaps do not necessarily mean Estonia stopped funding R&D; they often mean the published budget no longer "
                "reports the money in a directly comparable institution-by-institution format."
            ),
            "fit": "Moderate — useful for the 1991–2011 institutional series, with a cautious post-2011 hybrid continuation.",
            "rating": "moderate",
        },
        "Latvia": {
            "years": "1992–2018, with a short early ruble-era bridge and a mainly programme-based core from 1995 onward",
            "source": "Annual Latvian budget laws and budget tables (`likumi.lv` PDFs and a small number of legacy budget-document wrappers)",
            "gaps": (
                "Latvia is still one of the weaker audited budget panels in the app, but it is now more tightly traced to the original files than before. "
                "The earliest surviving observations (1992–1993) are explicitly in thousand rubles and should be treated only as a short bridge, not as "
                "directly comparable with the later LVL/EUR years. From 1995 onward the usable backbone is mostly programme- and subprogramme-based rather "
                "than a stable roster of named science agencies: the strongest part of the panel is the audited cluster around science totals, science base "
                "funding / scientific-activity provision, state-commissioned research, and university science development in the late 1990s and 2000s. "
                "Many other apparent rows in the source family were wrapper text, zero rows, finance-mechanics shells, ERDF sub-lines, or institutional "
                "subcomponents misread as totals. Late observations such as 2015 and 2018 are valid but behave more like targeted earmarks than a full "
                "science budget table."
            ),
            "fit": "Limited — usable only for cautious, audited programme-level trend reading; weak for institutional continuity and cross-era comparison.",
            "rating": "limited",
        },
        "Lithuania": {
            "years": "1993–2025 (audited backbone; still hybrid across monetary and reporting eras)",
            "source": (
                "Annual Lithuanian budget laws and annex tables, combining parsed DOCX/PDF tables with "
                "manual verified overrides traced to the original files"
            ),
            "gaps": (
                "Lithuania is now a much stronger traceability-first panel than before, but it remains a hybrid series. "
                "The monetary regime changes from talonas (1993) to litas (1994–2014) to euro (2015 onward), and the "
                "budget presentation also changes across eras. Several years had to be rebuilt from institution-level "
                "totals in the original documents because the raw extractor either missed the file entirely or picked "
                "secondary columns such as wage or asset-acquisition sublines instead of the full institutional appropriation. "
                "The app therefore shows a documented institutional backbone, not a single homogeneous currency-constant series. "
                "The very large 1993 State Science, Studies and Technology Service amount is intentionally excluded from the "
                "final panel because it comes from a pre-litas thousand-talonas appropriation that is not methodologically "
                "comparable with the later series."
            ),
            "fit": (
                "Moderate to good — strong for audited named institutions and for 2003 onward continuity, but long-run "
                "cross-era comparison still requires explicit currency conversion and caution."
            ),
            "rating": "moderate",
        },
        "Luxembourg": {
            "years": "2001–2025 for the final audited panel; earlier source files are preserved but excluded from the app-ready series",
            "source": (
                "Luxembourg annual state budget laws and annex tables, with page-level manual verification against the "
                "original PDFs for every observation retained in the final panel"
            ),
            "gaps": (
                "Luxembourg is now a deliberately conservative, traceability-first institutional panel. The final app-ready "
                "series keeps 105 observations across 6 canonicals, and all retained rows are locked `verified_override` "
                "entries traced to the original budget pages. Earlier ministry totals from the late 1970s to 1990s were "
                "removed after source audit because several traced pages resolved to mixed or non-matching culture, health, "
                "laboratory, or higher-education sections rather than a clean research-ministry aggregate. The panel is "
                "therefore narrower than the raw extraction history, but materially cleaner."
            ),
            "fit": (
                "Good for a compact institutional backbone from 2001 onward — especially FNR, Université du Luxembourg, "
                "LIST, LISER, LIH, and CRP Gabriel Lippmann. Not suitable as a complete long-run ministry-total series."
            ),
            "rating": "good",
        },
        "Mexico": {
            "years": "1986–2024 (usable years are discontinuous)",
            "source": "Federal budget decrees and annex tables for CONACYT / Ramo 38 and selected named institutions",
            "gaps": (
                "The Mexican panel is intentionally conservative. Final amounts are normalized to full Mexican pesos "
                "(MXN), and surviving rows were kept only where they can be checked against the original budget document; "
                "narrow research-only, project-specific, or proxy rows were removed when they did not represent an "
                "institution-wide budget. Several years remain missing because the currently available files are either "
                "wrong-source, non-budget legal text, or not reliably extractable, especially 2004–2005, 2019, 2022, and 2023."
            ),
            "fit": "Moderate — strongest for audited CONACYT / Ramo 38 totals and a subset of clearly documented institutional lines.",
            "rating": "moderate",
        },
        "Norway": {
            "years": "1975–2026",
            "source": "Statsbudsjettet (State Budget) — annual budget proposition documents",
            "gaps": (
                "Norway has the strongest long-run panel in this dataset with no missing years 1975–2026. "
                "The Research Council of Norway (RCN), SINTEF, and university operating grants are tracked "
                "continuously. Some pre-1990 series rely on predecessor institution names and ministry "
                "chapter aggregates, but the series is broadly comparable across the full span."
            ),
            "fit": "Good — one of the most complete and consistent country panels.",
            "rating": "good",
        },
        "Netherlands": {
            "years": "1975–2025",
            "source": "Rijksbegroting (State Budget) — OCW, EZ, and other ministry chapters",
            "gaps": (
                "Complete coverage 1975–2025 with no missing years. Key caution: the unit system changed "
                "in 2002 from millions of guilders (NLG) to thousands of euros (EUR), so pre- and post-2002 "
                "values are in different currencies and cannot be directly compared without conversion. "
                "NWO, KNAW, TNO, and university block grants are the most stable series."
            ),
            "fit": "Moderate to good — complete timeline but the 2002 currency switch means long-run comparisons require care.",
            "rating": "moderate",
        },
        "New Zealand": {
            "years": "1975–2025 (audited institutional backbone; strongest from 1996 onward)",
            "source": (
                "Annual Appropriation Estimates Acts and earlier budget volumes, traced to cached original text and "
                "normalized to full New Zealand dollars (NZD)"
            ),
            "gaps": (
                "New Zealand is now a conservative, traceability-first panel rather than a dense uninterrupted science-budget ledger. "
                "The strongest anchor points are the DSIR-era institutional totals, the explicit Research, Science and Technology vote "
                "in 1996–1999 and 2010, Crown Research Institute core funding in 2011–2015, and the modern named funds and agencies "
                "from 2015 onward. The main remaining weakness is 2000–2009: the underlying budget files often expose Marsden Fund "
                "cleanly but not a defensible whole-of-vote total, so that decade is structurally partial in the institutional panel. "
                "A small set of years remains intentionally blank because the source text is ambiguous or non-recoverable, including "
                "DSIR 1975/1977/1984, the science-vote gaps in 1990/1995/2001, and Regional Research Institutes in 2020."
            ),
            "fit": (
                "Moderate to good — strong for audited institutional and fund analysis, especially 1996+ and the modern 2015+ portfolio; "
                "long-run trend reading should treat 2000–2009 as partial coverage rather than a complete annual science budget."
            ),
            "rating": "moderate",
        },
        "Portugal": {
            "years": "1977–2025 in the source family; the current app-ready panel keeps 38 audited observations",
            "source": (
                "Annual Portuguese budget laws and annex tables, plus deterministic/manual source audits against the "
                "original PDFs, especially MAPA V / MAPA VII pages where a clean institutional appropriation is visible"
            ),
            "gaps": (
                "Portugal is intentionally presented as a narrow, traceability-first institutional panel. The final series "
                "keeps only 38 defendable observations across FCT, JNICT, LNEC, and ANI, all stored in full currency units "
                "(`escudo` before the euro era and `euro` afterward) rather than `thousand`. Many tempting rows in the source "
                "family were deliberately excluded after manual page review because they resolved to chapter totals, municipal "
                "tables, plurianual responsibility tables (for example MAPA 14), legal transfer language, or programme/project "
                "bundles such as PIDDAC instead of a clean institution-level annual appropriation. Recent 2021–2025 gaps are "
                "mostly structural in the current file set: the pages we can recover tend to be legal text or transfer-style "
                "references rather than an explicit institutional budget row for FCT or ANI."
            ),
            "fit": (
                "Moderate — strong as a conservative, transparent institutional backbone for Portugal; not suitable as a "
                "complete annual ledger of all Portuguese public R&D appropriations."
            ),
            "rating": "moderate",
        },
        "Poland": {
            "years": "1990–2025 in the source family; the current app-ready panel keeps only 24 strictly audited observations",
            "source": (
                "Annual Polish budget laws and annex-style budget tables, plus a small set of manual verified overrides "
                "traced to the original PDFs"
            ),
            "gaps": (
                "Poland is highly discontinuous in its current audited form and should not be read as a coherent annual time series. "
                "The strict app-ready panel keeps only 24 explicit same-page matches across 1990–2025, which leaves most years and "
                "most institution-year combinations blank. Many exclusions are not because the documents are silent, but because the "
                "available pages only expose weak numeric traces, fragmented sub-lines, generic aggregates, or level-mismatched entities. "
                "The source inventory is also structurally weak in several years: 1995 and 2012 behave like incomplete legal-wrapper PDFs, "
                "2000 has a broken text layer, and 2001–2007 plus 2009 look more like legal text than usable institutional budget annexes."
            ),
            "fit": (
                "Limited — usable only as a sparse, traceability-first reference panel for a few audited institutional points; "
                "not suitable as a consistent long-run time series of Polish public R&D."
            ),
            "rating": "limited",
        },
        "Finland": {
            "years": "1985–2025",
            "source": "Valtion talousarvio (State Budget) — annual budget proposals, digital PDFs",
            "gaps": (
                "Coverage is 1985–2025 for 8 agencies with no missing years. Two currency eras: "
                "pre-2002 amounts are in Finnish markka (FIM, full units — not thousands or millions), "
                "while 2002 onward is in euros (EUR). The series cannot be read as a single-currency "
                "trend without FIM→EUR conversion (rate: 5.94573 FIM/EUR). "
                "Key agencies tracked: Suomen Akatemia research grants (moment 29.88.50 + 29.88.53 combined), "
                "Academy operating costs, Business Finland / Tekes operating and public R&D grants, "
                "VTT, GTK, Luke/MTT/Metla/RKTL, and VATT. "
                "Five FIM-era years (1992, 1993, 1996, 1999, 2001) were manually verified against original "
                "budget proposal texts and overridden where LLM extraction picked commitment authorities "
                "(myöntämisvaltuus) instead of annual appropriations. All overrides are document-traceable."
            ),
            "fit": "Good — complete 1985–2025 for all 8 agencies; FIM-era values require currency conversion for cross-era comparison.",
            "rating": "good",
        },
        "Switzerland": {
            "years": "1978–2025 (some gaps in 1979–1993 window)",
            "source": "Voranschlag der Schweizerischen Eidgenossenschaft — Bundesblatt (1975–2020) and VA-Band3-d (2021+)",
            "gaps": (
                "Two-era document structure: pre-2021 Bundesblatt editions are short aggregate documents (3–10 pages) "
                "that do not break out individual ETH institutions or SNF separately, so those years show only "
                "top-level totals where available. The 2021+ VA-Band3-d documents provide full departmental detail "
                "including ETH Zürich, EPFL, SNF, Innosuisse, CERN, and ESA lines. All Swiss federal budget amounts "
                "are in full Swiss francs (not thousands or millions), which required unit correction in the pipeline."
            ),
            "fit": "Moderate — strong from 2021+; historical pre-2021 coverage is partial and agency-level only for aggregate ETH-Bereich.",
            "rating": "moderate",
        },
        "Slovakia": {
            "years": "1992–2025, with a verified source gap in 2023–2024",
            "source": "Annual Slovak state budget laws and annex tables, plus manual source audits of modern PDFs",
            "gaps": (
                "The panel starts in 1992 because the repo does not currently contain a usable 1991 Slovak budget file, "
                "and the lone 1990 file in the folder is actually a Polish budget misfiled under Slovakia. "
                "There is also a verified break between 2022 and 2025: the 2023 PDF in the repo is only the legal act text "
                "without the numeric annex tables, and the 2024 PDF is only a one-page aggregate balance summary rather than the "
                "detailed expenditure annex. Those two years are therefore genuine source limitations, not just missed extraction. "
                "Long-run charts also span two monetary regimes: pre-2009 Slovak koruna (SKK) and 2009 onward euro (EUR)."
            ),
            "fit": "Moderate — usable for audited institutional trends, but cross-era level comparison requires explicit currency conversion and the 2023–2024 source gap must be respected.",
            "rating": "moderate",
        },
        "Slovenia": {
            "years": "1992–2025, audited as a conservative six-series R&D panel",
            "source": "Uradni list budget acts, RS companion budget files, and manual verification against original Slovenian budget PDFs",
            "gaps": (
                "The Slovenian panel is intentionally conservative and traceability-first. It currently keeps 87 verified observations across six "
                "audited series rather than trying to represent the full Slovenian public R&D system. The strongest long-run anchors are "
                "SAZU and programme 0502, with ARRS explicit from 2004 onward and two additional modern series added for European Space Agency "
                "programmes and research and innovation capacities. Important source constraints remain: 2014 in the current file set behaves like "
                "legal-wrapper text rather than a recoverable numeric annex, one misfiled 2004/2005 file was explicitly excluded, and 32 "
                "series-year combinations still remain genuine missing values. Long-run interpretation also spans two monetary regimes: SIT before "
                "2007 and EUR from 2007 onward."
            ),
            "fit": "Moderate to good — strong for the audited R&D core and modern level checks, but still a partial institutional panel rather than a complete whole-of-government R&D ledger.",
            "rating": "moderate",
        },
        "Spain": {
            "years": "2002–2023 (hybrid panel; strongest from 2018 onward)",
            "source": "Presupuestos Generales del Estado / BOE budget tables, with manual verification from original Spain budget pages where extraction missed named rows",
            "gaps": (
                "Spain now has no missing years or outliers in the final panel used by the app, but the series is deliberately hybrid. "
                "The historical span is anchored on programme 463B ('Plan Nacional I+D' / promotion and coordination of scientific and technical research), "
                "while the modern span tracks explicit institutions such as AEI, CSIC, CIEMAT, ISCIII, CDTI, and selected one-year bodies. "
                "This means the long run is not a single homogeneous institutional ledger: pre-2017 values are mostly programme-level appropriations, "
                "whereas 2018+ is much more institutional. Several 2022 rows and five missing 463B years were manually verified against original BOE tables."
            ),
            "fit": "Moderate to good — usable for long-run trend context, strongest for 2018+ institutional analysis and for the verified 463B programme series.",
            "rating": "moderate",
        },
        "Turkey": {
            "years": "1976–1977, 2006–2009",
            "source": (
                "Original Turkish budget laws and budget-justification tables, with manual PDF review of weak historical survivors "
                "and row-level verification for 2006–2009"
            ),
            "gaps": (
                "Turkey is intentionally presented as a strict, traceability-first panel. Several earlier candidate observations "
                "(notably 1975, 1978, and 1982) were removed after manual review of the original PDFs because the retained rows "
                "could not be tied to defensible agency appropriations on the cited pages. The strongest evidence is concentrated "
                "in 2006–2009, where TÜBİTAK, TAEK, TÜBA, and KOSGEB can be traced to exact budget-table rows. The two retained "
                "1970s points (1976 and 1977 for TAEK) are usable but rely on strong page-level anchors rather than fully clean "
                "literal row excerpts. There is currently no defendable continuous institutional series for the long gap between "
                "the late 1970s and 2006."
            ),
            "fit": (
                "Limited but reliable — suitable as a small audited institutional reference panel, especially for 2006–2009; "
                "not suitable as a dense continuous long-run R&D budget series for Turkey."
            ),
            "rating": "limited",
        },
    }
    _BUD_RATING_COLORS = {
        "good":     ("#1a7340", "#d4edda", "#1a734020"),
        "moderate": ("#7a5a00", "#fff3cd", "#7a5a0020"),
        "limited":  ("#8b0000", "#fde8e8", "#8b000020"),
    }

    _active_countries = sel_bud_ctry if sel_bud_ctry else []

    db_f = _filtered_budget_df()

    # ── Currency helpers ──
    _currencies = db_f["currency"].dropna().unique() if "currency" in db_f.columns else []
    _multi_currency = len(_currencies) > 1
    _ccy = _currencies[0] if len(_currencies) == 1 else "local currency"
    _amt_col = "Amount (M)"  # generic column name for charts

    def _fmt_amt(df_col):
        return f"{_ccy} (millions)" if not _multi_currency else "Amount (millions, local currency)"

    def _to_millions(series):
        return series / 1e6

    # ── KPI strip ──
    _n_countries = db_f["country"].nunique() if "country" in db_f.columns else 1
    _n_agencies  = db_f["canonical_name"].nunique() if "canonical_name" in db_f.columns else "—"
    _yr_range    = f"{int(db_f['year'].min())}–{int(db_f['year'].max())}" if not db_f.empty else "—"
    if _multi_currency:
        _spend_kpi = f"{_n_countries} countries"
    else:
        _spend_kpi = f"{_ccy} {db_f['amount_local'].sum()/1e6:,.0f} M"
    stat_row([
        (f"{len(db_f):,}",      "Budget lines"),
        (_spend_kpi,             "Total spend (local currency)"),
        (_yr_range,              "Years covered"),
        (f"{_n_agencies}",       "Agencies tracked"),
    ])

    # ── Chart 1: Line chart(s) by year ──
    _ctry_pal = [NAVY, ORANGE, TEAL, GREEN, BLUE, "#9B59B6", "#E74C3C", GREY]
    _n_ctry = db_f["country"].nunique() if "country" in db_f.columns else 1

    if _multi_currency and _n_ctry > 1:
        # Multiple countries: facet grid — split country panels into separate traces
        # when a country changes currency across eras.
        section_header("R&D-related budget over time by country (local currency)")
        yr_country = db_f.groupby(["year", "country", "currency"], dropna=False)["amount_local"].sum().reset_index()
        yr_country[_amt_col] = _to_millions(yr_country["amount_local"])
        _all_years = sorted(db_f["year"].dropna().unique())
        yr_country["currency"] = yr_country["currency"].fillna("Unknown")
        yr_country["label"] = yr_country["currency"].map(_budget_currency_label).fillna("Unknown currency")
        yr_country["series_key"] = yr_country["country"] + "||" + yr_country["label"]
        _full_grid = pd.DataFrame(
            [
                (y, c, lbl, key)
                for c, lbl, key in yr_country[["country", "label", "series_key"]].drop_duplicates().itertuples(index=False, name=None)
                for y in _all_years
            ],
            columns=["year", "country", "label", "series_key"],
        )
        yr_country = _full_grid.merge(
            yr_country.drop(columns=["currency"], errors="ignore"),
            on=["year", "country", "label", "series_key"],
            how="left",
        )
        yr_country["currency"] = yr_country["label"].str.split(" — ").str[0].fillna("Unknown")
        _sorted_ctry = sorted(yr_country["country"].unique())
        _currency_labels = sorted(yr_country["label"].dropna().unique())
        _color_map1 = _budget_currency_color_map(_currency_labels)
        _wrap = min(4, _n_ctry)
        _n_rows = (_n_ctry + _wrap - 1) // _wrap
        fig1 = px.line(
            yr_country, x="year", y=_amt_col,
            facet_col="country", facet_col_wrap=_wrap,
            color="label",
            line_dash="label",
            line_group="series_key",
            markers=True,
            color_discrete_map=_color_map1,
            labels={"year": "Year", _amt_col: "Amount (M, local)", "country": "", "label": ""},
            custom_data=["country", "label"],
            facet_row_spacing=0.03,
            facet_col_spacing=0.06,
        )
        fig1.update_traces(
            line_width=2, marker_size=5,
            hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]}<br>Year: %{x}<br>Amount: %{y:,.1f} M<extra></extra>",
        )
        fig1.update_yaxes(matches=None, showticklabels=True, mirror=False)
        fig1.update_xaxes(matches="x", showticklabels=True, tickangle=-45)
        fig1.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
        apply_style(fig1, height=185 * _n_rows, xtitle="", ytitle="Amount (M, local currency)")
        st.plotly_chart(fig1, use_container_width=True)
        caption_note("Each panel shows one country in its local currency. Countries with monetary transitions now split into separate lines by currency era. Y-axes are independent — do not compare levels across panels.")

    elif _multi_currency:
        section_header("R&D-related budget by year (local currency)")
        yr_ct = db_f.groupby(["year", "currency"])["amount_local"].sum().reset_index()
        yr_ct[_amt_col] = _to_millions(yr_ct["amount_local"])
        yr_ct["label"] = yr_ct["currency"].map(_budget_currency_label).fillna("Unknown currency")
        # Full year grid per currency — NaN where no data so line breaks at gaps
        _sorted_labels = sorted(yr_ct["label"].dropna().unique())
        _yr_range_mc = range(int(yr_ct["year"].min()), int(yr_ct["year"].max()) + 1)
        _grid_mc = pd.DataFrame([(y, l) for l in _sorted_labels for y in _yr_range_mc], columns=["year", "label"])
        yr_ct = _grid_mc.merge(yr_ct.drop(columns=["currency"], errors="ignore"), on=["year", "label"], how="left")
        _color_map1 = _budget_currency_color_map(_sorted_labels)
        fig1 = px.line(
            yr_ct, x="year", y=_amt_col, color="label",
            color_discrete_map=_color_map1,
            markers=True,
            labels={"year": "Year", _amt_col: "Amount (millions, local currency)", "label": ""},
            custom_data=["label"],
        )
        fig1.update_traces(
            line_width=2, marker_size=5,
            hovertemplate="<b>%{customdata[0]}</b><br>Year: %{x}<br>Amount: %{y:,.0f} M<extra></extra>",
        )
        apply_style(fig1, height=360, xtitle="Year", ytitle="Amount (millions, local currency)")
        st.plotly_chart(fig1, use_container_width=True)
        caption_note("This selection mixes multiple currencies (e.g. FRF and EUR) — levels are not directly comparable across the full span.")

    else:
        section_header(f"R&D-related budget by year ({_ccy} millions)")
        yr_ct = db_f.groupby("year")["amount_local"].sum().reset_index()
        yr_ct[_amt_col] = _to_millions(yr_ct["amount_local"])
        # Full year grid — NaN where no data so line breaks at gaps
        _yr_range_sc = pd.DataFrame({"year": range(int(yr_ct["year"].min()), int(yr_ct["year"].max()) + 1)})
        yr_ct = _yr_range_sc.merge(yr_ct, on="year", how="left")
        _ytitle1 = _fmt_amt(None)
        _countries_in_data = sorted(db_f["country"].dropna().unique()) if "country" in db_f.columns else []
        fig1 = px.line(
            yr_ct, x="year", y=_amt_col,
            markers=True,
            labels={"year": "Year", _amt_col: _ytitle1},
        )
        fig1.update_traces(
            line_width=2, marker_size=5, line_color=NAVY,
            hovertemplate="Year: %{x}<br>Amount: %{y:,.0f} M<extra></extra>",
        )
        apply_style(fig1, height=360, xtitle="Year", ytitle=_ytitle1)
        st.plotly_chart(fig1, use_container_width=True)
        caption_note(f"Source: Finance Bills — {', '.join(_countries_in_data)}. Numbers in {_ccy} millions.")

    _dq_notes = [(_c, _BUD_DOC_QUALITY[_c]) for _c in _active_countries if _c in _BUD_DOC_QUALITY]
    if _dq_notes and len(_active_countries) <= 3:
        _dq_cols = st.columns(len(_dq_notes))
        for _dq_col, (_dq_ctry, _dq) in zip(_dq_cols, _dq_notes):
            _fc, _bg, _border_bg = _BUD_RATING_COLORS.get(_dq["rating"], ("#555", "#f0f0f0", "#55555520"))
            _dq_col.markdown(
                f'<div style="border:1px solid {_bg};border-top:3px solid {_fc};'
                f'background:{_bg};border-radius:0 0 4px 4px;padding:.65rem .85rem;'
                f'margin-bottom:.8rem;">'
                f'<div style="font-size:.78rem;font-weight:800;color:{_fc};margin-bottom:.3rem;">'
                f'{_dq_ctry}'
                f'<span style="font-weight:400;color:#666;font-size:.7rem;margin-left:.5rem;">'
                f'{_dq["years"]}</span></div>'
                f'<div style="font-size:.7rem;color:#555;margin-bottom:.25rem;">'
                f'<b>Source:</b> {_dq["source"]}</div>'
                f'<div style="font-size:.7rem;color:#555;margin-bottom:.25rem;">'
                f'<b>Gaps:</b> {_dq["gaps"]}</div>'
                f'<div style="font-size:.7rem;color:{_fc};font-weight:600;">'
                f'{_dq["fit"]}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    if "Poland" in _active_countries:
        st.markdown(
            """
            <div style="
                margin:.1rem 0 .9rem 0;
                padding:.65rem .8rem;
                border:1px solid #cfe2ff;
                border-left:4px solid #0d6efd;
                border-radius:6px;
                background:#f4f8ff;
                color:#163a5f;
                font-size:.78rem;">
                <div style="font-weight:800;margin-bottom:.2rem;">Poland panel shown: strict audited panel</div>
                <div>
                    The app currently displays the strict Poland panel with
                    <b>24 observations</b>, all retained only when the institution and amount
                    can be defended on the same source page. Weaker block-trace rows are excluded
                    from the app-ready view.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    _gap_summary = load_budget_gap_deepdive_summary()
    _gap_country_options = []
    if not _gap_summary.empty and "country" in _gap_summary.columns:
        _gap_country_options = sorted(_gap_summary["country"].dropna().astype(str).unique().tolist())
    if _active_countries:
        _gap_country_options = [c for c in _active_countries if c in _gap_country_options] or _active_countries

    if _gap_country_options:
        with st.expander("Budget Gap Explorer", expanded=bool(_active_countries and len(_active_countries) == 1)):
            _full_bud = load_budget()
            _selected_gap_countries = [c for c in _active_countries if c in _gap_country_options] or _gap_country_options
            _run_log = load_budget_run_log()
            _gap_rows = []

            for _gap_country in _selected_gap_countries:
                _country_review = load_budget_country_gap_review_table(_gap_country)
                _country_gap_report = load_budget_country_gap_report(_gap_country)
                _ctry_all = (
                    _full_bud[_full_bud["country"] == _gap_country]
                    if "country" in _full_bud.columns else pd.DataFrame()
                )
                _calendar_gaps: list[int] = []
                if not _ctry_all.empty and "year" in _ctry_all.columns:
                    _yrs_with_data = set(_ctry_all["year"].dropna().astype(int).unique())
                    if _yrs_with_data:
                        _full_range = set(range(min(_yrs_with_data), max(_yrs_with_data) + 1))
                        _calendar_gaps = sorted(_full_range - _yrs_with_data)

                _ctry_log = (
                    _run_log[_run_log["country"].str.lower() == _gap_country.lower()]
                    if not _run_log.empty and "country" in _run_log.columns else pd.DataFrame()
                )

                for _gy in _calendar_gaps:
                    _yr_log = (
                        _ctry_log[_ctry_log["year"] == _gy]
                        if not _ctry_log.empty else pd.DataFrame()
                    )
                    _yr_review = (
                        _country_review[_country_review["year"] == _gy]
                        if not _country_review.empty and "year" in _country_review.columns else pd.DataFrame()
                    )
                    _yr_gap_report = (
                        _country_gap_report[
                            (_country_gap_report["year"] == _gy)
                            & (_country_gap_report["gap_type"].fillna("ok") != "ok")
                        ]
                        if not _country_gap_report.empty and "year" in _country_gap_report.columns else pd.DataFrame()
                    )
                    if not _yr_log.empty and "source_file" in _yr_log.columns:
                        _docs = ", ".join(_yr_log["source_file"].dropna().astype(str).unique())
                    else:
                        _inventory_docs = _inventory_docs_for_year(_gap_country, int(_gy))
                        if _inventory_docs:
                            _docs = ", ".join(_inventory_docs)
                        else:
                            _nearby_docs = _nearby_inventory_docs(_gap_country, int(_gy), window=1)
                            _docs = ", ".join(_nearby_docs) if _nearby_docs else "—"
                    _issue, _analysis = _budget_gap_explorer_detail(
                        _gap_country,
                        int(_gy),
                        _yr_review,
                        _yr_gap_report,
                        _yr_log,
                    )

                    _tried, _concl, _fixable = _gap_investigation_lookup(_gap_country, int(_gy))
                    _gap_rows.append(
                        {
                            "Country": _gap_country,
                            "Year": _gy,
                            "Documents": _docs,
                            "Issue": _issue,
                            "Analysis": _analysis,
                            "What Was Tried": _tried,
                            "Conclusion / Fixable?": f"[{_fixable}] {_concl}" if _fixable else _concl,
                        }
                    )

            if not _gap_rows:
                if len(_selected_gap_countries) == 1:
                    st.info(f"No missing years detected for {_selected_gap_countries[0]} within its data range.")
                else:
                    st.info("No missing years detected for the selected countries within their data ranges.")
            else:
                _gap_df = pd.DataFrame(_gap_rows).sort_values(["Country", "Year"]).reset_index(drop=True)
                render_table(
                    _gap_df,
                    wide_cols=["Documents", "Issue", "Analysis", "What Was Tried", "Conclusion / Fixable?"],
                    max_rows=250,
                )
                _gap_dl_col1, _gap_dl_col2 = st.columns(2)
                with _gap_dl_col1:
                    st.download_button(
                        "Download Gap Explorer (CSV)",
                        _gap_df.to_csv(index=False).encode("utf-8"),
                        "budget_gap_explorer.csv",
                        "text/csv",
                        key="budget_gap_explorer_csv",
                    )
                with _gap_dl_col2:
                    st.download_button(
                        "Download Gap Explorer (Excel)",
                        _df_to_excel_bytes(_gap_df, sheet_name="Budget Gap Explorer"),
                        "budget_gap_explorer.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="budget_gap_explorer_excel",
                    )

    if _multi_currency:
        st.info("Some selected years mix currencies. Ministry totals, category shares, and year-over-year change are hidden because they would aggregate non-comparable currencies.")
        if db_f["country"].nunique() == 1 and set(db_f["country"].dropna().astype(str)) == {"Slovakia"}:
            st.warning("Slovakia has a verified source gap in 2023–2024, and the visible series mixes SKK (pre-2009) with EUR (2009 onward). Do not read the full span as a single comparable monetary trend.")
    else:
        # ── Chart 2 & 3 side by side ──
        col_a, col_b_ = st.columns(2)

        with col_a:
            _min_title = "Cumulative R&D spend by ministry (top 10)"
            section_header(_min_title)
            if "ministry_display" in db_f.columns:
                top_min = (
                    db_f.groupby("ministry_display")["amount_local"]
                    .sum().sort_values(ascending=True).tail(10).reset_index()
                )
                top_min["Amt M"] = _to_millions(top_min["amount_local"])
                top_min["pct"]   = 100 * top_min["Amt M"] / top_min["Amt M"].sum()

                fig2 = go.Figure(go.Bar(
                    x=top_min["Amt M"],
                    y=top_min["ministry_display"],
                    orientation="h",
                    marker_color=NAVY,
                    marker_line_width=0,
                    text=top_min["Amt M"].map(lambda x: f"{x:,.0f}"),
                    textposition="outside",
                    textfont=dict(size=9.5, color=TEXT),
                ))
                apply_style(fig2, height=380, xtitle=_fmt_amt(None), legend_bottom=False)
                fig2.update_layout(showlegend=False,
                                   xaxis=dict(showgrid=True, gridcolor="#EBEBEB"))
                st.plotly_chart(fig2, use_container_width=True)

        with col_b_:
            section_header("R&D category breakdown (% of total)")
            if "budget_category" in db_f.columns:
                cat_tot = (
                    db_f.groupby("budget_category")["amount_local"]
                    .sum().sort_values(ascending=False).reset_index()
                )
                cat_tot["Amt M"] = _to_millions(cat_tot["amount_local"])
                cat_tot["label"] = cat_tot["budget_category"].map(
                    lambda x: RD_CATEGORY_LABELS.get(x, x.replace("_", " ").title())
                )
                cat_tot["pct"]   = 100 * cat_tot["Amt M"] / cat_tot["Amt M"].sum()

                fig3 = go.Figure(go.Bar(
                    x=cat_tot["label"],
                    y=cat_tot["Amt M"],
                    marker_color=[BUDGET_CATEGORY_COLORS.get(c, GREY) for c in cat_tot["budget_category"]],
                    marker_line_width=0,
                    text=cat_tot["pct"].map(lambda x: f"{x:.1f}%"),
                    textposition="outside",
                    textfont=dict(size=10.5, color=TEXT),
                    hovertemplate="<b>%{x}</b><br>Amount: %{y:,.0f} M<extra></extra>",
                ))
                apply_style(fig3, height=380, ytitle=_fmt_amt(None), legend_bottom=False)
                fig3.update_layout(showlegend=False, xaxis=dict(showgrid=False))
                fig3.update_yaxes(range=[0, cat_tot["Amt M"].max() * 1.22])
                st.plotly_chart(fig3, use_container_width=True)

        # ── Chart 4: YoY growth ──
        section_header("Year-over-year change in identified R&D budget (%)")
        yr_tot = db_f.groupby("year")["amount_local"].sum().reset_index()
        yr_tot["chg"] = yr_tot["amount_local"].pct_change() * 100
        yr_tot_yoy = yr_tot.dropna(subset=["chg"])
        if not yr_tot_yoy.empty:
            colors_yoy = [GREEN if v >= 0 else "#C1272D" for v in yr_tot_yoy["chg"]]
            fig_yoy = go.Figure(go.Bar(
                x=yr_tot_yoy["year"], y=yr_tot_yoy["chg"],
                marker_color=colors_yoy, marker_line_width=0,
                text=yr_tot_yoy["chg"].map(lambda x: f"{x:+.1f}%"),
                textposition="outside",
                textfont=dict(size=10, color=TEXT),
            ))
            apply_style(fig_yoy, height=230, xtitle="Year", ytitle="% change",
                        legend_bottom=False)
            fig_yoy.add_hline(y=0, line_color=BORDER, line_width=1.5)
            fig_yoy.update_yaxes(range=[
                yr_tot_yoy["chg"].min() * 1.3,
                yr_tot_yoy["chg"].max() * 1.3,
            ])
            fig_yoy.update_layout(showlegend=False)
            st.plotly_chart(fig_yoy, use_container_width=True)
            caption_note("Year-over-year change in total identified R&D-related spending.")

    # ── Data table ──
    section_header("Budget line detail")
    _BUD_DISP_COLS = [c for c in [
        "country", "year", "ministry_display", "budget_line_display",
        "amount_local", "unit", "currency", "currency_era", "budget_category", "item_type", "source_file", "series_notes",
    ] if c in db_f.columns]
    _BUD_COL_LABELS = {
        "country": "Country", "year": "Year", "ministry_display": "Ministry",
        "budget_line_display": "Description", "amount_local": f"Amount ({_ccy})",
        "unit": "Unit", "currency": "Currency", "currency_era": "Currency era", "budget_category": "R&D category",
        "item_type": "Item type",
        "source_file": "Source file",
        "series_notes": "Series notes",
    }
    _bud_search = st.text_input(
        "Search table", key="bud_search", placeholder="Ministry, description, category…",
        label_visibility="collapsed",
    )
    _tbl = db_f.copy()
    if _bud_search:
        _mask = _tbl.astype(str).apply(
            lambda col: col.str.contains(_bud_search, case=False, na=False)
        ).any(axis=1)
        _tbl = _tbl[_mask]
    _is_korea_only = (
        not _tbl.empty
        and "country" in _tbl.columns
        and set(_tbl["country"].dropna().unique()) == {"Korea"}
    )
    if _is_korea_only:
        _tbl["amount_display_source"] = pd.to_numeric(
            _tbl["amount_local"], errors="coerce"
        ).map(_format_korea_source_amount)
    if _multi_currency:
        caption_note(f"{len(_tbl):,} rows  ·  {_n_countries} countries (amounts in local currency)")
        if _n_countries == 1 and "country" in _tbl.columns and set(_tbl["country"].dropna().astype(str)) == {"Slovakia"}:
            caption_note("Slovakia switches from SKK to EUR in 2009. Use the Currency era column to separate pre-2009 and 2009+ rows.")
    else:
        caption_note(f"{len(_tbl):,} rows  ·  {_ccy} {_tbl['amount_local'].sum()/1e6:,.1f} M")
    if _is_korea_only:
        caption_note(
            "Korea display amounts are shown in reader-facing Korean units (억원 / 조원). "
            "The pipeline still stores normalized values internally."
        )
        _korea_cols = [c for c in [
            "country", "year", "ministry_display", "budget_line_display",
            "amount_display_source", "currency", "budget_category",
            "item_type", "source_file", "series_notes",
        ] if c in _tbl.columns]
        _korea_labels = dict(_BUD_COL_LABELS)
        _korea_labels["amount_display_source"] = "Amount (Korean display)"
        render_table(
            _tbl[_korea_cols].sort_values("year"),
            col_labels=_korea_labels,
            wide_cols=["budget_line_display", "ministry_display", "series_notes"],
        )
    else:
        render_table(
            _tbl[_BUD_DISP_COLS].sort_values("year"),
            col_labels=_BUD_COL_LABELS,
            num_cols=["amount_local"],
            wide_cols=["budget_line_display", "ministry_display", "series_notes"],
        )
    _bud_download_cols = _BUD_DISP_COLS
    if _is_korea_only:
        _bud_download_cols = [c for c in [
            "country", "year", "ministry_display", "budget_line_display",
            "amount_display_source", "currency", "budget_category",
            "item_type", "source_file", "series_notes",
        ] if c in _tbl.columns]
    st.download_button(
        "Download CSV",
        _tbl[_bud_download_cols].to_csv(index=False).encode("utf-8"),
        "budget_lines.csv", "text/csv", key="bud_dl",
    )

    # ── Korea-only thematic panel ──
    if _is_korea_only:
        _k_theme = load_korea_theme_panel()
        if not _k_theme.empty and "year" in _k_theme.columns:
            _k_theme = _k_theme[
                (_k_theme["year"] >= yr_b[0]) & (_k_theme["year"] <= yr_b[1])
            ].copy()
            if not _k_theme.empty:
                section_header("Korea thematic R&D panel")
                caption_note(
                    "Korea's source family mixes audited totals with theme-based budget briefs. "
                    "This panel preserves explicit thematic subtotals as shown in the original files, "
                    "alongside normalized KRW values for filtering and charting."
                )

                _k_chart = _k_theme.copy()
                _k_chart["Amount (M)"] = _k_chart["amount_local"] / 1e6
                _k_fig = px.bar(
                    _k_chart,
                    x="year",
                    y="Amount (M)",
                    color="theme_bucket",
                    barmode="group",
                    labels={"year": "Year", "Amount (M)": "KRW (millions)", "theme_bucket": ""},
                    custom_data=["theme_label", "source_amount_display", "source_file", "page_number"],
                )
                _k_fig.update_traces(
                    marker_line_width=0,
                    hovertemplate=(
                        "<b>%{customdata[0]}</b><br>"
                        "Year: %{x}<br>"
                        "Amount: %{y:,.0f} M KRW<br>"
                        "Source amount: %{customdata[1]}<br>"
                        "Source: %{customdata[2]} (p. %{customdata[3]})<extra></extra>"
                    ),
                )
                apply_style(_k_fig, height=360, xtitle="Year", ytitle="KRW (millions)")
                st.plotly_chart(_k_fig, use_container_width=True)

                _K_THEME_COLS = [c for c in [
                    "year", "theme_bucket", "theme_label", "source_amount_display",
                    "source_file",
                    "page_number", "comparability_note",
                ] if c in _k_theme.columns]
                _K_THEME_LABELS = {
                    "year": "Year",
                    "theme_bucket": "Theme bucket",
                    "theme_label": "Theme label",
                    "source_amount_display": "Amount in source",
                    "source_file": "Source file",
                    "page_number": "Page",
                    "comparability_note": "Comparability note",
                }
                render_table(
                    _k_theme[_K_THEME_COLS].sort_values(["year", "theme_bucket"]),
                    col_labels=_K_THEME_LABELS,
                    num_cols=["page_number"],
                    wide_cols=["theme_label", "source_file", "comparability_note"],
                )
                st.download_button(
                    "Download Korea thematic panel (CSV)",
                    _k_theme[_K_THEME_COLS].to_csv(index=False).encode("utf-8"),
                    "korea_theme_panel.csv",
                    "text/csv",
                    key="bud_dl_korea_theme",
                )


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2 — INNOVATION REFORMS
# ═════════════════════════════════════════════════════════════════════════════

with TAB_REFORMS:
    if _dr.empty:
        st.info("No reform data found. Run: python main.py --reforms-only --reforms-country DNK")
        st.stop()

    # ── Stage indicator ──
    _stage_labels_map = {
        "stage1": "Stage 1 — GPT-4o-mini (primary extraction)",
        "stage2": "Stage 2 — Claude Haiku (secondary extraction)",
        "stage3": "Stage 3 — GPT-4o-mini (2nd run, tertiary extraction)",
        "merged": "Stage 4 — Cross-verified (adjudicator-reviewed across all model runs)",
    }
    _stage_note = _stage_labels_map.get(sel_stage, sel_stage)
    if show_excluded and sel_stage == "merged":
        _stage_note += " · showing all reforms including excluded"
    st.markdown(
        f'<div style="font-size:.78rem;color:#555;background:#F7F8FA;'
        f'border:1px solid #E2E6EA;border-radius:4px;padding:.4rem .8rem;margin-bottom:.8rem;">'
        f'<b>Data source:</b> {_stage_note}</div>',
        unsafe_allow_html=True,
    )

    # ── Cross-verification summary (merged stage only) ──
    if sel_stage == "merged" and "verification_bucket" in _dr_all.columns:
        _vb_counts = _dr_all["verification_bucket"].value_counts()
        _n_3c  = int(_vb_counts.get("All 3 models agreed", 0))
        _n_2i  = int(_vb_counts.get("2 of 3 models", 0))
        _n_cc  = int(_vb_counts.get("Both models agreed", 0))
        _n_1i  = int(_vb_counts.get("1 model only", 0))
        _n_3r  = int(_vb_counts.get("Excluded — all 3 models agreed", 0))
        _n_2e  = int(_vb_counts.get("Excluded — 2 of 3 models", 0))
        _n_cr  = int(_vb_counts.get("Excluded — both models agreed", 0))
        _n_1e  = int(_vb_counts.get("Excluded — 1 model only", 0))

        _three_model = (_n_3c + _n_2i + _n_3r + _n_2e) > 0
        _n_total = len(_dr_all)

        if _three_model:
            stat_row([
                (_n_3c,       "All 3 models — confirmed"),
                (_n_2i,       "2 of 3 models — included"),
                (_n_1i,       "1 model only — included"),
                (_n_3r + _n_2e + _n_1e, "Excluded (any tier)"),
            ])
            _n_in_dataset = _n_3c + _n_2i + _n_1i
            _n_excluded   = _n_3r + _n_2e + _n_1e
        else:
            stat_row([
                (_n_cc, "Both models agreed — confirmed"),
                (_n_1i, "One model only — included"),
                (_n_cr, "Both models agreed — rejected"),
                (_n_1e, "One model only — excluded"),
            ])
            _n_in_dataset = _n_cc + _n_1i
            _n_excluded   = _n_cr + _n_1e

        caption_note(
            f"Total reforms reviewed by LLM adjudicator: {_n_total} · "
            f"In final dataset: {_n_in_dataset} · "
            f"Excluded: {_n_excluded}"
        )
        st.markdown("<div style='margin-bottom:.6rem;'></div>", unsafe_allow_html=True)

    # ── KPI strip ──
    _dr_all_tab2 = _dr_all
    n_gs   = int((dr_f["growth_orientation"] == "growth_supporting").sum()) \
             if "growth_orientation" in dr_f.columns else 0
    n_gh   = int((dr_f["growth_orientation"] == "growth_hindering").sum()) \
             if "growth_orientation" in dr_f.columns else 0
    n_maj  = int(dr_f["is_major_reform"].sum()) if "is_major_reform" in dr_f.columns else 0
    n_ctry = int(dr_f["country_name"].nunique()) if "country_name" in dr_f.columns else 0
    n_surv = int(_dr_all_tab2["survey_year"].nunique()) if "survey_year" in _dr_all_tab2.columns else 0
    stat_row([
        (str(len(dr_f)), "Reform events"),
        (str(n_maj),     "Major reforms"),
        (str(n_gs),      "Growth-supporting"),
        (str(n_gh),      "Growth-hindering"),
        (str(n_ctry),    "Countries"),
        (str(n_surv),    "Surveys"),
    ])

    # ── Chart 1: reforms per year ──
    section_header("Reform events per year by innovation sub-type")
    YR_COL = "display_year"
    _multi_ctry = "country_name" in dr_f.columns and dr_f["country_name"].nunique() > 1
    if YR_COL in dr_f.columns and dr_f[YR_COL].notna().any():
        df_yr = dr_f.dropna(subset=[YR_COL]).copy()
        df_yr["yr"] = df_yr[YR_COL].astype(int)

        _c1_col_opts = ["Innovation sub-type", "Country"] if _multi_ctry else ["Innovation sub-type"]
        _c1_color_by = st.radio(
            "Color by", _c1_col_opts, horizontal=True, key="ref_c1_color",
            label_visibility="collapsed",
        ) if _multi_ctry else "Innovation sub-type"

        if _c1_color_by == "Country":
            # Country palette — consistent across app
            _ctry_names = sorted(df_yr["country_name"].dropna().unique())
            _ctry_pal = [NAVY, ORANGE, TEAL, GREEN, BLUE, GREY]
            _ctry_color_map = {c: _ctry_pal[i % len(_ctry_pal)] for i, c in enumerate(_ctry_names)}
            yr_st = df_yr.groupby(["yr", "country_name"]).size().reset_index(name="n")
            fig_yr = px.bar(
                yr_st, x="yr", y="n",
                color="country_name",
                color_discrete_map=_ctry_color_map,
                barmode="stack",
                labels={"yr": "Year", "n": "Reform events", "country_name": ""},
            )
            fig_yr.update_traces(
                hovertemplate="<b>%{fullData.name}</b><br>Year %{x}: %{y} reform(s)<extra></extra>",
                marker_line_width=0,
            )
        else:
            order = (df_yr.groupby("sub_theme").size()
                     .sort_values(ascending=False).index.tolist())
            yr_st = df_yr.groupby(["yr", "sub_theme"]).size().reset_index(name="n")
            yr_st["short"] = yr_st["sub_theme"].map(lambda x: SUBTHEME_SHORT.get(x, x))
            fig_yr = px.bar(
                yr_st, x="yr", y="n",
                color="sub_theme",
                color_discrete_map=SUBTHEME_COLORS,
                barmode="stack",
                category_orders={"sub_theme": order},
                labels={"yr": "Year", "n": "Reform events", "sub_theme": ""},
                custom_data=["short"],
            )
            fig_yr.update_traces(
                hovertemplate="<b>%{customdata[0]}</b><br>Year %{x}: %{y} reform(s)<extra></extra>",
                marker_line_width=0,
            )
            for trace in fig_yr.data:
                trace.name = SUBTHEME_SHORT.get(trace.name, trace.name)

        apply_style(fig_yr, height=320, xtitle="Year", ytitle="Reform events")
        st.plotly_chart(fig_yr, use_container_width=True)
        _src_note = {
            "stage1": "Extraction: GPT-4o-mini (OECD key).",
            "stage2": "Extraction: Claude Haiku (OECD Anthropic key).",
            "stage3": "Extraction: GPT-4o-mini (2nd run).",
            "merged": "Extraction: GPT-4o-mini + Claude Haiku + GPT-4o-mini (2nd run), adjudicated by GPT-4o-mini.",
        }.get(sel_stage, "")
        caption_note(
            f"Source: OECD Economic Surveys. {_src_note} "
            "Year = implementation year (imputed to survey year when not stated)."
        )

    # ── Charts 2 & 3 side by side ──
    col_a, col_b_ = st.columns(2)

    with col_a:
        section_header("Sub-type by growth orientation")
        if "growth_orientation" in dr_f.columns and "sub_theme" in dr_f.columns and not dr_f.empty:
            go_df = (dr_f.groupby(["sub_theme","growth_orientation"])
                     .size().reset_index(name="n"))
            go_df["sub_short"]    = go_df["sub_theme"].map(lambda x: SUBTHEME_SHORT.get(x, x))
            go_df["orient_label"] = go_df["growth_orientation"].map(
                lambda x: ORIENTATION_LABELS.get(x, x)
            )
            st_order = (go_df.groupby("sub_short")["n"].sum()
                        .sort_values(ascending=False).index.tolist())
            orient_order = [ORIENTATION_LABELS[k] for k in
                            ["growth_supporting","mixed","unclear_or_neutral","growth_hindering"]
                            if ORIENTATION_LABELS[k] in go_df["orient_label"].values]
            orient_colors_mapped = {v: ORIENTATION_COLORS[k]
                                    for k, v in ORIENTATION_LABELS.items()}
            fig_go = px.bar(
                go_df, x="sub_short", y="n",
                color="orient_label",
                color_discrete_map=orient_colors_mapped,
                barmode="stack",
                category_orders={"sub_short": st_order, "orient_label": orient_order},
                labels={"sub_short":"","n":"Reforms","orient_label":""},
            )
            fig_go.update_traces(marker_line_width=0)
            apply_style(fig_go, height=300, ytitle="Reform events", xangle=-28)
            st.plotly_chart(fig_go, use_container_width=True)

    with col_b_:
        section_header("R&D actor and stage")
        if "rd_actor" in dr_f.columns and "rd_stage" in dr_f.columns and not dr_f.empty:
            as_df = (dr_f.groupby(["rd_actor_label","rd_stage_label"])
                     .size().reset_index(name="n"))
            stage_order = ["Basic research","Applied research",
                           "Commercialisation","Adoption & diffusion","Unknown"]
            actor_order = ["Public sector","Private sector","Public–Private","Unknown"]
            fig_hm = px.density_heatmap(
                as_df,
                x="rd_stage_label", y="rd_actor_label", z="n",
                color_continuous_scale=[[0,"#EEF3FB"],[0.5,"#6699CC"],[1,NAVY]],
                labels={"rd_stage_label":"R&D Stage","rd_actor_label":"","n":"Reforms"},
                text_auto=True,
            )
            fig_hm.update_xaxes(
                categoryorder="array",
                categoryarray=[s for s in stage_order if s in as_df["rd_stage_label"].values],
            )
            fig_hm.update_yaxes(
                categoryorder="array",
                categoryarray=[a for a in reversed(actor_order) if a in as_df["rd_actor_label"].values],
            )
            fig_hm.update_traces(textfont=dict(size=12, color="white"), texttemplate="%{z}")
            apply_style(fig_hm, height=300, xangle=-20, legend_bottom=False)
            fig_hm.update_coloraxes(
                colorbar=dict(thickness=10, len=0.8, title=dict(text="n", font=dict(size=10)))
            )
            st.plotly_chart(fig_hm, use_container_width=True)

    # ── Chart 4: status & importance ──
    section_header("Status and importance breakdown")
    col_c, col_d_ = st.columns(2)

    with col_c:
        if "status" in dr_f.columns and not dr_f.empty:
            stat_df = dr_f["status_label"].value_counts().reset_index()
            stat_df.columns = ["status_label", "n"]
            stat_colors = {STATUS_LABELS[k]: c for k, c in
                           {"implemented": NAVY, "legislated": BLUE,
                            "announced": TEAL}.items()
                           if k in dr_f["status"].values}
            fig_stat = go.Figure(go.Bar(
                x=stat_df["n"], y=stat_df["status_label"],
                orientation="h",
                marker_color=[stat_colors.get(s, GREY) for s in stat_df["status_label"]],
                marker_line_width=0,
                text=stat_df["n"], textposition="outside",
                textfont=dict(size=11, color=TEXT),
            ))
            apply_style(fig_stat, height=240, xtitle="Reform events", legend_bottom=False)
            fig_stat.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))
            fig_stat.update_xaxes(range=[0, stat_df["n"].max() * 1.2])
            st.plotly_chart(fig_stat, use_container_width=True)

    with col_d_:
        if "importance_bucket" in dr_f.columns and not dr_f.empty:
            imp_df = (dr_f["importance_bucket"].value_counts()
                      .reset_index().rename(columns={"count": "n"})
                      .sort_values("importance_bucket"))
            imp_df["label"] = imp_df["importance_bucket"].map(
                {1: "Minor (1)", 2: "Moderate (2)", 3: "Major (3)"}
            )
            imp_colors = {1: "#DDE1E7", 2: BLUE, 3: NAVY}
            fig_imp = go.Figure(go.Bar(
                x=imp_df["label"], y=imp_df["n"],
                marker_color=[imp_colors.get(b, GREY) for b in imp_df["importance_bucket"]],
                marker_line_width=0,
                text=imp_df["n"], textposition="outside",
                textfont=dict(size=11, color=TEXT),
            ))
            apply_style(fig_imp, height=240, ytitle="Reform events", legend_bottom=False)
            fig_imp.update_layout(showlegend=False, xaxis=dict(showgrid=False))
            fig_imp.update_yaxes(range=[0, imp_df["n"].max() * 1.2])
            st.plotly_chart(fig_imp, use_container_width=True)

    # ── Reform catalogue ──
    section_header(f"Reform catalogue  —  {len(dr_f)} events")

    import html as _html

    _cat_sort_cols = st.columns([3, 1])
    with _cat_sort_cols[0]:
        sort_opt = st.radio(
            "Sort by",
            ["Year (newest first)", "Importance (highest first)", "Sub-type (A–Z)", "Country (A–Z)"],
            horizontal=True, key="sort_cat",
            label_visibility="collapsed",
        )
    sort_map = {
        "Year (newest first)":       ("implementation_year", False),
        "Importance (highest first)":("importance_bucket",   False),
        "Sub-type (A–Z)":            ("sub_theme",           True),
        "Country (A–Z)":             ("country_name",        True),
    }
    sc, sa = sort_map[sort_opt]
    df_cat_all = dr_f.sort_values(sc, ascending=sa)

    # Stable country palette for card left-border accent
    _cat_ctry_names = sorted(dr_f["country_name"].dropna().unique()) if "country_name" in dr_f.columns else []
    _cat_ctry_pal   = [NAVY, ORANGE, TEAL, GREEN, BLUE, GREY]
    _cat_ctry_color = {c: _cat_ctry_pal[i % len(_cat_ctry_pal)] for i, c in enumerate(_cat_ctry_names)}

    # Pagination via session state — reset when sort or filters change
    _cat_page_key = f"cat_n_{sc}_{sa}_{len(dr_f)}"
    if st.session_state.get("_last_cat_key") != _cat_page_key:
        st.session_state["cat_visible"] = 10
        st.session_state["_last_cat_key"] = _cat_page_key
    _n_visible = st.session_state.get("cat_visible", 10)
    df_cat = df_cat_all.head(_n_visible)

    # Render cards as pure HTML block — fast and always visible
    cards_html = ""
    for _, row in df_cat.iterrows():
        import html as _html2
        major    = bool(row.get("is_major_reform", False))
        orient   = str(row.get("growth_orientation") or "unclear_or_neutral")
        tag_col  = ORIENTATION_COLORS.get(orient, GREY)
        tag_txt  = ORIENTATION_LABELS.get(orient, "Unclear / Neutral")
        impl_yr  = row.get("implementation_year")
        yr_s     = str(int(float(impl_yr))) if pd.notna(impl_yr) else "n.d."
        surv_yr  = row.get("survey_year")
        surv_s   = f"Survey {int(float(surv_yr))}" if pd.notna(surv_yr) else ""
        first_seen = row.get("first_seen_survey_year")
        last_seen = row.get("last_seen_survey_year")
        first_seen_s = str(int(float(first_seen))) if pd.notna(first_seen) else "—"
        last_seen_s = str(int(float(last_seen))) if pd.notna(last_seen) else "—"
        sub_key  = str(row.get("sub_theme") or "other")
        sub_s    = SUBTHEME_LABELS.get(sub_key, sub_key.replace("_", " ").title())
        lbl_clr  = SUBTHEME_COLORS.get(sub_key, GREY)
        status_s = STATUS_LABELS.get(str(row.get("status") or ""), str(row.get("status") or "—").title())
        actor_s  = ACTOR_LABELS.get(str(row.get("rd_actor") or "unknown"), "—")
        stage_s  = STAGE_LABELS.get(str(row.get("rd_stage") or "unknown"), "—")
        imp      = row.get("importance_bucket")
        imp_s    = f"{int(imp)}/3" if pd.notna(imp) else "—"
        country_s  = _html2.escape(str(row.get("country_name") or "—"))
        ctry_color = _cat_ctry_color.get(str(row.get("country_name") or ""), NAVY)
        desc_s   = _html2.escape(str(row.get("description") or ""))
        quote_s  = str(row.get("source_quote") or "")
        source_page_start = row.get("source_page_start")
        source_page_end = row.get("source_page_end")
        survey_label = str(row.get("country_code") or "")
        mention_yrs = str(row.get("all_seen_survey_years") or row.get("mention_survey_years") or "")

        # ── CV metadata (merged stage only) ──────────────────────────────────
        cv_status   = str(row.get("cross_verification_status") or "")
        cv_note     = str(row.get("cross_verification_note") or "")
        found_by    = str(row.get("found_by_display") or row.get("found_by_models") or "")
        verification_bucket = str(row.get("verification_bucket") or "")
        cv_included = row.get("cv_included")
        is_excluded = (
            sel_stage == "merged" and
            cv_status in {
                "three_model_rejected", "two_model_excluded", "one_model_excluded",
                "consensus_rejected", "disputed_excluded",
            }
        )

        # CV agreement badge (merged stage only)
        _cv_badge_map = {
            "All 3 models agreed":             ("#1a7340", "#d4edda", "All 3 models agreed"),
            "2 of 3 models":                   ("#1a4f7a", "#d0e4f7", "2 of 3 models"),
            "Both models agreed":              ("#1a4f7a", "#d0e4f7", "Both models agreed"),
            "1 model only":                    ("#7a5a00", "#fff3cd", "1 model only"),
            "Excluded — all 3 models agreed":  ("#8b0000", "#fde8e8", "Excluded — all 3 agreed"),
            "Excluded — 2 of 3 models":        ("#8b0000", "#fde8e8", "Excluded — 2 of 3"),
            "Excluded — both models agreed":   ("#8b0000", "#fde8e8", "Excluded — both agreed"),
            "Excluded — 1 model only":         ("#666",    "#f0f0f0", "Excluded — 1 model"),
        }
        if sel_stage == "merged" and verification_bucket in _cv_badge_map:
            _cv_fc, _cv_bg, _cv_lbl = _cv_badge_map[verification_bucket]
            cv_badge = (
                f'<span style="display:inline-block;padding:1px 7px;border-radius:2px;'
                f'background:{_cv_bg};color:{_cv_fc};border:1px solid {_cv_fc}30;'
                f'font-weight:700;font-size:.64rem;letter-spacing:.03em;">'
                f'{_cv_lbl}</span>'
            )
        else:
            cv_badge = ""

        # Card left-border: red for excluded, country-color for included
        card_border_color = "#cc3333" if is_excluded else ctry_color
        card_bg = "#fff8f8" if is_excluded else "#fff"

        major_badge = (
            f'<span style="display:inline-block;padding:1px 7px;border-radius:2px;'
            f'background:{NAVY};color:#fff;font-weight:700;font-size:.66rem;'
            f'letter-spacing:.04em;">MAJOR</span>'
            if major else
            f'<span style="display:inline-block;padding:1px 7px;border-radius:2px;'
            f'background:#F1F3F6;color:#666;border:1px solid #D7DCE3;'
            f'font-weight:700;font-size:.66rem;letter-spacing:.04em;">NOT MAJOR</span>'
        )
        if pd.notna(source_page_start) and pd.notna(source_page_end):
            if int(float(source_page_start)) == int(float(source_page_end)):
                source_pages_s = f"p. {int(float(source_page_start))}"
            else:
                source_pages_s = f"pp. {int(float(source_page_start))}-{int(float(source_page_end))}"
        elif pd.notna(source_page_start):
            source_pages_s = f"p. {int(float(source_page_start))}"
        else:
            source_pages_s = ""
        source_meta_parts = []
        if survey_label and surv_s:
            source_meta_parts.append(f"{survey_label} {surv_s.replace('Survey ', '')}")
        elif surv_s:
            source_meta_parts.append(surv_s)
        if source_pages_s:
            source_meta_parts.append(source_pages_s)
        source_meta_s = " · ".join(source_meta_parts)
        quote_block = (
            f'<div style="margin:.55rem 0 .25rem;padding:.45rem .75rem;background:#FAFBFD;'
            f'border-left:3px solid {lbl_clr};color:#555;border-radius:2px;">'
            f'<div style="font-size:.67rem;color:#7a7a7a;font-weight:700;letter-spacing:.03em;'
            f'text-transform:uppercase;margin-bottom:.2rem;">Source quote'
            f'{(" · " + _html2.escape(source_meta_s)) if source_meta_s else ""}</div>'
            f'<div style="font-size:.79rem;font-style:italic;">'
            f'&ldquo;{_html2.escape(quote_s[:420])}{"…" if len(quote_s) > 420 else ""}&rdquo;'
            f'</div></div>'
            if quote_s else ""
        )
        # Adjudicator rationale — always show for excluded, optionally for included
        cv_note_block = (
            f'<div style="font-size:.7rem;color:{"#aa2222" if is_excluded else "#777"};'
            f'margin-top:.3rem;padding:.25rem .5rem;'
            f'background:{"#fde8e8" if is_excluded else "#F7F8FA"};border-radius:3px;">'
            f'<b>Adjudicator:</b> {_html2.escape(cv_note)}</div>'
            if cv_note and sel_stage == "merged" else ""
        )
        found_by_block = (
            f'<div style="font-size:.69rem;color:#aaa;margin-top:.2rem;">'
            f'Found by: {_html2.escape(found_by)}</div>'
            if found_by and sel_stage == "merged" else ""
        )
        mentions_block = (
            f'<div style="font-size:.69rem;color:#aaa;margin-top:.4rem;">'
            f'Anchor survey: {_html2.escape(surv_s) if surv_s else "—"}'
            f' · First seen: {_html2.escape(first_seen_s)}'
            f' · Last seen: {_html2.escape(last_seen_s)}'
            f'{(" · Seen in: " + _html2.escape(str(mention_yrs))) if mention_yrs else ""}'
            f'</div>'
            if surv_s or mention_yrs or pd.notna(first_seen) or pd.notna(last_seen) else ""
        )

        cards_html += textwrap.dedent(f"""
        <div style="border:1px solid {BORDER};border-left:4px solid {card_border_color};border-radius:0 5px 5px 0;padding:.75rem 1rem;margin-bottom:.55rem;background:{card_bg};">
          <div style="display:flex;align-items:center;gap:.45rem;flex-wrap:wrap;margin-bottom:.4rem;">
            <span style="font-size:.75rem;font-weight:700;color:{ctry_color};background:{ctry_color}12;padding:2px 8px;border-radius:3px;border:1px solid {ctry_color}40;">{country_s}</span>
            <span style="font-size:.75rem;font-weight:700;color:{NAVY};background:#EEF3FB;padding:2px 8px;border-radius:3px;">{yr_s}</span>
            <span style="display:inline-block;padding:2px 9px;border-radius:2px;background:{lbl_clr}15;color:{lbl_clr};border:1px solid {lbl_clr}40;font-weight:700;font-size:.68rem;">{_html2.escape(sub_s)}</span>
            {major_badge}
            {cv_badge}
            <span style="margin-left:auto;font-size:.71rem;color:#888;font-weight:600;">{_html2.escape(status_s)}</span>
          </div>
          <div style="display:grid;grid-template-columns:1fr 155px;gap:.75rem;">
            <div>
              <div style="font-size:.84rem;font-weight:700;color:{TEXT};line-height:1.45;{'opacity:.6;' if is_excluded else ''}">{desc_s}</div>
              {quote_block}
              {cv_note_block}
              {found_by_block}
              {mentions_block}
            </div>
            <div style="font-size:.73rem;color:{TEXT};line-height:1.9;border-left:1px solid {BORDER};padding-left:.75rem;">
              <span style="display:inline-block;padding:2px 8px;border-radius:2px;background:{tag_col}15;color:{tag_col};border:1px solid {tag_col}40;font-weight:700;font-size:.67rem;">{_html2.escape(tag_txt)}</span><br>
              <span style="color:#888;font-size:.69rem;font-weight:700;text-transform:uppercase;letter-spacing:.04em;">Actor</span><br>
              <span style="color:{TEXT};">{_html2.escape(actor_s)}</span><br>
              <span style="color:#888;font-size:.69rem;font-weight:700;text-transform:uppercase;letter-spacing:.04em;">R&amp;D stage</span><br>
              <span style="color:{TEXT};">{_html2.escape(stage_s)}</span><br>
              <span style="color:#888;font-size:.69rem;font-weight:700;text-transform:uppercase;letter-spacing:.04em;">Importance</span><br>
              <span style="color:{TEXT};">{_html2.escape(imp_s)}</span>
            </div>
          </div>
        </div>
        """).strip()

    st.markdown(cards_html, unsafe_allow_html=True)

    # Load more / count indicator
    _remaining = len(df_cat_all) - _n_visible
    if _remaining > 0:
        _load_cols = st.columns([1, 2, 1])
        with _load_cols[1]:
            caption_note(f"Showing {_n_visible} of {len(df_cat_all)} reforms")
            if st.button(f"Load {min(10, _remaining)} more", key="cat_load_more", use_container_width=True):
                st.session_state["cat_visible"] = _n_visible + 10
                st.rerun()
    else:
        caption_note(f"Showing all {len(df_cat_all)} reforms")

    # ── Data table ──
    section_header("Reform event detail")
    _base_ref_cols = [
        "country_name", "survey_year", "implementation_year", "sub_theme_label",
        "orientation_label", "status_label", "is_major_reform",
        "importance_bucket", "rd_actor_label", "rd_stage_label",
        "package_name", "description",
    ]
    # Add CV columns for merged stage
    if sel_stage == "merged":
        _base_ref_cols = ["verification_bucket", "cross_verification_status", "found_by_display"] + _base_ref_cols
        if show_excluded:
            _base_ref_cols = ["cv_included"] + _base_ref_cols
    _REF_DISP_COLS = [c for c in _base_ref_cols if c in dr_f.columns]
    _REF_COL_LABELS = {
        "country_name": "Country", "survey_year": "Survey year",
        "cv_included": "Included",
        "verification_bucket": "Verification",
        "cross_verification_status": "CV Status",
        "found_by_display": "Found by",
        "found_by_models": "Found by (raw)",
        "first_seen_survey_year": "First seen",
        "last_seen_survey_year": "Last seen",
        "all_seen_survey_years": "Seen in surveys",
        "implementation_year": "Year",
        "sub_theme_label": "Innovation type", "orientation_label": "Growth orientation",
        "status_label": "Status", "is_major_reform": "Major?",
        "importance_bucket": "Importance", "rd_actor_label": "Actor",
        "rd_stage_label": "Stage", "package_name": "Reform name",
        "description": "Description",
    }
    _ref_search = st.text_input(
        "Search table", key="ref_search",
        placeholder="Innovation type, country, description…",
        label_visibility="collapsed",
    )
    _tbl_r = dr_f.copy()
    if _ref_search:
        mask_r = _tbl_r.astype(str).apply(
            lambda col: col.str.contains(_ref_search, case=False, na=False)
        ).any(axis=1)
        _tbl_r = _tbl_r[mask_r]
    caption_note(f"{len(_tbl_r):,} reforms")
    render_table(
        _tbl_r[_REF_DISP_COLS].sort_values("implementation_year"
                                            if "implementation_year" in _REF_DISP_COLS else _REF_DISP_COLS[0]),
        col_labels=_REF_COL_LABELS,
        num_cols=["survey_year", "first_seen_survey_year", "last_seen_survey_year", "implementation_year", "importance_bucket"],
        bool_cols=["is_major_reform"],
        wide_cols=["all_seen_survey_years", "description", "package_name"],
    )
    st.download_button(
        "Download CSV",
        _tbl_r[_REF_DISP_COLS].to_csv(index=False).encode("utf-8"),
        "reforms_filtered.csv", "text/csv", key="ref_dl",
    )

    # ── Survey coverage ──
    section_header("Survey coverage — reforms per survey")
    # Use the full (unfiltered) stage data so sidebar filters don't distort coverage
    _mentions = load_reforms_stage(sel_stage, included_only=(sel_stage == "merged"))
    if not _mentions.empty and "survey_year" in _mentions.columns:
        _multi_surv = "country_code" in _mentions.columns and _mentions["country_code"].nunique() > 1

        if _multi_surv:
            _ctry_names_m = sorted(_mentions["country_code"].dropna().unique())
            _surv_pal = [NAVY, ORANGE, TEAL, GREEN, BLUE, GREY]
            _surv_color_map = {c: _surv_pal[i % len(_surv_pal)] for i, c in enumerate(_ctry_names_m)}
            surv_cnt = (
                _mentions.groupby(["survey_year", "country_code"]).size()
                .reset_index(name="n").sort_values("survey_year")
            )
            fig_surv = px.bar(
                surv_cnt, x="survey_year", y="n",
                color="country_code",
                color_discrete_map=_surv_color_map,
                barmode="stack",
                labels={"survey_year": "Survey year", "n": "Reforms", "country_code": ""},
            )
            fig_surv.update_traces(marker_line_width=0)
            apply_style(fig_surv, height=260, xtitle="Survey year", ytitle="Reforms")
            fig_surv.update_xaxes(showgrid=False)
        else:
            surv_cnt = (
                _mentions.groupby("survey_year").size().reset_index(name="n")
                .sort_values("survey_year")
            )
            fig_surv = go.Figure(go.Bar(
                x=surv_cnt["survey_year"].astype(int),
                y=surv_cnt["n"],
                marker_color=NAVY, marker_line_width=0,
                text=surv_cnt["n"], textposition="outside",
                textfont=dict(size=10, color=TEXT),
            ))
            apply_style(fig_surv, height=260,
                        xtitle="Survey year", ytitle="Reforms",
                        legend_bottom=False)
            fig_surv.update_layout(showlegend=False, xaxis=dict(showgrid=False))
            fig_surv.update_yaxes(range=[0, surv_cnt["n"].max() * 1.2])

        st.plotly_chart(fig_surv, use_container_width=True)
        _surv_n = _mentions["survey_year"].nunique()
        _ctry_n = _mentions["country_code"].nunique() if "country_code" in _mentions.columns else 1
        caption_note(
            f"{len(_mentions):,} reforms across {_surv_n} surveys "
            f"({_ctry_n} {'country' if _ctry_n == 1 else 'countries'})."
        )

    # ── Downloads ──
    section_header("Download data")
    _dl_cols = st.columns(2)
    with _dl_cols[0]:
        # Full database for this stage
        _full_db_path = STAGE_PATHS.get(sel_stage, {}).get("database")
        if _full_db_path and _full_db_path.exists():
            _full_db_bytes = _full_db_path.read_bytes()
            _dl_label = {
                "stage1": "reforms_stage1_gpt4omini.csv",
                "stage2": "reforms_stage2_claude_haiku.csv",
                "stage3": "reforms_stage3_gpt4omini_2ndrun.csv",
                "merged": "reforms_cross_verified.csv",
            }.get(sel_stage, "reforms_database.csv")
            st.download_button(
                "Download full database (CSV)",
                _full_db_bytes,
                _dl_label,
                "text/csv",
                key="ref_dl_full",
                use_container_width=True,
            )
            st.caption(
                "All reforms in this stage" +
                (" including excluded ones" if sel_stage == "merged" and _full_db_path.exists() else "")
            )
    with _dl_cols[1]:
        # Filtered view (what is visible on screen)
        _tbl_dl = _tbl_r[_REF_DISP_COLS] if "_tbl_r" in dir() and "_REF_DISP_COLS" in dir() else dr_f
        st.download_button(
            "Download filtered view (CSV)",
            dr_f.to_csv(index=False).encode("utf-8"),
            "reforms_filtered.csv",
            "text/csv",
            key="ref_dl_filtered",
            use_container_width=True,
        )
        st.caption("Only reforms matching the current sidebar filters")



# ═════════════════════════════════════════════════════════════════════════════
# TAB 3 — COMBINED VIEW
# ═════════════════════════════════════════════════════════════════════════════

with TAB_COMBINED:
    bk = budget_available()
    rk = not _dr.empty

    # ── Stream comparison ──
    section_header("Stream comparison")
    # Build dynamic time-range labels from actual data
    if bk:
        _db_cmp = _filtered_budget_df()
        _bud_yrs_cmp = sorted(_db_cmp["year"].dropna().unique())
        _bud_ctry_cmp = sorted(_db_cmp["country"].dropna().unique()) if "country" in _db_cmp.columns else ["Denmark"]
        _s1_range = f"{min(_bud_yrs_cmp)}&#8211;{max(_bud_yrs_cmp)} ({', '.join(_bud_ctry_cmp)})" if _bud_yrs_cmp else "n/a"
    else:
        _s1_range = "n/a"
    if rk:
        _dr_cmp = _dr
        _surv_yrs_cmp = sorted(_dr_cmp["survey_year"].dropna().astype(int).unique()) if "survey_year" in _dr_cmp.columns else []
        _ref_ctry_cmp = sorted(_dr_cmp["country_name"].dropna().unique()) if "country_name" in _dr_cmp.columns else []
        _s2_range = (
            f"{min(_surv_yrs_cmp)}&#8211;present ({', '.join(_ref_ctry_cmp[:3])}{'…' if len(_ref_ctry_cmp) > 3 else ''})"
            if _surv_yrs_cmp else "n/a"
        )
    else:
        _s2_range = "n/a"
    _cmp_rows = [
        ("Measures",     "Amount budgeted for R&amp;D (local currency)",  "Innovation policy reforms enacted"),
        ("Time range",   _s1_range,                                    _s2_range),
        ("Unit",         "Budget line &#8594; Ministry &#8594; R&amp;D category",
                         "Reform event &#8594; sub-type &#8594; actor &#8594; stage"),
        ("Method",       "OCR + J-Rule taxonomy scoring",              "LLM extraction + cross-survey dedup"),
        ("Analytic use", "R&amp;D intensity (<em>how much</em>)",      "Reform direction (<em>what changed</em>)"),
    ]
    _cmp_body = ""
    for i, (label, s1, s2) in enumerate(_cmp_rows):
        bg = LGREY if i % 2 else "#fff"
        _cmp_body += (
            f'<tr style="border-bottom:1px solid {BORDER};background:{bg};">'
            f'<td style="padding:.4rem .8rem;font-weight:700;color:#777;white-space:nowrap;">{label}</td>'
            f'<td style="padding:.4rem .8rem;">{s1}</td>'
            f'<td style="padding:.4rem .8rem;">{s2}</td>'
            f'</tr>'
        )
    st.markdown(
        f'<table style="width:100%;border-collapse:collapse;font-size:.82rem;color:{TEXT};">'
        f'<thead><tr style="background:{LGREY};border-bottom:2px solid {BORDER};">'
        f'<th style="padding:.45rem .8rem;text-align:left;color:{NAVY};"></th>'
        f'<th style="padding:.45rem .8rem;text-align:left;color:{NAVY};">Stream 1 &#8212; Finance Bills</th>'
        f'<th style="padding:.45rem .8rem;text-align:left;color:{NAVY};">Stream 2 &#8212; OECD Surveys</th>'
        f'</tr></thead><tbody>{_cmp_body}</tbody></table>',
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    # ── Reform sub-type + orientation breakdown ──
    if rk:
        if not dr_f.empty:
            col3a, col3b = st.columns(2)

            with col3a:
                section_header("Reforms by innovation sub-type")
                st_cnt = (dr_f.groupby("sub_theme").size().reset_index(name="n")
                          .sort_values("n", ascending=True))
                st_cnt["label"] = st_cnt["sub_theme"].map(lambda x: SUBTHEME_SHORT.get(x, x))
                fig_st = go.Figure(go.Bar(
                    x=st_cnt["n"], y=st_cnt["label"],
                    orientation="h",
                    marker_color=[SUBTHEME_COLORS.get(k, GREY) for k in st_cnt["sub_theme"]],
                    marker_line_width=0,
                    text=st_cnt["n"], textposition="outside",
                    textfont=dict(size=11, color=TEXT),
                ))
                apply_style(fig_st, height=280, xtitle="Reform events", legend_bottom=False)
                fig_st.update_layout(showlegend=False, yaxis=dict(showgrid=False))
                fig_st.update_xaxes(range=[0, st_cnt["n"].max() * 1.25])
                st.plotly_chart(fig_st, use_container_width=True)

            with col3b:
                section_header("Reform timeline — year × sub-type")
                _df_tl = dr_f.dropna(subset=["display_year"]).copy()
                if not _df_tl.empty:
                    _df_tl["yr"] = _df_tl["display_year"].astype(int)
                    _df_tl["sub_short"] = _df_tl["sub_theme"].map(lambda x: SUBTHEME_SHORT.get(x, x))
                    _df_tl["importance"] = _df_tl["importance_bucket"].fillna(1).astype(int)
                    _df_tl["label"] = _df_tl.apply(
                        lambda r: str(r.get("package_name") or r.get("description",""))[:60], axis=1
                    )
                    fig_tl = px.scatter(
                        _df_tl, x="yr", y="sub_short",
                        size="importance",
                        color="sub_theme",
                        color_discrete_map=SUBTHEME_COLORS,
                        hover_name="label",
                        hover_data={"yr": True, "sub_short": False,
                                    "sub_theme": False, "importance": True},
                        labels={"yr": "Year", "sub_short": "", "importance": "Importance"},
                        size_max=20,
                    )
                    fig_tl.update_traces(marker_line_width=0)
                    apply_style(fig_tl, height=280, xtitle="Year", legend_bottom=False)
                    fig_tl.update_layout(showlegend=False, yaxis=dict(showgrid=True))
                    st.plotly_chart(fig_tl, use_container_width=True)
                    caption_note("Bubble size = importance (1–3). Hover for reform name.")
                else:
                    st.info("No reforms with an assigned year yet.")

    # ── Dual-axis overlay ──
    if bk and rk and not dr_f.empty:
        section_header("R&D budget allocation vs. innovation reform activity")
        _db3 = _filtered_budget_df()
        if not _db3.empty:
            _db3_ccy = _db3["currency"].dropna().unique() if "currency" in _db3.columns else []
            _db3_multi = len(_db3_ccy) > 1
            _db3_lbl = _db3_ccy[0] if len(_db3_ccy) == 1 else "local currency"
            df_i3 = dr_f.dropna(subset=["display_year"]).copy()
            df_i3["yr"] = df_i3["display_year"].astype(int)
            rc3 = df_i3.groupby("yr").size().reset_index(name="n")
            fig_dual = go.Figure()
            if _db3_multi:
                if _db3["country"].nunique() == 1:
                    b_yr3 = _db3.groupby(["year", "currency"], dropna=False)["amount_local"].sum().reset_index()
                    b_yr3["label"] = b_yr3["currency"].fillna("Unknown currency")
                    _bar_colors = _budget_currency_color_map(sorted(b_yr3["label"].dropna().unique()))
                else:
                    b_yr3 = _db3.groupby(["year", "country", "currency"], dropna=False)["amount_local"].sum().reset_index()
                    b_yr3["label"] = b_yr3["country"] + " (" + b_yr3["currency"].fillna("Unknown") + ")"
                    _labels = sorted(b_yr3["label"].dropna().unique())
                    _pal = [NAVY, ORANGE, TEAL, GREEN, BLUE, "#9B59B6", "#E74C3C", GREY]
                    _bar_colors = {lab: _pal[i % len(_pal)] for i, lab in enumerate(_labels)}
                b_yr3["Amt M"] = b_yr3["amount_local"] / 1e6
                for _label in sorted(b_yr3["label"].dropna().unique()):
                    _sub = b_yr3[b_yr3["label"] == _label]
                    fig_dual.add_trace(go.Bar(
                        x=_sub["year"], y=_sub["Amt M"],
                        name=str(_label),
                        marker_color=_bar_colors.get(_label, NAVY),
                        opacity=0.8, marker_line_width=0, yaxis="y1",
                    ))
                _ytitle_dual = "R&D Budget (millions, local currency)"
            else:
                b_yr3 = _db3.groupby("year")["amount_local"].sum().reset_index()
                b_yr3["Amt M"] = b_yr3["amount_local"] / 1e6
                fig_dual.add_trace(go.Bar(
                    x=b_yr3["year"], y=b_yr3["Amt M"],
                    name=f"R&D budget ({_db3_lbl} M)",
                    marker_color=NAVY, opacity=0.72, marker_line_width=0, yaxis="y1",
                ))
                _ytitle_dual = f"R&D Budget ({_db3_lbl} millions)"
            if not rc3.empty:
                fig_dual.add_trace(go.Scatter(
                    x=rc3["yr"], y=rc3["n"],
                    name="Innovation reform events",
                    mode="lines+markers",
                    line=dict(color=ORANGE, width=2.5),
                    marker=dict(size=8, color=ORANGE, line=dict(width=2, color="white")),
                    yaxis="y2",
                ))
            if "is_major_reform" in df_i3.columns:
                maj3 = df_i3[df_i3["is_major_reform"]].groupby("yr").size().reset_index(name="nm")
                if not maj3.empty:
                    fig_dual.add_trace(go.Scatter(
                        x=maj3["yr"], y=maj3["nm"],
                        name="Major reform events",
                        mode="markers",
                        marker=dict(symbol="diamond", size=14,
                                    color="white", line=dict(width=2.5, color=ORANGE)),
                        yaxis="y2",
                    ))
            fig_dual.update_layout(
                height=400,
                xaxis=dict(title="Year", dtick=2, showgrid=False,
                           linecolor=BORDER, tickfont=dict(size=10.5)),
                yaxis=dict(title=_ytitle_dual, side="left",
                           gridcolor="#EBEBEB", linecolor=BORDER),
                yaxis2=dict(title="Reform event count", side="right",
                            overlaying="y", showgrid=False, linecolor=BORDER),
                legend=dict(orientation="h", y=1.05, x=0, font=dict(size=10.5),
                            bgcolor="rgba(0,0,0,0)"),
                barmode="group" if _db3_multi else "relative",
                **PLOTLY_BASE,
                margin=dict(t=44, b=36, l=8, r=8),
            )
            st.plotly_chart(fig_dual, use_container_width=True)
            _b_span = f"{b_yr3['year'].min()}–{b_yr3['year'].max()}" if not b_yr3.empty else "n/a"
            _r_span = f"{rc3['yr'].min()}–{rc3['yr'].max()}" if not rc3.empty else "n/a"
            _dual_note = (
                "Budget bars are split by currency because the current selection mixes monetary regimes."
                if _db3_multi else
                "Budget bars are shown in a single currency."
            )
            caption_note(
                f"Finance Bills: {_b_span}. OECD Survey reforms: {_r_span}. {_dual_note}"
            )

    # ── Subtheme composition over time (reform_panel_subtheme) ──
    if rk:
        _pst = load_reform_panel_subtheme()
        _pst_f = _pst[_pst["country_code"].isin(sel_ctry)] if sel_ctry and not _pst.empty else _pst
        if not _pst_f.empty and "reform_count" in _pst_f.columns and _pst_f["reform_count"].sum() > 0:
            section_header("Innovation reform composition by sub-type over time")
            _pst_act = _pst_f[_pst_f["reform_count"] > 0].copy()
            if not _pst_act.empty:
                _pst_act["year"] = _pst_act["year"].astype(int)
                _pst_act["sub_short"] = _pst_act["sub_theme"].map(
                    lambda x: SUBTHEME_SHORT.get(x, x)
                )
                pst_order = (
                    _pst_act.groupby("sub_theme")["reform_count"]
                    .sum().sort_values(ascending=False).index.tolist()
                )
                fig_pst = px.bar(
                    _pst_act, x="year", y="reform_count",
                    color="sub_theme",
                    color_discrete_map=SUBTHEME_COLORS,
                    barmode="stack",
                    category_orders={"sub_theme": pst_order},
                    labels={"year": "Year", "reform_count": "Reform events", "sub_theme": ""},
                )
                for trace in fig_pst.data:
                    trace.name = SUBTHEME_SHORT.get(trace.name, trace.name)
                fig_pst.update_traces(marker_line_width=0)
                apply_style(fig_pst, height=300, xtitle="Year", ytitle="Reform events")
                st.plotly_chart(fig_pst, use_container_width=True)
                caption_note(
                    "Each bar shows the mix of innovation sub-types enacted in a given year. "
                    "Source: reform_panel_subtheme.csv — country × year × sub-type panel."
                )

    # ── Reform intensity score ──
    if not _dr.empty:
        panel_df = load_reform_panel_stage(sel_stage)
        if "reform_intensity_score" in panel_df.columns:
            section_header("Reform intensity score (composite 0–1 indicator)")
            caption_note(
                "Four equal-weighted components: (1) reform volume [log-scaled], "
                "(2) share growth-supporting, (3) share major reforms, "
                "(4) sub-type diversity.  Score = 0 for country-years with no reforms."
            )
            sc_df = panel_df[panel_df["reform_intensity_score"] > 0].copy()
            if not sc_df.empty:
                multi = sc_df["country_code"].nunique() > 1
                fig_sc = px.line(
                    sc_df, x="year", y="reform_intensity_score",
                    color="country_code" if multi else None,
                    markers=True,
                    color_discrete_sequence=[NAVY, ORANGE, TEAL, GREEN],
                    labels={"reform_intensity_score": "Intensity (0–1)",
                            "year": "Year", "country_code": "Country"},
                )
                apply_style(fig_sc, height=270, ytitle="Intensity score (0–1)", xtitle="Year")
                fig_sc.update_yaxes(range=[0, 1.05], gridcolor="#EBEBEB")
                fig_sc.update_traces(line_width=2.2)
                st.plotly_chart(fig_sc, use_container_width=True)
            else:
                st.info("Run the reform pipeline to populate the intensity score.")

    # ── Budget trend (Stream 1 only, always show if available) ──
    if bk:
        section_header("R&D budget by year (Stream 1)")
        _db3 = _filtered_budget_df()
        if not _db3.empty:
            _b3_ccy = _db3["currency"].dropna().unique() if "currency" in _db3.columns else []
            _b3_lbl = _b3_ccy[0] if len(_b3_ccy) == 1 else "local currency"
            _b3_multi = len(_b3_ccy) > 1
            if _b3_multi:
                if _db3["country"].nunique() == 1:
                    b_yr3 = _db3.groupby(["year", "currency"], dropna=False)["amount_local"].sum().reset_index()
                    b_yr3["label"] = b_yr3["currency"].map(_budget_currency_label).fillna("Unknown currency")
                    _color_map3 = _budget_currency_color_map(sorted(b_yr3["label"].dropna().unique()))
                    _color_col = "label"
                    _b3_cap = "Bars are grouped by currency because the selected budget panel mixes monetary regimes."
                else:
                    b_yr3 = _db3.groupby(["year", "country", "currency"], dropna=False)["amount_local"].sum().reset_index()
                    b_yr3["label"] = b_yr3["country"] + " (" + b_yr3["currency"].map(_budget_currency_label).fillna("Unknown") + ")"
                    _labels = sorted(b_yr3["label"].dropna().unique())
                    _color_map3 = _budget_currency_color_map(_labels)
                    _color_col = "label"
                    _b3_cap = "Bars are grouped by country-currency pair. Levels are not comparable across currencies."
            else:
                b_yr3 = _db3.groupby("year")["amount_local"].sum().reset_index()
                _color_map3 = None
                _color_col = None
                _b3_cap = "Bars are shown in a single currency."
            b_yr3["Amt M"] = b_yr3["amount_local"] / 1e6
            _b3_ytitle = "Amount (millions, local currency)" if _b3_multi else f"{_b3_lbl} (millions)"
            fig_b3 = px.bar(
                b_yr3, x="year", y="Amt M",
                color=_color_col,
                labels={"year": "Year", "Amt M": _b3_ytitle, "country": "Country", "label": ""},
                color_discrete_map=_color_map3,
                color_discrete_sequence=[NAVY] if not _b3_multi else None,
                barmode="group" if _b3_multi else "relative",
            )
            fig_b3.update_traces(marker_line_width=0)
            apply_style(fig_b3, height=240, xtitle="Year", ytitle=_b3_ytitle)
            st.plotly_chart(fig_b3, use_container_width=True)
            _b3_countries = sorted(_db3["country"].dropna().unique()) if "country" in _db3.columns else []
            caption_note(f"Finance Bills — {', '.join(_b3_countries) if _b3_countries else 'all countries'}. {_b3_cap}")

    # ── Top reforms table ──
    if rk and not dr_f.empty:
        section_header("Key reform events")
        _top = (dr_f.sort_values("importance_bucket", ascending=False)
                .head(20).copy())
        _top_cols = [c for c in ["country_name","survey_year","first_seen_survey_year","last_seen_survey_year",
                                  "all_seen_survey_years","implementation_year","sub_theme_label",
                                  "status_label","importance_bucket","is_major_reform",
                                  "package_name","description"] if c in _top.columns]
        _top_labels = {
            "country_name": "Country", "survey_year": "Anchor survey",
            "first_seen_survey_year": "First seen", "last_seen_survey_year": "Last seen",
            "all_seen_survey_years": "Seen in surveys", "implementation_year": "Year",
            "sub_theme_label": "Type", "status_label": "Status",
            "importance_bucket": "Importance", "is_major_reform": "Major?",
            "package_name": "Reform", "description": "Description",
        }
        render_table(_top[_top_cols], col_labels=_top_labels,
                     num_cols=["survey_year","first_seen_survey_year","last_seen_survey_year","implementation_year","importance_bucket"],
                     bool_cols=["is_major_reform"], wide_cols=["all_seen_survey_years","description","package_name"])

    # ── Multi-country heatmap (only when >1 country) ──
    if rk and not dr_f.empty and dr_f["country_name"].nunique() > 1:
        section_header("Reform activity — country × year")
        pv4 = (
            dr_f.dropna(subset=["display_year"])
            .assign(yr=lambda d: d["display_year"].astype(int))
            .groupby(["country_name","yr"]).size().reset_index(name="n")
        )
        fig_ht = px.density_heatmap(
            pv4, x="yr", y="country_name", z="n",
            color_continuous_scale=[[0,"#F0F4FF"],[0.5,"#6699CC"],[1,NAVY]],
            labels={"yr":"Year","country_name":"","n":"Reforms"},
            text_auto=True,
        )
        apply_style(fig_ht, height=max(280, pv4["country_name"].nunique() * 38),
                    legend_bottom=False)
        fig_ht.update_traces(textfont=dict(size=11, color="white"))
        fig_ht.update_coloraxes(colorbar=dict(thickness=10))
        st.plotly_chart(fig_ht, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 4 — DATA TABLE
# ═════════════════════════════════════════════════════════════════════════════

with TAB_TABLE:
    view = st.radio(
        "Dataset", ["Budget lines", "Reform events"],
        horizontal=True, label_visibility="collapsed",
    )

    _T5_BUD_LABELS = {
        "country": "Country", "year": "Year", "section_code": "Ministry code",
        "ministry_display": "Ministry", "budget_line_display": "Description",
        "amount_local": "Amount (local currency)", "unit": "Unit", "currency": "Currency", "currency_era": "Currency era",
        "budget_category": "R&D category", "item_type": "Item type",
        "source_file": "Source", "page_number": "Page", "series_notes": "Series notes",
    }
    _T5_REF_LABELS = {
        "country_name": "Country", "survey_year": "Survey year",
        "first_seen_survey_year": "First seen",
        "last_seen_survey_year": "Last seen",
        "all_seen_survey_years": "Seen in surveys",
        "implementation_year": "Year", "sub_theme_label": "Innovation type",
        "orientation_label": "Growth orientation", "status_label": "Status",
        "is_major_reform": "Major?", "importance_bucket": "Importance",
        "rd_actor_label": "Actor", "rd_stage_label": "Stage",
        "package_name": "Reform name", "description": "Description",
        "source_quote": "Source quote",
    }

    if view == "Budget lines":
        if not budget_available():
            st.info("No budget data.")
        else:
            _db5 = load_budget()
            m5 = (_db5["year"] >= yr_b[0]) & (_db5["year"] <= yr_b[1])
            if dec_b and "decision" in _db5.columns:
                m5 &= _db5["decision"].isin(dec_b)
            if cat_b != "All": m5 &= _db5["budget_category"] == cat_b
            if sel_bud_ctry and "country" in _db5.columns:
                m5 &= _db5["country"].isin(sel_bud_ctry)
            if conf_b is not None and "confidence" in _db5.columns:
                _conf5 = pd.to_numeric(_db5["confidence"], errors="coerce")
                m5 &= _conf5.between(conf_b[0], conf_b[1], inclusive="both")
            df5 = _db5[m5]
            cols5 = [c for c in _T5_BUD_LABELS if c in df5.columns]
            _df5_disp = df5[cols5].copy()
            _t5_ccy = df5["currency"].dropna().unique() if "currency" in df5.columns else []
            if len(_t5_ccy) == 1:
                caption_note(f"{len(df5):,} rows  ·  {_t5_ccy[0]} {df5['amount_local'].sum()/1e6:,.1f} M")
            else:
                caption_note(f"{len(df5):,} rows  ·  multiple currencies (see Currency column)")
                if "country" in df5.columns and set(df5["country"].dropna().astype(str)) == {"Slovakia"}:
                    caption_note("Slovakia switches from SKK to EUR in 2009. Use the Currency era column to separate pre-2009 and 2009+ rows.")
            render_table(_df5_disp.sort_values(["year","section_code"] if "section_code" in cols5 else ["year"]),
                         col_labels=_T5_BUD_LABELS,
                         num_cols=["amount_local","page_number"],
                         wide_cols=["budget_line_display","ministry_display","series_notes"])
            st.download_button("Download (CSV)", df5[cols5].to_csv(index=False).encode(),
                               "budget_lines.csv", "text/csv")
    else:
        if _dr.empty:
            st.info("No reform data.")
        else:
            _dr5 = _dr_all
            cols5r = [c for c in _T5_REF_LABELS if c in _dr5.columns]
            caption_note(f"{len(_dr5):,} reform events")
            render_table(
                _dr5[cols5r].sort_values(["country_name","implementation_year"]
                                         if "implementation_year" in cols5r else cols5r[:1]),
                col_labels=_T5_REF_LABELS,
                num_cols=["survey_year","first_seen_survey_year","last_seen_survey_year","implementation_year","importance_bucket"],
                bool_cols=["is_major_reform"],
                wide_cols=["all_seen_survey_years","description","source_quote","package_name"],
            )
            st.download_button("Download (CSV)", _dr5[cols5r].to_csv(index=False).encode(),
                               "reform_events.csv", "text/csv")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 5 — METHODOLOGY
# ═════════════════════════════════════════════════════════════════════════════

with TAB_METHODS:
    col_l, col_r = st.columns([3, 2])

    with col_l:
        section_header("Project overview")
        st.markdown("""
This dataset measures innovation policy effort along two dimensions:

**Stream 1 — Budget allocation** tracks the monetary value of government R&D
expenditure extracted from scanned Finance Bill PDFs. Budget line items are scored
against a multilingual taxonomy (Balazs search library) using J-Rule scoring,
producing a time series of budget amounts classified by R&D category and Ministry.

**Stream 2 — Policy reforms** tracks structural changes in innovation policy extracted
from OECD Economic Survey narratives. A large language model (GPT-4o or Claude) extracts
reform events, which are deduplicated within and across survey vintages to produce a
canonical reform event panel with full metadata.
        """)

        section_header("Innovation taxonomy")
        rows_tax = "".join(
            f'<tr style="border-bottom:1px solid {BORDER};'
            f'{"background:"+LGREY if i%2 else ""}">'
            f'<td style="padding:.38rem .7rem;font-family:monospace;font-size:.75rem;'
            f'color:{NAVY};">{k}</td>'
            f'<td style="padding:.38rem .7rem;font-size:.8rem;">{v}</td>'
            f'</tr>'
            for i,(k,v) in enumerate({
                "rd_funding":              "Public R&D budgets, research councils, universities",
                "innovation_instruments":  "R&D tax credits, direct grants, innovation vouchers",
                "research_infrastructure": "Shared labs, science parks, HPC, open data",
                "knowledge_transfer":      "TTOs, spinoffs, IP regimes, university–industry collaboration",
                "startup_ecosystem":       "Incubators, accelerators, venture capital, clusters",
                "human_capital":           "Doctoral programmes, fellowships, researcher mobility",
                "sectoral_rd":             "Mission R&D: health, climate, AI, energy, defence",
                "other":                   "Innovation-relevant but does not fit above (use sparingly)",
            }.items())
        )
        st.markdown(f"""
        <table style="width:100%;border-collapse:collapse;font-size:.8rem;">
          <thead><tr style="background:{LGREY};border-bottom:2px solid {BORDER};">
            <th style="padding:.42rem .7rem;text-align:left;color:{NAVY};">Key</th>
            <th style="padding:.42rem .7rem;text-align:left;color:{NAVY};">Description</th>
          </tr></thead>
          <tbody>{rows_tax}</tbody>
        </table>
        """, unsafe_allow_html=True)

        section_header("Reform intensity score")
        st.latex(r"""
\text{Score}_{c,t} = \frac{1}{4}\Bigl(
  \underbrace{\frac{\ln(1+n)}{\ln(11)}}_{\text{volume}}
  +\underbrace{\frac{n_{gs}}{n}}_{\text{quality}}
  +\underbrace{\frac{n_{major}}{n}}_{\text{depth}}
  +\underbrace{\frac{k}{8}}_{\text{breadth}}
\Bigr)
        """)
        caption_note(
            "n = reform events · n_gs = growth-supporting · n_major = major reforms · "
            "k = distinct sub-types · Score = 0 for country-years with no reforms."
        )

    with col_r:
        section_header("Running the pipeline")
        st.code("""
# Finance Bills (no API key needed)
python main.py --budget-only

# OECD Surveys (LLM key in config.yaml)
python main.py --reforms-only \\
    --reforms-country DNK

# Rebuild panel without LLM
python main.py --reforms-build-panel-only

# Launch dashboard
streamlit run app/streamlit_app.py
        """, language="bash")

        section_header("Pipeline architecture")
        st.markdown(f"""
        <div style="font-size:.77rem;background:{LGREY};border:1px solid {BORDER};
             border-radius:4px;padding:.9rem 1.1rem;font-family:'Courier New',monospace;
             line-height:2;color:{TEXT};">
        Finance Bill PDFs<br>
        &nbsp; ↓ OCR (pytesseract / PyMuPDF)<br>
        &nbsp; ↓ J-Rule taxonomy scoring<br>
        &nbsp; ↓ <span style="color:{NAVY};font-weight:700;">results.csv</span><br>
        <br>
        OECD Survey PDFs<br>
        &nbsp; ↓ pdfplumber + section prioritisation<br>
        &nbsp; ↓ LLM extraction (chunked)<br>
        &nbsp; ↓ Within-survey deduplication<br>
        &nbsp; ↓ Cross-survey deduplication<br>
        &nbsp; ↓ <span style="color:{NAVY};font-weight:700;">reform_panel.csv</span>
        </div>
        """, unsafe_allow_html=True)

        section_header("Output files")
        rows_out = "".join(
            f'<tr style="border-bottom:1px solid {BORDER};'
            f'{"background:"+LGREY if i%2 else ""}">'
            f'<td style="padding:.35rem .6rem;font-family:monospace;font-size:.7rem;color:{NAVY};">{f}</td>'
            f'<td style="padding:.35rem .6rem;font-size:.78rem;">{d}</td>'
            f'</tr>'
            for i,(f,d) in enumerate([
                ("results.csv",                "R&D budget lines"),
                ("results_ai_verified.csv",    "AI-validated subset"),
                ("reforms_events.csv",         "Deduplicated reform events"),
                ("reform_panel.csv",           "Country × year panel"),
                ("reform_panel_subtheme.csv",  "Long panel by sub-type"),
            ])
        )
        st.markdown(f"""
        <table style="width:100%;border-collapse:collapse;">
          <thead><tr style="background:{LGREY};border-bottom:2px solid {BORDER};">
            <th style="padding:.35rem .6rem;text-align:left;font-size:.73rem;color:{NAVY};">File</th>
            <th style="padding:.35rem .6rem;text-align:left;font-size:.73rem;color:{NAVY};">Contents</th>
          </tr></thead>
          <tbody>{rows_out}</tbody>
        </table>
        """, unsafe_allow_html=True)

        st.markdown(f'<br><div style="font-size:.68rem;color:#aaa;">pandas · pdfplumber · pytesseract · openai / anthropic · streamlit · plotly</div>',
                    unsafe_allow_html=True)
