"""
Innovation Policy Dataset — Research Dashboard
Run:  streamlit run app/streamlit_app.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.data_loader import (
    ACTOR_LABELS, ORIENTATION_COLORS, ORIENTATION_LABELS,
    RD_CATEGORY_COLORS, RD_CATEGORY_LABELS,
    REFORM_PANEL, STAGE_LABELS, STATUS_LABELS,
    SUBTHEME_COLORS, SUBTHEME_LABELS, SUBTHEME_SHORT,
    budget_available, get_app_password, load_budget, load_reform_panel, load_reforms,
    load_reform_mentions, load_reform_panel_subtheme,
    reforms_available,
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
body, p, li, td, th, span, div {
    font-family: "Source Sans Pro", "Helvetica Neue", Arial, sans-serif !important;
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
    "Direct R&D": NAVY,
    "Innovation": GREEN,
    "Ambiguous": ORANGE,
    "Exclude": GREY,
}

PLOTLY_BASE = dict(
    template="plotly_white",
    font=dict(family="Source Sans Pro, Helvetica Neue, Arial", size=11.5, color=TEXT),
    plot_bgcolor="#ffffff",
    paper_bgcolor="#ffffff",
)


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
            # Country selector (currently Denmark only; ready for multi-country)
            _bud_ctry_opts = sorted(_db["country"].dropna().unique()) \
                if "country" in _db.columns else ["Denmark"]
            sel_bud_ctry = st.multiselect(
                "Country", _bud_ctry_opts, default=_bud_ctry_opts, key="bud_ctry",
            )
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
    if reforms_available():
        _dr = load_reforms()
        _ctry = sorted(_dr["country_name"].dropna().unique())
        sel_ctry = st.multiselect("Country", _ctry, default=_ctry, key="ctry")

        _st_opts = sorted(_dr["sub_theme"].dropna().unique()) if "sub_theme" in _dr.columns else []
        sel_st = st.multiselect(
            "Innovation type", _st_opts, default=_st_opts, key="st_filt",
            format_func=lambda x: SUBTHEME_LABELS.get(x, x),
        )
        _stat_opts = sorted(_dr["status"].dropna().unique()) if "status" in _dr.columns else []
        sel_stat = st.multiselect(
            "Status", _stat_opts,
            default=_stat_opts,
            key="stat_filt",
            format_func=lambda x: STATUS_LABELS.get(x, x),
        )
        _svy_opts = sorted(_dr["survey_year"].dropna().astype(int).unique()) if "survey_year" in _dr.columns else []
        sel_svy = st.multiselect(
            "Survey year", _svy_opts,
            default=_svy_opts,
            key="svy_filt",
        )
        only_major = st.checkbox("Major reforms only", key="maj_filt")
        # Year range for charts: use display_year so pre-1990 survey-era reforms
        # still appear when implementation_year is missing.
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

        # Show which countries have data vs. just panel placeholders
        _panel_df_sb = load_reform_panel()
        if not _panel_df_sb.empty and "reform_count" in _panel_df_sb.columns:
            _countries_with_data = sorted(
                _panel_df_sb[_panel_df_sb["reform_count"] > 0]["country_code"].unique()
            )
            _total_panel = _panel_df_sb["country_code"].nunique()
            st.markdown(
                f'<div style="font-size:.67rem;color:#888;margin-top:.4rem;">'
                f'<b style="color:#555;">{len(_countries_with_data)}</b> of {_total_panel} panel countries have reform data.<br>'
                f'Add more: <code style="font-size:.65rem;">python main.py --reforms-country FRA</code></div>',
                unsafe_allow_html=True,
            )
    else:
        st.caption("No data — run `python main.py --reforms-only`")
        yr_r = (1995, 2025)

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
if reforms_available():
    _dr_hdr = load_reforms()
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
if reforms_available():
    _dr_all = load_reforms()
    dr_f = _dr_all.copy()
    if sel_ctry:  dr_f = dr_f[dr_f["country_name"].isin(sel_ctry)]
    if sel_st:    dr_f = dr_f[dr_f["sub_theme"].isin(sel_st)]
    if sel_stat:  dr_f = dr_f[dr_f["status"].isin(sel_stat)]
    if sel_svy and "survey_year" in dr_f.columns:
        dr_f = dr_f[dr_f["survey_year"].isin(sel_svy)]
    if only_major and "is_major_reform" in dr_f.columns:
        dr_f = dr_f[dr_f["is_major_reform"] == True]  # noqa: E712
    if "display_year" in dr_f.columns:
        dr_f = dr_f[
            dr_f["display_year"].isna() |
            ((dr_f["display_year"] >= yr_r[0]) & (dr_f["display_year"] <= yr_r[1]))
        ]
else:
    dr_f = pd.DataFrame()

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

    db = load_budget()
    m = (db["year"] >= yr_b[0]) & (db["year"] <= yr_b[1])
    if dec_b and "decision" in db.columns:
        m &= db["decision"].isin(dec_b)
    if cat_b != "All":
        m &= db["budget_category"] == cat_b
    if sel_bud_ctry and "country" in db.columns:
        m &= db["country"].isin(sel_bud_ctry)
    if "confidence" in db.columns:
        _conf = pd.to_numeric(db["confidence"], errors="coerce")
        m &= _conf.between(conf_b[0], conf_b[1], inclusive="both")
    db_f = db[m].copy()

    # ── KPI strip ──
    n_inc = int((db_f["decision"] == "include").sum()) if "decision" in db_f.columns else 0
    stat_row([
        (f"{len(db_f):,}",                                   "Budget lines"),
        (f"DKK {db_f['amount_local'].sum()/1e6:,.1f} M",     "Total R&D spend identified"),
        (f"{n_inc:,}",                                        "High-confidence (include)"),
        (f"{db_f['section_code'].nunique() if 'section_code' in db_f.columns else '—'}",
                                                              "Ministries"),
    ])

    # ── Chart 1: Stacked bar by year ──
    section_header("R&D-related budget by year and category")

    gcol  = "budget_category"
    glab  = "budget_category_label" if "budget_category_label" in db_f.columns else gcol
    yr_ct = db_f.groupby(["year", gcol])["amount_local"].sum().reset_index()
    yr_ct["DKK M"] = yr_ct["amount_local"] / 1e6
    yr_ct["label"] = yr_ct[gcol]

    fig1 = px.bar(
        yr_ct, x="year", y="DKK M", color="label",
        color_discrete_map={k: BUDGET_CATEGORY_COLORS.get(k, GREY)
                            for k in yr_ct["label"].dropna().unique()},
        barmode="stack",
        labels={"year": "Year", "DKK M": "DKK (millions)", "label": ""},
        text_auto=".0f",
    )
    fig1.update_traces(
        marker_line_width=0,
        textposition="inside",
        textfont=dict(size=9.5, color="white"),
    )
    apply_style(fig1, height=340, xtitle="Year", ytitle="DKK (millions)")
    st.plotly_chart(fig1, use_container_width=True)
    caption_note("Source: Danish Finance Bills (Finanslov) 1975–1984. "
                 "Taxonomy: J-Rule scoring against OECD search library. "
                 "Numbers shown in DKK millions.")

    # ── Chart 2 & 3 side by side ──
    col_a, col_b_ = st.columns(2)

    with col_a:
        section_header("Cumulative R&D spend by ministry (top 10)")
        if "ministry_display" in db_f.columns:
            top_min = (
                db_f.groupby("ministry_display")["amount_local"]
                .sum().sort_values(ascending=True).tail(10).reset_index()
            )
            top_min["DKK M"] = top_min["amount_local"] / 1e6
            top_min["pct"]   = 100 * top_min["DKK M"] / top_min["DKK M"].sum()

            fig2 = go.Figure(go.Bar(
                x=top_min["DKK M"],
                y=top_min["ministry_display"],
                orientation="h",
                marker_color=NAVY,
                marker_line_width=0,
                text=top_min["DKK M"].map(lambda x: f"{x:,.0f}"),
                textposition="outside",
                textfont=dict(size=9.5, color=TEXT),
            ))
            apply_style(fig2, height=380, xtitle="DKK (millions)", legend_bottom=False)
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
            cat_tot["DKK M"] = cat_tot["amount_local"] / 1e6
            cat_tot["label"] = cat_tot["budget_category"]
            cat_tot["pct"]   = 100 * cat_tot["DKK M"] / cat_tot["DKK M"].sum()

            fig3 = go.Figure(go.Bar(
                x=cat_tot["label"],
                y=cat_tot["DKK M"],
                marker_color=[BUDGET_CATEGORY_COLORS.get(c, GREY) for c in cat_tot["budget_category"]],
                marker_line_width=0,
                text=cat_tot["pct"].map(lambda x: f"{x:.1f}%"),
                textposition="outside",
                textfont=dict(size=10.5, color=TEXT),
            ))
            apply_style(fig3, height=380, ytitle="DKK (millions)", legend_bottom=False)
            fig3.update_layout(showlegend=False, xaxis=dict(showgrid=False))
            fig3.update_yaxes(range=[0, cat_tot["DKK M"].max() * 1.18])
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
        "year", "ministry_display", "budget_line_display",
        "amount_local", "budget_category", "confidence", "ai_decision", "ai_rationale", "source_file",
    ] if c in db_f.columns]
    _BUD_COL_LABELS = {
        "year": "Year", "ministry_display": "Ministry",
        "budget_line_display": "Description", "amount_local": "Amount (DKK)",
        "budget_category": "R&D category", "confidence": "Confidence",
        "ai_decision": "Decision", "ai_rationale": "Rationale",
        "source_file": "Source file",
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
    caption_note(f"{len(_tbl):,} rows  ·  DKK {_tbl['amount_local'].sum()/1e6:,.1f} M")
    render_table(
        _tbl[_BUD_DISP_COLS].sort_values("year"),
        col_labels=_BUD_COL_LABELS,
        num_cols=["amount_local", "confidence"],
        wide_cols=["budget_line_display", "ministry_display", "ai_rationale"],
    )
    st.download_button(
        "Download CSV",
        _tbl[_BUD_DISP_COLS].to_csv(index=False).encode("utf-8"),
        "budget_lines.csv", "text/csv", key="bud_dl",
    )


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2 — INNOVATION REFORMS
# ═════════════════════════════════════════════════════════════════════════════

with TAB_REFORMS:
    if not reforms_available():
        st.info("Run `python main.py --reforms-only --reforms-country DNK` to generate reform data.")
        st.stop()

    # ── KPI strip ──
    _dr_all_tab2 = load_reforms()
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
        caption_note(
            "Source: OECD Economic Surveys. Extraction: GPT-4o / Claude Sonnet. "
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
        imp_rat  = str(row.get("importance_rationale") or "")
        go_rat   = str(row.get("growth_orientation_rationale") or "")
        mentions = row.get("n_mentions")
        mention_yrs = str(row.get("all_seen_survey_years") or row.get("mention_survey_years") or "")

        major_badge = (
            f'<span style="display:inline-block;padding:1px 7px;border-radius:2px;'
            f'background:{NAVY};color:#fff;font-weight:700;font-size:.66rem;'
            f'letter-spacing:.04em;">MAJOR</span>'
            if major else
            f'<span style="display:inline-block;padding:1px 7px;border-radius:2px;'
            f'background:#F1F3F6;color:#666;border:1px solid #D7DCE3;'
            f'font-weight:700;font-size:.66rem;letter-spacing:.04em;">NOT MAJOR</span>'
        )
        quote_block = (
            f'<div style="margin:.5rem 0 .3rem;padding:.35rem .75rem;'
            f'border-left:3px solid {lbl_clr};color:#555;font-size:.79rem;font-style:italic;">'
            f'&ldquo;{_html2.escape(quote_s[:300])}{"…" if len(quote_s) > 300 else ""}&rdquo;</div>'
            if quote_s else ""
        )
        imp_rat_block = (
            f'<div style="font-size:.7rem;color:#888;margin-top:.25rem;">'
            f'<b>Importance:</b> {_html2.escape(imp_rat)}</div>'
            if imp_rat else ""
        )
        go_rat_block = (
            f'<div style="font-size:.7rem;color:#888;margin-top:.15rem;">'
            f'<b>Growth mechanism:</b> {_html2.escape(go_rat)}</div>'
            if go_rat else ""
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

        cards_html += f"""
        <div style="border:1px solid {BORDER};border-left:4px solid {ctry_color};
                    border-radius:0 5px 5px 0;padding:.75rem 1rem;
                    margin-bottom:.55rem;background:#fff;">
          <!-- header row -->
          <div style="display:flex;align-items:center;gap:.45rem;flex-wrap:wrap;margin-bottom:.4rem;">
            <span style="font-size:.75rem;font-weight:700;color:{ctry_color};
                         background:{ctry_color}12;padding:2px 8px;border-radius:3px;
                         border:1px solid {ctry_color}40;">{country_s}</span>
            <span style="font-size:.75rem;font-weight:700;color:{NAVY};
                         background:#EEF3FB;padding:2px 8px;border-radius:3px;">{yr_s}</span>
            <span style="display:inline-block;padding:2px 9px;border-radius:2px;
                         background:{lbl_clr}15;color:{lbl_clr};border:1px solid {lbl_clr}40;
                         font-weight:700;font-size:.68rem;">{_html2.escape(sub_s)}</span>
            {major_badge}
            <span style="margin-left:auto;font-size:.71rem;color:#888;font-weight:600;">
              {_html2.escape(status_s)}</span>
          </div>
          <!-- body -->
          <div style="display:grid;grid-template-columns:1fr 155px;gap:.75rem;">
            <div>
              <div style="font-size:.84rem;font-weight:700;color:{TEXT};line-height:1.45;">
                {desc_s}
              </div>
              {quote_block}
              {imp_rat_block}
              {go_rat_block}
              {mentions_block}
            </div>
            <div style="font-size:.73rem;color:{TEXT};line-height:1.9;border-left:1px solid {BORDER};
                        padding-left:.75rem;">
              <span style="display:inline-block;padding:2px 8px;border-radius:2px;
                           background:{tag_col}15;color:{tag_col};border:1px solid {tag_col}40;
                           font-weight:700;font-size:.67rem;">{_html2.escape(tag_txt)}</span><br>
              <span style="color:#888;font-size:.69rem;font-weight:700;text-transform:uppercase;
                           letter-spacing:.04em;">Actor</span><br>
              <span style="color:{TEXT};">{_html2.escape(actor_s)}</span><br>
              <span style="color:#888;font-size:.69rem;font-weight:700;text-transform:uppercase;
                           letter-spacing:.04em;">Stage</span><br>
              <span style="color:{TEXT};">{_html2.escape(stage_s)}</span><br>
              <span style="color:#888;font-size:.69rem;font-weight:700;text-transform:uppercase;
                           letter-spacing:.04em;">Importance</span><br>
              <span style="color:{TEXT};">{_html2.escape(imp_s)}</span>
            </div>
          </div>
        </div>
        """

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
    _REF_DISP_COLS = [c for c in [
        "country_name", "survey_year", "first_seen_survey_year", "last_seen_survey_year",
        "all_seen_survey_years", "implementation_year", "sub_theme_label",
        "orientation_label", "status_label", "is_major_reform",
        "importance_bucket", "rd_actor_label", "rd_stage_label",
        "package_name", "description",
    ] if c in dr_f.columns]
    _REF_COL_LABELS = {
        "country_name": "Country", "survey_year": "Anchor survey",
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
    section_header("Survey coverage — mentions per survey")
    _mentions = load_reform_mentions()
    if not _mentions.empty and "survey_year" in _mentions.columns:
        _multi_surv = "country_code" in _mentions.columns and _mentions["country_code"].nunique() > 1

        if _multi_surv:
            # Stacked bar: year × country
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
                labels={"survey_year": "Survey year", "n": "Mentions extracted", "country_code": ""},
            )
            fig_surv.update_traces(marker_line_width=0)
            apply_style(fig_surv, height=240, xtitle="Survey year", ytitle="Mentions extracted")
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
            apply_style(fig_surv, height=240,
                        xtitle="Survey year", ytitle="Mentions extracted",
                        legend_bottom=False)
            fig_surv.update_layout(showlegend=False, xaxis=dict(showgrid=False))
            fig_surv.update_yaxes(range=[0, surv_cnt["n"].max() * 1.2])

        st.plotly_chart(fig_surv, use_container_width=True)
        _surv_n = _mentions["survey_year"].nunique()
        _ctry_n = _mentions["country_code"].nunique() if "country_code" in _mentions.columns else 1
        caption_note(
            f"{len(_mentions):,} raw mentions across {_surv_n} surveys "
            f"({_ctry_n} {'country' if _ctry_n == 1 else 'countries'}) "
            f"→ {len(dr_f):,} deduplicated events after cross-survey deduplication."
        )



# ═════════════════════════════════════════════════════════════════════════════
# TAB 3 — COMBINED VIEW
# ═════════════════════════════════════════════════════════════════════════════

with TAB_COMBINED:
    bk = budget_available()
    rk = reforms_available()

    if not bk and not rk:
        st.info("Run both pipelines to see the combined view.")
        st.stop()

    # ── Stream comparison ──
    section_header("Stream comparison")
    # Build dynamic time-range labels from actual data
    if bk:
        _db_cmp = load_budget()
        _bud_yrs_cmp = sorted(_db_cmp["year"].dropna().unique())
        _bud_ctry_cmp = sorted(_db_cmp["country"].dropna().unique()) if "country" in _db_cmp.columns else ["Denmark"]
        _s1_range = f"{min(_bud_yrs_cmp)}&#8211;{max(_bud_yrs_cmp)} ({', '.join(_bud_ctry_cmp)})" if _bud_yrs_cmp else "n/a"
    else:
        _s1_range = "n/a"
    if rk:
        _dr_cmp = load_reforms()
        _surv_yrs_cmp = sorted(_dr_cmp["survey_year"].dropna().astype(int).unique()) if "survey_year" in _dr_cmp.columns else []
        _ref_ctry_cmp = sorted(_dr_cmp["country_name"].dropna().unique()) if "country_name" in _dr_cmp.columns else []
        _s2_range = (
            f"{min(_surv_yrs_cmp)}&#8211;present ({', '.join(_ref_ctry_cmp[:3])}{'…' if len(_ref_ctry_cmp) > 3 else ''})"
            if _surv_yrs_cmp else "n/a"
        )
    else:
        _s2_range = "n/a"
    _cmp_rows = [
        ("Measures",     "DKK amount budgeted for R&amp;D",           "Innovation policy reforms enacted"),
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
        _db3 = load_budget()
        b_yr3 = _db3.groupby("year")["amount_local"].sum().reset_index()
        b_yr3["DKK M"] = b_yr3["amount_local"] / 1e6
        df_i3 = dr_f.dropna(subset=["display_year"]).copy()
        df_i3["yr"] = df_i3["display_year"].astype(int)
        rc3 = df_i3.groupby("yr").size().reset_index(name="n")
        fig_dual = go.Figure()
        fig_dual.add_trace(go.Bar(
            x=b_yr3["year"], y=b_yr3["DKK M"],
            name="R&D budget (DKK M)",
            marker_color=NAVY, opacity=0.72, marker_line_width=0, yaxis="y1",
        ))
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
            yaxis=dict(title="R&D Budget (DKK millions)", side="left",
                       gridcolor="#EBEBEB", linecolor=BORDER),
            yaxis2=dict(title="Reform event count", side="right",
                        overlaying="y", showgrid=False, linecolor=BORDER),
            legend=dict(orientation="h", y=1.05, x=0, font=dict(size=10.5),
                        bgcolor="rgba(0,0,0,0)"),
            **PLOTLY_BASE,
            margin=dict(t=44, b=36, l=8, r=8),
        )
        st.plotly_chart(fig_dual, use_container_width=True)
        _b_span = f"{b_yr3['year'].min()}–{b_yr3['year'].max()}" if not b_yr3.empty else "n/a"
        _r_span = f"{rc3['yr'].min()}–{rc3['yr'].max()}" if not rc3.empty else "n/a"
        caption_note(
            f"Finance Bills: {_b_span}. OECD Survey reforms: {_r_span}. "
            "The two streams are complementary evidence of innovation policy effort."
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
    if REFORM_PANEL.exists():
        panel_df = load_reform_panel()
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
        _db3 = load_budget()
        if not _db3.empty:
            b_yr3 = _db3.groupby("year")["amount_local"].sum().reset_index()
            b_yr3["DKK M"] = b_yr3["amount_local"] / 1e6
            fig_b3 = px.bar(
                b_yr3, x="year", y="DKK M",
                labels={"year": "Year", "DKK M": "DKK (millions)"},
                color_discrete_sequence=[NAVY],
            )
            fig_b3.update_traces(marker_color=NAVY, marker_line_width=0)
            apply_style(fig_b3, height=240, xtitle="Year", ytitle="DKK (millions)")
            st.plotly_chart(fig_b3, use_container_width=True)
            caption_note("Finance Bills (Finanslov) 1975–1984. High-confidence R&D lines only.")

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
        "amount_local": "Amount (DKK)", "currency": "Currency",
        "budget_category": "R&D category", "confidence": "Confidence",
        "ai_decision": "Decision", "ai_rationale": "Rationale",
        "source_file": "Source", "page_number": "Page",
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
            if "confidence" in _db5.columns:
                _conf5 = pd.to_numeric(_db5["confidence"], errors="coerce")
                m5 &= _conf5.between(conf_b[0], conf_b[1], inclusive="both")
            df5 = _db5[m5]
            cols5 = [c for c in _T5_BUD_LABELS if c in df5.columns]
            _df5_disp = df5[cols5].copy()
            caption_note(f"{len(df5):,} rows  ·  DKK {df5['amount_local'].sum()/1e6:,.1f} M")
            render_table(_df5_disp.sort_values(["year","section_code"] if "section_code" in cols5 else ["year"]),
                         col_labels=_T5_BUD_LABELS,
                         num_cols=["amount_local","confidence","page_number"],
                         wide_cols=["budget_line_display","ministry_display","ai_rationale"])
            st.download_button("Download (CSV)", df5[cols5].to_csv(index=False).encode(),
                               "budget_lines.csv", "text/csv")
    else:
        if not reforms_available():
            st.info("No reform data.")
        else:
            _dr5 = _dr_all if reforms_available() else pd.DataFrame()
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
producing a time series of DKK amounts classified by R&D category and Ministry.

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
