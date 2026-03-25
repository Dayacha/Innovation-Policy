"""
Innovation Policy Dashboard
============================
Streamlit app combining both pipeline outputs:

  Tab 1 — R&D Spending        (Stream 1: Finance Bill budget lines, 1975-1984)
  Tab 2 — Innovation Reforms  (Stream 2: OECD Economic Survey reform events)
  Tab 3 — Combined View       (dual-axis: spending trend + reform event markers)
  Tab 4 — About               (methodology and data sources)

Run with:
    streamlit run app/streamlit_app.py
"""

import sys
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.data_loader import (
    ACTOR_LABELS, ORIENTATION_COLORS, RD_CATEGORY_COLORS,
    STAGE_LABELS, STATUS_LABELS, SUBTHEME_COLORS, SUBTHEME_LABELS,
    budget_available, load_budget, load_reforms, reforms_available,
)

# ── Page config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Innovation Policy Dashboard",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Global CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
  .metric-card {
    background: #f8f9fa;
    border-left: 4px solid #1f77b4;
    padding: 0.8rem 1rem;
    border-radius: 0 6px 6px 0;
    margin-bottom: 0.5rem;
  }
  .metric-card .label { font-size: 0.78rem; color: #666; text-transform: uppercase; letter-spacing: 0.05em; }
  .metric-card .value { font-size: 1.6rem; font-weight: 700; color: #1a1a2e; }
  .section-header {
    font-size: 1.1rem; font-weight: 600; color: #1a1a2e;
    border-bottom: 2px solid #e9ecef; padding-bottom: 0.4rem; margin: 1.2rem 0 0.8rem;
  }
  .tag {
    display: inline-block; padding: 2px 8px; border-radius: 12px;
    font-size: 0.75rem; font-weight: 600; margin: 2px;
  }
  .tag-green  { background: #d4edda; color: #155724; }
  .tag-red    { background: #f8d7da; color: #721c24; }
  .tag-orange { background: #fff3cd; color: #856404; }
  .tag-grey   { background: #e2e3e5; color: #383d41; }
</style>
""", unsafe_allow_html=True)


def metric_card(label, value, color="#1f77b4"):
    st.markdown(f"""
    <div class="metric-card" style="border-left-color:{color}">
      <div class="label">{label}</div>
      <div class="value">{value}</div>
    </div>
    """, unsafe_allow_html=True)


# ── Header ───────────────────────────────────────────────────────────────────

st.markdown("""
# Innovation Policy Dashboard
**Two complementary windows into how governments invest in innovation**
""")
st.markdown("---")


# ── Tabs ─────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 R&D Spending",
    "🔬 Innovation Reforms",
    "🔗 Combined View",
    "ℹ️ About",
])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — R&D SPENDING (Stream 1)
# ════════════════════════════════════════════════════════════════════════════

with tab1:
    st.markdown("## R&D Spending from Finance Bills")
    st.caption("Stream 1 — Budget line items extracted from scanned Finance Bill PDFs using OCR + taxonomy scoring.")

    if not budget_available():
        st.warning("Budget data not found. Run `python main.py --budget-only` to generate it.")
        st.stop()

    df_b = load_budget()
    if df_b.empty:
        st.warning("Budget results file is empty.")
        st.stop()

    # ── Sidebar filters ──
    col_f1, col_f2, col_f3 = st.columns([2, 2, 2])
    with col_f1:
        years_b = sorted(df_b["year"].unique())
        sel_years = st.select_slider(
            "Year range", options=years_b,
            value=(min(years_b), max(years_b)),
        )
    with col_f2:
        cats = ["All"] + sorted(df_b["rd_category"].dropna().unique().tolist())
        sel_cat = st.selectbox("R&D category", cats)
    with col_f3:
        decisions = ["include + review", "include only"]
        sel_dec = st.selectbox("Decision filter", decisions)

    # Apply filters
    mask = (df_b["year"] >= sel_years[0]) & (df_b["year"] <= sel_years[1])
    if sel_cat != "All":
        mask &= df_b["rd_category"] == sel_cat
    if sel_dec == "include only":
        mask &= df_b["decision"] == "include"
    df_filtered = df_b[mask].copy()

    # ── Summary metrics ──
    st.markdown('<div class="section-header">Summary</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    total_dkk = df_filtered["amount_local"].sum()
    with c1:
        metric_card("Total budget lines", f"{len(df_filtered):,}")
    with c2:
        metric_card("Total amount (DKK)", f"{total_dkk/1e6:,.1f}M")
    with c3:
        metric_card("Years covered", f"{sel_years[0]}–{sel_years[1]}")
    with c4:
        n_sections = df_filtered["section_code"].nunique() if "section_code" in df_filtered.columns else "—"
        metric_card("Ministries", str(n_sections))

    # ── Chart 1: Total spending by year ──
    st.markdown('<div class="section-header">R&D Budget by Year</div>', unsafe_allow_html=True)

    group_col = "rd_category" if sel_cat == "All" else "section_code"
    label_map = RD_CATEGORY_COLORS if group_col == "rd_category" else {}

    yearly = (
        df_filtered.groupby(["year", group_col])["amount_local"]
        .sum()
        .reset_index()
    )
    yearly["amount_M"] = yearly["amount_local"] / 1e6
    yearly[group_col] = yearly[group_col].fillna("other")

    color_map = RD_CATEGORY_COLORS if group_col == "rd_category" else None

    fig_bar = px.bar(
        yearly, x="year", y="amount_M", color=group_col,
        color_discrete_map=color_map,
        labels={"amount_M": "DKK (millions)", "year": "Year", group_col: "Category"},
        title=f"R&D-related budget lines by year ({sel_dec})",
        barmode="stack",
        template="plotly_white",
    )
    fig_bar.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # ── Chart 2: Top ministries ──
    if "section_name_en" in df_filtered.columns:
        st.markdown('<div class="section-header">Top Ministries by R&D Budget</div>', unsafe_allow_html=True)
        top_sec = (
            df_filtered.groupby("section_name_en")["amount_local"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )
        top_sec["amount_M"] = top_sec["amount_local"] / 1e6
        fig_horiz = px.bar(
            top_sec, x="amount_M", y="section_name_en",
            orientation="h",
            labels={"amount_M": "DKK (millions)", "section_name_en": ""},
            title="Cumulative R&D spending by Ministry",
            template="plotly_white",
            color_discrete_sequence=["#1f77b4"],
        )
        fig_horiz.update_layout(yaxis=dict(autorange="reversed"), height=380)
        st.plotly_chart(fig_horiz, use_container_width=True)

    # ── Data table ──
    with st.expander("Show raw data table"):
        show_cols = [c for c in [
            "year", "section_code", "section_name_en",
            "program_description_en", "line_description_en",
            "amount_local", "currency", "rd_category", "decision", "taxonomy_score",
        ] if c in df_filtered.columns]
        st.dataframe(
            df_filtered[show_cols].sort_values("year"),
            use_container_width=True,
            height=350,
        )


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — INNOVATION REFORMS (Stream 2)
# ════════════════════════════════════════════════════════════════════════════

with tab2:
    st.markdown("## Innovation Policy Reforms")
    st.caption("Stream 2 — Reform events extracted from OECD Economic Survey PDFs using an LLM (GPT-4o / Claude).")

    if not reforms_available():
        st.warning("Reform data not found. Run `python main.py --reforms-only` to generate it.")
        st.stop()

    df_r = load_reforms()
    if df_r.empty:
        st.warning("No reform events found. Run the reform pipeline first.")
        st.stop()

    # ── Filters ──
    col_f1, col_f2, col_f3, col_f4 = st.columns([2, 2, 2, 2])
    with col_f1:
        all_countries = sorted(df_r["country_name"].dropna().unique())
        sel_countries = st.multiselect("Country", all_countries, default=all_countries)
    with col_f2:
        all_subtypes = sorted(df_r["sub_theme"].dropna().unique()) if "sub_theme" in df_r.columns else []
        subtype_options = ["All"] + all_subtypes
        sel_subtype = st.multiselect(
            "Innovation type",
            all_subtypes,
            default=all_subtypes,
            format_func=lambda x: SUBTHEME_LABELS.get(x, x),
        )
    with col_f3:
        all_statuses = sorted(df_r["status"].dropna().unique()) if "status" in df_r.columns else []
        sel_status = st.multiselect(
            "Status", all_statuses,
            default=[s for s in all_statuses if s in ("implemented", "legislated")],
            format_func=lambda x: STATUS_LABELS.get(x, x),
        )
    with col_f4:
        only_major = st.checkbox("Major reforms only", value=False)

    # Apply filters
    df_rf = df_r.copy()
    if sel_countries:
        df_rf = df_rf[df_rf["country_name"].isin(sel_countries)]
    if sel_subtype:
        df_rf = df_rf[df_rf["sub_theme"].isin(sel_subtype)]
    if sel_status:
        df_rf = df_rf[df_rf["status"].isin(sel_status)]
    if only_major and "is_major_reform" in df_rf.columns:
        df_rf = df_rf[df_rf["is_major_reform"] == True]

    # ── Summary metrics ──
    st.markdown('<div class="section-header">Summary</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Reform events", str(len(df_rf)))
    with c2:
        n_major = int(df_rf["is_major_reform"].sum()) if "is_major_reform" in df_rf.columns else "—"
        metric_card("Major reforms", str(n_major), "#e74c3c")
    with c3:
        n_gs = int((df_rf["growth_orientation"] == "growth_supporting").sum()) if "growth_orientation" in df_rf.columns else "—"
        metric_card("Growth-supporting", str(n_gs), "#2ecc71")
    with c4:
        countries_n = df_rf["country_name"].nunique()
        metric_card("Countries", str(countries_n))

    # ── Chart 1: Bubble timeline ──
    st.markdown('<div class="section-header">Reform Timeline</div>', unsafe_allow_html=True)

    year_col = "implementation_year"
    if year_col in df_rf.columns and df_rf[year_col].notna().any():
        df_timeline = df_rf.dropna(subset=[year_col]).copy()
        df_timeline["year_int"] = df_timeline[year_col].astype(int)
        df_timeline["sub_theme_label"] = df_timeline["sub_theme"].map(
            lambda x: SUBTHEME_LABELS.get(x, x)
        )
        df_timeline["size"] = df_timeline["importance_bucket"].fillna(2).astype(float) * 10
        df_timeline["orientation_color"] = df_timeline["growth_orientation"].map(
            ORIENTATION_COLORS
        ).fillna("#95a5a6")
        df_timeline["status_label"] = df_timeline["status"].map(
            lambda x: STATUS_LABELS.get(x, x)
        )

        fig_timeline = go.Figure()
        for subtheme, color in SUBTHEME_COLORS.items():
            sub_df = df_timeline[df_timeline["sub_theme"] == subtheme]
            if sub_df.empty:
                continue
            fig_timeline.add_trace(go.Scatter(
                x=sub_df["year_int"],
                y=sub_df["sub_theme_label"],
                mode="markers",
                marker=dict(
                    size=sub_df["size"],
                    color=sub_df["orientation_color"],
                    line=dict(width=1, color="white"),
                    opacity=0.85,
                ),
                name=SUBTHEME_LABELS.get(subtheme, subtheme),
                text=sub_df.apply(
                    lambda r: (
                        f"<b>{r.get('package_name', '')}</b><br>"
                        f"{r.get('description', '')[:120]}...<br>"
                        f"Status: {STATUS_LABELS.get(r.get('status',''), r.get('status',''))}<br>"
                        f"Actor: {ACTOR_LABELS.get(r.get('rd_actor','unknown'), r.get('rd_actor',''))}<br>"
                        f"Stage: {STAGE_LABELS.get(r.get('rd_stage','unknown'), r.get('rd_stage',''))}<br>"
                        f"Growth: {r.get('growth_orientation','').replace('_',' ')}"
                    ),
                    axis=1,
                ),
                hovertemplate="%{text}<extra></extra>",
            ))

        fig_timeline.update_layout(
            title="Innovation reform events by type and year<br><sup>Bubble size = importance | Color = growth orientation (green=supporting, red=hindering)</sup>",
            xaxis_title="Year",
            yaxis_title="",
            template="plotly_white",
            height=480,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            showlegend=True,
        )
        # Add color legend annotation
        fig_timeline.add_annotation(
            text="● Growth-supporting  ● Growth-hindering  ● Mixed  ● Unclear",
            xref="paper", yref="paper", x=0, y=-0.1,
            font=dict(size=11, color="#666"),
            showarrow=False,
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("No implementation year data available for timeline chart.")

    # ── Chart 2: Sub-theme breakdown ──
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="section-header">Reforms by Type</div>', unsafe_allow_html=True)
        if "sub_theme" in df_rf.columns:
            st_counts = (
                df_rf["sub_theme"].value_counts().reset_index()
            )
            st_counts.columns = ["sub_theme", "count"]
            st_counts["label"] = st_counts["sub_theme"].map(
                lambda x: SUBTHEME_LABELS.get(x, x)
            )
            st_counts["color"] = st_counts["sub_theme"].map(SUBTHEME_COLORS)
            fig_pie = px.pie(
                st_counts, values="count", names="label",
                color="sub_theme",
                color_discrete_map=SUBTHEME_COLORS,
                template="plotly_white",
                hole=0.4,
            )
            fig_pie.update_traces(textposition="outside", textinfo="percent+label")
            fig_pie.update_layout(showlegend=False, height=360, margin=dict(t=20))
            st.plotly_chart(fig_pie, use_container_width=True)

    with col_b:
        st.markdown('<div class="section-header">R&D Actor × Stage</div>', unsafe_allow_html=True)
        if "rd_actor" in df_rf.columns and "rd_stage" in df_rf.columns:
            heatmap_df = (
                df_rf.groupby(["rd_actor", "rd_stage"])
                .size()
                .reset_index(name="count")
            )
            heatmap_df["actor_label"] = heatmap_df["rd_actor"].map(
                lambda x: ACTOR_LABELS.get(x, x)
            )
            heatmap_df["stage_label"] = heatmap_df["rd_stage"].map(
                lambda x: STAGE_LABELS.get(x, x)
            )
            fig_hm = px.density_heatmap(
                heatmap_df, x="stage_label", y="actor_label", z="count",
                color_continuous_scale="Blues",
                labels={"stage_label": "R&D Stage", "actor_label": "Actor", "count": "Reforms"},
                template="plotly_white",
            )
            fig_hm.update_layout(height=360, margin=dict(t=20),
                                  xaxis_tickangle=-20)
            st.plotly_chart(fig_hm, use_container_width=True)

    # ── Reform cards ──
    st.markdown('<div class="section-header">Reform Details</div>', unsafe_allow_html=True)
    df_display = df_rf.sort_values(
        ["implementation_year", "importance_bucket"],
        ascending=[False, False]
    ).head(50)

    for _, row in df_display.iterrows():
        major = row.get("is_major_reform", False)
        orient = row.get("growth_orientation", "unclear_or_neutral")
        color_cls = {
            "growth_supporting": "tag-green",
            "growth_hindering":  "tag-red",
            "mixed":             "tag-orange",
        }.get(orient, "tag-grey")

        st_label = SUBTHEME_LABELS.get(row.get("sub_theme", "other"), "—")
        status_label = STATUS_LABELS.get(row.get("status", ""), row.get("status", "—"))
        year_str = str(int(row["implementation_year"])) if pd.notna(row.get("implementation_year")) else "?"

        title = row.get("package_name") or row.get("description", "")[:80]
        star = "★ " if major else ""

        with st.expander(f"{star}{year_str} · {row.get('country_name','?')} · {st_label} — {title[:90]}"):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**{row.get('description', '')}**")
                if pd.notna(row.get("source_quote")):
                    st.markdown(f"> *\"{row['source_quote']}\"*")
                if pd.notna(row.get("importance_rationale")):
                    st.caption(f"**Importance:** {row['importance_rationale']}")
                if pd.notna(row.get("growth_orientation_rationale")):
                    st.caption(f"**Growth:** {row['growth_orientation_rationale']}")
            with col2:
                st.markdown(
                    f"<span class='tag {color_cls}'>{orient.replace('_',' ')}</span><br>"
                    f"<span class='tag tag-grey'>{status_label}</span><br>"
                    f"<span class='tag tag-grey'>Actor: {ACTOR_LABELS.get(row.get('rd_actor','unknown'), '?')}</span><br>"
                    f"<span class='tag tag-grey'>Stage: {STAGE_LABELS.get(row.get('rd_stage','unknown'), '?')}</span>",
                    unsafe_allow_html=True,
                )
                if "n_mentions" in row and pd.notna(row["n_mentions"]):
                    st.caption(f"Mentioned in {int(row['n_mentions'])} survey(s)")


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — COMBINED VIEW
# ════════════════════════════════════════════════════════════════════════════

with tab3:
    st.markdown("## Combined View: Spending × Policy Reforms")
    st.caption(
        "Stream 1 (budget lines, 1975–1984) and Stream 2 (reform events, 1995–present) "
        "are complementary evidence of innovation policy effort. This view overlays reform "
        "event markers onto the spending trend."
    )

    budget_ok  = budget_available()
    reforms_ok = reforms_available()

    if not budget_ok and not reforms_ok:
        st.warning("Neither pipeline has been run yet. Run `python main.py` to generate data.")
        st.stop()

    # ── Spending trend ──
    if budget_ok:
        df_b = load_budget()
        yearly_total = (
            df_b.groupby("year")["amount_local"].sum().reset_index()
        )
        yearly_total["amount_M"] = yearly_total["amount_local"] / 1e6

        fig_combined = go.Figure()
        fig_combined.add_trace(go.Bar(
            x=yearly_total["year"],
            y=yearly_total["amount_M"],
            name="R&D Budget (Finance Bills, DKK M)",
            marker_color="#1f77b4",
            opacity=0.7,
            yaxis="y1",
        ))

    # ── Reform event markers ──
    if reforms_ok:
        df_r2 = load_reforms()
        df_impl = df_r2.dropna(subset=["implementation_year"]).copy()
        df_impl["year_int"] = df_impl["implementation_year"].astype(int)

        # Count reforms per year
        reform_counts = df_impl.groupby("year_int").size().reset_index(name="n_reforms")

        if budget_ok:
            fig_combined.add_trace(go.Scatter(
                x=reform_counts["year_int"],
                y=reform_counts["n_reforms"],
                mode="lines+markers",
                name="Innovation Reform Events (count)",
                line=dict(color="#e74c3c", width=2.5),
                marker=dict(size=9, color="#e74c3c"),
                yaxis="y2",
            ))

            # Major reform stars
            major_df = df_impl[df_impl.get("is_major_reform", pd.Series(False, index=df_impl.index)) == True] \
                if "is_major_reform" in df_impl.columns else pd.DataFrame()
            if not major_df.empty:
                major_yr = major_df.groupby("year_int").size().reset_index(name="n")
                fig_combined.add_trace(go.Scatter(
                    x=major_yr["year_int"],
                    y=major_yr["n"],
                    mode="markers+text",
                    marker_symbol="star",
                    marker=dict(size=16, color="gold", line=dict(width=1, color="#333")),
                    text=major_yr["n"].astype(str),
                    textposition="top center",
                    name="Major reform events",
                    yaxis="y2",
                ))
        else:
            # Only reforms available
            fig_combined = px.bar(
                reform_counts, x="year_int", y="n_reforms",
                title="Innovation reform events by year",
                labels={"year_int": "Year", "n_reforms": "Reforms"},
                template="plotly_white",
            )

    if budget_ok:
        fig_combined.update_layout(
            title="R&D Budget (bars) vs. Innovation Reform Events (line)",
            xaxis=dict(title="Year", dtick=1),
            yaxis=dict(title="R&D Budget (DKK millions)", side="left"),
            yaxis2=dict(
                title="Reform event count",
                side="right",
                overlaying="y",
                showgrid=False,
            ),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            template="plotly_white",
            height=480,
            barmode="group",
        )
        st.plotly_chart(fig_combined, use_container_width=True)

    # ── Insight box ──
    st.markdown('<div class="section-header">How the streams connect</div>', unsafe_allow_html=True)
    st.markdown("""
    | Dimension | Stream 1 (Finance Bills) | Stream 2 (OECD Surveys) |
    |-----------|--------------------------|-------------------------|
    | **What it measures** | DKK amounts budgeted for R&D | Policy reforms enacted |
    | **Time coverage** | 1975–1984 (Denmark) | 1997–present (multi-country) |
    | **Granularity** | Budget line → Ministry | Reform event → sub-type |
    | **Method** | OCR + taxonomy scoring | LLM extraction (GPT-4o) |
    | **Key output** | `results.csv` | `reforms_events.csv` |

    Together, they enable a composite innovation policy indicator:
    - **Stream 1** captures the *intensity* of public R&D investment (how much)
    - **Stream 2** captures the *direction* of reform effort (what changed)
    - A country-year panel combining both can be used for cross-country econometric analysis
    """)

    # ── Per-country reform intensity (if multi-country) ──
    if reforms_ok:
        df_r3 = load_reforms()
        if df_r3["country_name"].nunique() > 1:
            st.markdown('<div class="section-header">Reform intensity by country (heatmap)</div>',
                        unsafe_allow_html=True)
            pivot = (
                df_r3.dropna(subset=["implementation_year"])
                .assign(year=lambda d: d["implementation_year"].astype(int))
                .groupby(["country_name", "year"])
                .size()
                .reset_index(name="n_reforms")
            )
            fig_heat = px.density_heatmap(
                pivot, x="year", y="country_name", z="n_reforms",
                color_continuous_scale="YlOrRd",
                labels={"year": "Year", "country_name": "Country", "n_reforms": "Reforms"},
                template="plotly_white",
            )
            fig_heat.update_layout(height=max(300, pivot["country_name"].nunique() * 35))
            st.plotly_chart(fig_heat, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 4 — ABOUT
# ════════════════════════════════════════════════════════════════════════════

with tab4:
    st.markdown("## About this Dashboard")

    col_l, col_r = st.columns([3, 2])
    with col_l:
        st.markdown("""
### Project overview

This dashboard is part of a research project measuring **innovation policy** across OECD
countries using two complementary data sources:

**Stream 1 — Finance Bill Extraction**
Scanned government Finance Bill PDFs are processed with OCR and scored against a
multilingual R&D taxonomy derived from Balazs's search library. Each budget line
item is classified as direct R&D, innovation support, institutional, or sectoral R&D.

**Stream 2 — OECD Economic Survey Extraction**
OECD Economic Survey PDFs are processed with an LLM (GPT-4o or Claude) that extracts
innovation policy reform events and classifies them along three dimensions:
- **Sub-type** — what kind of innovation policy (8 types)
- **R&D Actor** — who benefits (public / private / joint)
- **R&D Stage** — where in the pipeline (basic → applied → commercialisation → adoption)

### Innovation taxonomy

| Sub-type | Description |
|----------|-------------|
| `rd_funding` | Public R&D funding (budgets, grants, research councils) |
| `innovation_instruments` | R&D tax credits, direct grants, innovation agencies |
| `research_infrastructure` | Labs, science parks, HPC, data infrastructure |
| `knowledge_transfer` | TTOs, spinoffs, patents, university–industry collaboration |
| `startup_ecosystem` | Incubators, accelerators, venture capital, clusters |
| `human_capital` | Doctoral programmes, fellowships, researcher mobility |
| `sectoral_rd` | Health, climate, AI, energy, defence R&D |
| `other` | Innovation-relevant but does not fit the above |

### Data pipeline

```
Finance Bill PDFs         OECD Survey PDFs
      │                          │
      ▼                          ▼
 OCR + taxonomy         LLM (GPT-4o / Claude)
      │                          │
      ▼                          ▼
 results.csv             reforms_events.csv
      │                          │
      └──────────┬───────────────┘
                 ▼
         Combined indicator
         (future work)
```
        """)

    with col_r:
        st.markdown("""
### Technical details

- **Budget pipeline**: PyMuPDF + pytesseract OCR → J-Rule taxonomy scoring
- **Reform pipeline**: pdfplumber → chunked LLM extraction → within-survey dedup → cross-survey dedup → panel
- **LLM models**: GPT-4o (default) or Claude Sonnet
- **Languages**: Danish Finance Bills (1975–1984); OECD Surveys in English
- **Taxonomy**: Balazs's search library (`Data/input/taxonomy/search_library.json`)

### Running the pipelines

```bash
# Finance Bills (no API key needed)
python main.py --budget-only

# OECD Surveys (needs LLM key)
python main.py --reforms-only --reforms-country DNK

# Rebuild panel without re-running LLM
python main.py --reforms-build-panel-only

# Launch this dashboard
streamlit run app/streamlit_app.py
```

### Output files

| File | Contents |
|------|----------|
| `Data/output/budget/results.csv` | Budget line items |
| `Data/output/reforms/output/reforms_events.csv` | Reform events |
| `Data/output/reforms/output/reform_panel.csv` | Country×year panel |
        """)

        st.markdown("---")
        st.caption("Built with Streamlit · Plotly · pandas · pdfplumber · pytesseract · OpenAI / Anthropic")
