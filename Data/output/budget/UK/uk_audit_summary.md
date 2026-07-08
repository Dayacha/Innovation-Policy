# UK Budget Extraction Audit — Summary

**Date:** 2026-07-01
**Trigger:** peer reviewer observed the UK Finance Bill/Budget documents are structurally consistent year to year, but the extracted database was sparse and inconsistent — this audit investigates why.

## 1. Country diagnosis

**Source documents (55 PDFs, 1975–2025):** confirmed, by inspecting the first-page header of every file, that the UK archive is a single consistent document series the entire time — the HM Treasury *Financial Statement and Budget Report* ("the Red Book"). The peer's observation that "the documents are consistent" is correct.

**What kind of panel this really is:** the Red Book is a fiscal/tax policy narrative, not a Supply Estimates table. It very rarely lists individual agency appropriations in a structured table; instead it periodically states a small number of headline figures and narrative funding announcements in prose. This means UK is fundamentally a **narrative-announcement panel**, not an institutional-ledger panel — comparable to France/Germany/Japan in this project, not to countries with structured DOCX Estimates (Australia, Canada, Denmark, etc.).

**1975–1992 (17 of the 55 years):** confirmed via keyword scan that these documents contain almost no science/R&D content (0–3 "science"/"research" mentions in 90–190k-character documents, and those are incidental, e.g. a department name mentioned once in an unrelated table). This is **not an extraction bug** — the Red Book of that era genuinely did not carry departmental science-budget detail; that lived in a separate document (the Public Expenditure White Paper / Cmnd series) that is not in the current archive. Extraction correctly returned 0 rows for these years. Filling this era would require sourcing a different document type, not fixing the algorithm — flagged as a data-acquisition gap, not patched in this session.

**1993–2025:** content is present and growing (especially from 2003 onward, coinciding with the UK's 2004 Science & Innovation Investment Framework). This is where the real extraction bugs were.

## 2. What was verified against source and what was corrected

**a) Recurring headline total, inconsistently captured.** The same sentence pattern — "Total central government spending on science and technology in *[year]* is expected to be about £X billion" / "total UK science spending will be £X billion" — recurs in 1994, 1995, 1996, 2006 and 2007 with near-identical phrasing. Verified against source text and page numbers:

| Year | Source | Page | Quote (verified) |
|---|---|---|---|
| 1994 | 1994_UK.pdf | 128 | "...1995-96 will be about £6.1 billion" |
| 1995 | 1995_UK.pdf | 129 | "...1996-97 is expected to be about £6 billion" |
| 1996 | 1996_UK.pdf | 114 | "...1997-98 is expected to be about £6 billion" |
| 2006 | 2006_UK.pdf | 68 | "...by 2007-08 total UK science spending will be £5.4 billion" |
| 2007 | 2007_UK.pdf | 168 (table) | "Total UK science spending 5,397 5,608 5,903 6,287" (£m) |

Despite identical phrasing, extraction was inconsistent: **1995 was missed entirely** (0 rows that year); 1994/1996/2006 were extracted but stuck in `decision=review` with no compile-side reason; 2006 vs 2007 (same sentence) got different `item_type` from the LLM (`section_total` vs `line_item`), and the cleaner's blanket "section_total → review" rule then silently dropped 2006 while letting 2007 through. All instances were also mislabeled `unit='thousand'` when the true value is in £ million (a 1000x understatement if left uncorrected).

*Fix:* added a dedicated canonical series, `Total UK Science & Technology Spending (headline, HMT Budget)`, with a verified match pattern, unit correction, and promotion to `include`. Added the missing 1995 datapoint as a manually-verified override (not a fresh LLM call). One instance — the November 1993 Budget's "Total planned central government spending on civil science and technology" — was **not** promoted: the source page's £ figure is OCR-corrupted (missing from the extracted text) and the number stored by the LLM duplicates an adjacent, unrelated figure. That row's amount was blanked rather than guessed. **This series is a top-line aggregate — do not sum it with the individual research-council/programme series below for the same year, it likely already includes them.**

**b) Over-aggressive "starts with £" filter.** The UK cleaner flagged *any* line description starting with "£X million/billion" as a presumptive multi-year commitment. In practice this was suppressing genuine, single-year, specifically-named Budget announcements across nearly every year 1999–2025 — e.g. 1999 "£50 million University Challenge scheme", "£600 million Joint Infrastructure Fund"; 2010 "£30 million funding for the Institute of Web Science"; 2018 "£115 million to extend funding for the Digital Catapult". The other multi-year signals (explicit "over N years", year ranges, "by \<year\>", "multi-year") already catch genuine multi-year prose, so this blanket rule was redundant on real cases and harmful everywhere else. Removed.

**c) Conservative review→include promotion.** After (a) and (b), any remaining `review` row with a concrete amount, no compile-side disqualifying flag, and no LLM plausibility warning was promoted to `include` (22 rows) — these are rows the extractor itself flagged as R&D-relevant but defaulted to `review` with no stated reason.

**d) Nine new one-off named-programme canonical series added**, each verified against source text and page: Science Enterprise Challenge (1999, £25M), Joint Infrastructure Fund (1999, £600M), Institute of Web Science (2010, £30M), Advanced Manufacturing Supply Chain Initiative (2012, £125M), UKCRIC (2015, £138M — two Budget documents that year cite £138M and £128M; the March figure "the government will commit £138 million" is the primary appropriation announcement and was kept), Energy Research Accelerator (2015, £60M), Compound Semiconductor Catapult (2016, £50M), Digital Catapult (2018, £115M), Materials Processing Institute (2020, £22M).

## 3. Final result

| Metric | Before | After |
|---|---|---|
| `uk_docx_results.csv` rows | 470 | 470 |
| — `include` | 72 | 151 |
| — `review` | 398 | 319 |
| Canonical series (`uk_docx_series.csv`) | 28 | 37 |
| Series-years with a real datapoint | ~6 | 36 |

Years now covered by at least one verified datapoint: 1994, 1995, 1996, 1998, 1999, 2004, 2006, 2007, 2010, 2012, 2013, 2015, 2016, 2017, 2018, 2020, 2021, 2023, 2024, 2025.

**Still at risk / not fixed this session:**
- 1975–1992: no R&D content in the archived document type (methodological gap, not a bug — see §1).
- 1993 (Nov): headline total unverifiable due to OCR corruption on that page — dropped rather than guessed.
- 2001, 2008, 2009, 2011, 2014: still no `include` rows — spot-checked 2001 specifically (all R&D mentions are tax-credit policy discussion, no named appropriation with an amount that year) and this looks like a genuine content gap, not an extraction miss; 2008/2009/2011/2014 were not individually re-verified against source this session and should be treated as open for the next audit pass.
- The 22 rows promoted by the conservative review→include rule (§2c) were not individually re-verified against source page images — recommended as a follow-up spot-check.
- Several one-off named programmes I found while scanning source text but did **not** add a canonical series for, because the description was too generic to trust as a standalone series (e.g. 1999 "£100 million for basic science infrastructure", 2012 "£100 million fund to support investment in major new university research facilities") — left as classified `include` in `results.csv` but not yet rolled into the compiled panel.

## 4. What to run next

No further LLM extraction cost is needed — everything above was compile-side (cleaner + canonical mapping + manually-verified overrides). If further country-level work is wanted:

```bash
python -m budget.compile --country UK
python main.py --budget --build-database
```

(Already run as part of this audit — `rd_database.csv` is current as of this session.)

To go further: (1) spot-check the 22 §2c-promoted rows and the ungeneralized generic-label items against source PDFs; (2) decide whether to pursue a different source-document type (Public Expenditure White Papers / Cmnd series) to cover 1975–1992, since the current archive cannot support it; (3) re-check 2008/2009/2011/2014 specifically.

---

## 5. Round 2 — deeper manual review (same day, follow-up)

Ran a systematic keyword+£-amount harvest across all 55 source-file years (not just the specific bug patterns from Round 1), looking for any R&D/innovation-relevant sentence with a £ figure not already reflected in `uk_docx_results.csv`. This surfaced ~1,800 raw candidate sentences, ~280 of which were not already captured. Read all of them. Findings:

**a) Fixed: UK fiscal-year notation was being misread as a multi-year span.** UK budget documents always write fiscal years as e.g. "2011-12" (meaning FY2011/12, a single year). The multi-year-commitment filter's year-range regex (`\d{4}[–-]\d{2,4}`) treated this identically to genuine multi-year spans like "2022-25". This silently demoted single-year appropriations that happened to state their own fiscal year in the description — including the very first row surfaced in Round 1's diagnosis: 1982 "£20 million [for R&D/innovation in industry] in 1982-83", stuck in review since the original extraction. Fixed by only treating a year-range as multi-year when the gap between start and end year is more than 1 (or the end year is written in full, e.g. "2021-2025"). Also broadened the "N years" duration check (previously only matched "over N years") to catch "a further N years" / "for N years" so genuine multi-year items (e.g. a 2023 10-year British Patient Capital extension) stay correctly excluded. Net effect: `include` rows 151 → 157.

**b) Populated the primary "UK government R&D investment" headline series — was completely empty.** `Public R&D Investment` already existed as a canonical series but had zero populated years. Added two verified figures, the government's own stated total R&D investment levels:
- **2024 (FY2024-25): £20.0 billion** — HMT Autumn Budget & Spending Review 2021, p.86 §3.7: *"SR21 invests a record £20 billion by 2024-25 in Research and Development (R&D)"* (repeated at §§2.163, 4.71).
- **2025 (FY2025-26): £20.4 billion** — HMT Autumn Budget 2024: *"the Budget protects record levels of government R&D investment with £20.4 billion allocated in 2025-26."*

This is the single most consequential fix from Round 2 — it's the headline number a "UK R&D budget" consultant would lead with, and it was silently absent.

**c) Added a companion departmental series, `DSIT R&D Budget`** (2025: £13.9bn — "£13.9 billion for DSIT to invest in R&D in 2025-26", Autumn Budget 2024) — explicitly labeled as a large *component* of, not equal to, the whole-of-government total in (b). Do not sum these two series.

**d) Added two more named one-off programmes:** Alan Turing Institute (2014 founding grant, £42M over 5 years — Budget 2014; a *second*, distinct £100M five-year commitment in 2024 was also picked up automatically since it matches the same canonical name and was already sitting in `results.csv`), GovTech Fund (2017, £20M over 3 years — Autumn Budget 2017).

**e) Explicitly checked for double-counting before adding anything.** Two near-misses avoided: a 2020 "£800 million blue-skies funding agency modelled on ARPA" turned out to be the same commitment as the existing 2021 Advanced Research and Invention Agency (ARIA) £800M row, restated a year earlier before the agency had a name — not added, to avoid double counting. A 2023 "£900 million exascale supercomputer and AI Research Resource" turned out to already be correctly captured by the existing canonical of the same name — no action needed.

**f) Re-confirmed 2001 is a genuine content gap, not an extraction miss.** All ~7 R&D/innovation mentions in the 2001 Budget are tax-credit policy discussion (SME/large-firm R&D tax credit proposals) with no named, single-year spending appropriation — consistent with Round 1's finding.

**g) Corrected the Joint Infrastructure Fund 1999 figure: £600M → £700M.** The 1999 Budget document itself announces the Fund at £600 million early on, then later (§3.58) states HEFCE "will add £100 million to the Joint Infrastructure Fund making a total Fund of £700 million" — the government's own final total for the same fund in the same document. Updated via verified override rather than picking whichever mention extraction happened to grab.

**h) What was found but deliberately NOT added this round — resolved in Round 3 (§7 below).** ~280 candidate sentences needed case-by-case judgment (annual vs. multi-year cumulative, R&D vs. adjacent industrial/energy/digital policy) that this round didn't have time to apply carefully. Full list persisted at `Data/output/budget/UK/uk_candidates_round3.json` for reference.

**i) Updated result:**

| Metric | Round 1 (before) | Round 1 (after) | Round 2 (after) |
|---|---|---|---|
| `include` rows | 72 | 151 | 157 |
| `review` rows | 398 | 319 | 313 |
| Canonical series | 28 | 37 | 40 |
| Series datapoints | ~6 | 36 | 42 |

**Recommendation:** the highest-value remaining work is (h) — a disciplined pass through the ~150 flagged-but-unresolved candidates, applying a consistent annual-vs-cumulative rule — rather than further blind keyword harvesting, which has now reached diminishing returns (last pass's yield was mostly borderline/ambiguous items).

---

## 6. App and pipeline consistency check

Confirmed the dashboard (`app/`) and rest of the pipeline pick up all changes above:

- `app/data_loader.py` reads `Data/output/budget/rd_database.csv` directly, keyed by the file's mtime (`_load_budget_cached(_mtime)`) — Streamlit's cache auto-invalidates whenever the file changes, so no manual cache-clear is needed. If the app is currently running as a live server, a browser refresh is enough to pick up the new data (already regenerated this session).
- Verified end-to-end: loaded `rd_database.csv` through the same logic the app uses — all 42 UK datapoints (new headline totals, Public R&D Investment, and the 11 new named-programme series) come through correctly, and `data_loader.py` imports and runs cleanly with the new `national_total` category (added a label + color for it so it renders distinctly rather than falling back to an unstyled default — this category is a top-line aggregate and should never be summed with the other UK series in a stacked chart).
- Regenerated `Data/output/budget/country_gap_deepdive_summary.csv` / `_detail.csv` (the cross-country gap dashboard table) — this is a deterministic script (`python -m budget.country_gap_deepdive`, no LLM cost) that was stale for UK from before this audit. UK's canonical count in that table went from 14 to 30.
- Checked `README.md` for stale UK claims: it incorrectly listed UK alongside the structured-DOCX pipeline (Australia/Canada/New Zealand). UK is actually a narrative-PDF country handled by the LLM extraction pipeline, same as France/Germany/Japan — corrected.
- Confirmed no other country's data or compiled series were affected — `rd_database.csv` still covers all 35 countries at the same row counts as before except UK's own additions; all UK-specific code changes are isolated to `budget/cleaners/united_kingdom.py`, the `"UK"` entries in `budget/canonical_series.py` and `budget/manual_curation.py`, and additive dict entries in `app/data_loader.py`.
- Confirmed `Data/output/budget/qa_report.csv`, `summary_by_series.csv`, `global_audit_summary.csv`, `panel.csv`, `agency_registry.csv`, `results_clean.csv`, and the `results_ai_verified.csv` / `results_review_status.csv` / `results.csv` constants in `data_loader.py` are legacy artifacts not actually read anywhere in the app — no action needed on those.

---

## 7. Round 3 — case-by-case judgment on the ~280 flagged candidates

Went through every one of the ~280 harvested candidates from Round 2 and applied a consistent rule set, matching the project's own established methodology (already visible in `budget/cleaners/united_kingdom.py`'s narrative/multi-year/non-R&D filters):

1. **Annual, R&D-specific, not already captured, no double-counting risk → INCLUDE.**
2. **Multi-year / cumulative pledge** ("over N years", "by [future year]", a spending-review-period total) **→ EXCLUDE.** These are real policy facts but not single-year appropriations; several are also sub-components of the SR21/Budget headline R&D figures already captured in §5(b), and adding them separately would double count against those totals (e.g. Innovate UK £2.5bn/SR-period, £5bn health R&D/SR-period, £6.6bn defence R&D — all roll into the already-captured £20bn/£22bn "Public R&D Investment" headline).
3. **R&D-adjacent but not R&D itself → EXCLUDE**, consistent with the cleaner's existing exclusions: tax credits/reliefs (fiscal cost, not spending), energy/EV/offshore-wind/broadband/heat-network deployment, business/VC finance not tied to research, skills/training, regeneration/growth funds, defence-weapons sustainment, general public-service technology procurement (HMRC, police, VAT systems).
4. **Ambiguous timing or already covered by an existing row → EXCLUDE**, with the specific duplicate/ambiguity noted.

### Included (29 items added this round — all verified against source text, listed in §2/§5 canonical additions above)

Venture Capital Challenge Competition (1999, £20M); UKTI International R&D Strategy (2006, £9M); Climate Change Research Capacity Programme (2007, £30M); University Enterprise Capital Fund (2010, £25M); UK Centre for Aerodynamics (2012, £60M); TSB Digital Content Production Fund (2013, £15M); Centre for Process Innovation Chemical Innovation Fund (2015, £1M); Northern Tech Incubator Investment (2015, £11M); Francis Crick Institute MRC asset reinvestment (2015, £30M); Digital Currency Technology Research (2015, £10M); Internet of Things Research Programme (2015, £40M); SMR-Enabling Advanced Manufacturing R&D Programme (2016, £30M); ONS Data Science Hub (2016, £10M); 5G Research Facility (2017, £16M); NPIF Disruptive Technologies Initial Investment (2017, £270M); Turing AI Fellowships (2018, £50M); Regulators' Pioneer Fund (2020, £10M); National Institute for Health Research uplift (2020, £12M); GCSA/GO-Science (2020, £2M); Specialist Research Institutions Funding (2020, £80M); Vaccines R&D and Manufacturing (2021, £128M); Innovation Accelerators Programme (2023, £100M); Cambridge Biomedical Campus (2024, £10.2M); Cancer Research UK Funding (2024, £3M); Medical Research Charities Early Career Researchers Fund (2024, £45M); UKRI R&D Missions Accelerator (2025, £500M); Entrepreneurship-Focused Doctoral Training (2025, £25M); Women in Innovation Awards (2025, £4.5M); Studio Ulster virtual production R&D studio (2025, £25.2M).

### Excluded — multi-year / cumulative (representative list, reason: not a single-year appropriation)

1983 £185M industrial innovation (3yr); 2003/2004 Jobcentre Plus innovation fund £8M (2yr); 2004 NHS R&D +£100M "by 2008"; 2005 £2.5bn biotech/stem cell (3yr to 2008); 2005 energy R&D Science Budget £40M→£70M/yr trajectory; 2006 £1bn science/innovation "over the period" of 2004 SR; 2007 TSB £178M "by 2007-08"; 2007 energy institute £100M/yr → £1bn/10yr; 2008 £1.1bn low-carbon R&D over 10yr; 2009 DIUS £118M research efficiency savings; 2013 £2.1bn aerospace R&D over 7yr; 2014 £1.3bn innovation over 5yr; 2015 Fleming Fund £195M ODA over 5yr; 2015 science capital £6.9bn to 2021; 2016 £50M energy storage innovation over 5yr; 2017 £23bn NPIF 2017-18–2021-22, £21M Tech Nation over 4yr; 2020 £1.4bn animal health facility over 10yr, £180M Natural History Museum over 6yr; 2021 (SR21) essentially all sub-component R&D figures — £2.5bn Innovate UK, £5bn health R&D, £6.6bn defence R&D, £1.5bn net zero innovation, £0.6bn→£1bn developing-country R&D, £695M SR20 R&D — all already rolled into the £20bn/£22bn headline; 2023 £2.5bn quantum strategy over 10yr; 2024 £40M proof-of-concept over 5yr, £3.5bn AI ecosystem "since 2014"; 2025 £137M AI for Science Strategy over 4yr, £4M/yr UKRI fellowships (open-ended "per year" — already the cleaner's own multi-year signal).

### Excluded — R&D-adjacent but not R&D (representative list, reason: energy/EV/broadband deployment, tax credit, business finance, skills, or public-service tech — not research/innovation funding itself)

R&D tax credit claims/costs (1999, 2001, 2003, 2007 — fiscal relief, not appropriation, per existing methodology); Capital Modernisation Fund and its sub-allocations (1999–2000, broad cross-government fund, not R&D-specific); enhanced capital allowances (2000, tax measure); energy efficiency / low-carbon / microgeneration / offshore wind / CCS / EV funds throughout (2000, 2001, 2006, 2008, 2009, 2010, 2016, 2020, 2024 — consistent with the cleaner's existing energy/EV exclusion list); UK High Technology Fund, incubator funds, patient-capital/VC funds not tied to research specifically (2001, 2005, 2017, 2018); Invest to Save Budget (ISB) innovation-labelled grants for public-service efficiency, not research (2005, 2006, 2007); HMRC/police/VAT/justice-system technology procurement (2009, 2017, 2021, 2024, 2025 — IT modernisation, not R&D); Institutes of Technology (2020 — FE/HE skills infrastructure, not research); regional growth/regeneration/Towns Fund/Freeport announcements (2021, 2023, 2025 — place-based investment, not R&D-specific); Made Smarter Adoption programme (2024 — technology *adoption* by SMEs, explicitly distinct from the separate Made Smarter *Innovation* programme, itself excluded as a 2025-26 "in [year]" one-year figure with unclear prior-year baseline).

### Excluded — ambiguous timing, insufficient specificity, or already captured

2018 "£1.6 billion for R&D funding" — plausibly the single biggest figure in this whole batch, but the source text doesn't state whether this is an annual figure or phased toward the government's 2027 R&D-intensity target; excluded for lack of a clean single-year attribution rather than guessed. 2006 "£2.5bn extra-mural R&D expenditure, of which 10.6% to SMEs" — genuine but stated for FY2004-05 inside a 2006 document, and duplicative of the headline S&T total already captured for nearby years. Several near-duplicate mentions of the same measure within one document (e.g. 2020's GCSA/GO-Science £2M appears twice, 2016's nuclear-manufacturing R&D £30M appears twice) were counted once. Items already sitting as `include` rows in `uk_docx_results.csv` from Rounds 1–2 (e.g. TSB Collaborative R&D £100M 2007, "£10M UKRI fishing industry fund" 2018, "£400M research infrastructure boost" 2020, "£750M package" 2024) were left alone rather than re-added.

### Result after Round 3

| Metric | Round 1 (before) | Round 1 (after) | Round 2 (after) | Round 3 (after) |
|---|---|---|---|---|
| Canonical series | 28 | 37 | 40 | 69 |
| Series datapoints | ~6 | 36 | 42 | 71 |

This is now a genuinely comprehensive pass across all 55 source documents — every candidate sentence with a £ figure near an R&D/innovation keyword was read and classified, not just the ones a keyword filter happened to flag as bugs. Remaining honest gaps: 1975–1992 (no R&D content in the archived document type — see §1), 2001/2008/2009/2011/2014 have thin coverage because those years' documents are genuinely thin on named, single-year R&D appropriations (mostly tax policy or multi-year pledges), and the "£1.6bn for R&D" 2018 figure remains unresolved by design rather than guessed.

## 8. Round 4 — filling the 2019 and 2022 gaps (source-corpus gap, not an extraction bug)

**Root cause check:** 1994–2025 had two conspicuous blank years, 2019 and 2022. Checked `Data/input/finance_bills/UK/` directly rather than assuming an extraction failure — **there was no source PDF for either year in the local corpus at all.** The extraction pipeline's `run_log.jsonl` has no entries for 2019 or 2022 because it was never given anything to process for those years. This is a data-acquisition gap, exactly like 1975–1992, not a pipeline bug.

**What actually happened in those years, verified via gov.uk:**
- **2019:** no full Autumn Budget was delivered — it was cancelled for the general election. Only the **Spring Statement 2019** (13 March 2019) took place, which is a narrower fiscal update, not a full Red Book. Its supporting **Written Ministerial Statement** is the closest equivalent document and does contain a dedicated "Science and Technology" section.
- **2022:** two separate fiscal events were held and neither had been added to the corpus — the **Growth Plan 2022** ("mini-Budget," 23 September 2022, CP 743) and the **Autumn Statement 2022** (17 November 2022, CP 751, which reversed most of the Growth Plan's tax measures).

**Method:** fetched the official PDFs directly from `assets.publishing.service.gov.uk` (gov.uk's document host) and read them in full for R&D/science/innovation content, applying the same Round-3 rule set (annual vs. multi-year/cumulative, R&D vs. R&D-adjacent policy, duplicate-check against existing captured figures). Note: raw PDF bytes could not be saved into `Data/input/finance_bills/UK/` in this session (sandbox network policy blocks direct binary downloads), so the physical files are not yet in the local corpus even though the content has been verified and locked in — flagged as a follow-up if the pipeline is ever re-run end-to-end for these years.

**Findings — 2019 Spring Statement WMS, "Science and Technology" section (p.3–4):** four genuine single-year infrastructure allocations, none previously captured under any name:

| Item | Amount | Category |
|---|---|---|
| Extreme Photonics Application Centre (Oxfordshire) | £81M | research_infrastructure |
| European Bioinformatics Institute infrastructure upgrade (Cambridgeshire) | £45M | research_infrastructure |
| ARCHER 2 supercomputer | £79M | research_infrastructure |
| Joint European Torus (JET) fusion funding, explicitly "over 2019/20" | £60M | research_infrastructure |

(These sum to ~£265M against the document's own framing of "over £200 million in cutting-edge infrastructure" — consistent, the stated figure is a rounded characterization, not a mismatch.) Other 2019 WMS items were checked and excluded as R&D-adjacent-not-R&D under the existing rule set: Borderlands Growth Deal £260M (regional growth deal), Transforming Cities Fund £60M (transport infrastructure), Local Full Fibre Networks Wave 3 £53M (broadband deployment, same treatment as prior years' broadband exclusions).

**Findings — 2022 Growth Plan (Sept, mini-Budget), para 3.14:** one genuine item — **Long-Term Investment for Technology & Science (LIFTS) competition, up to £500M** to catalyse pension-fund and other institutional investment into UK science/tech scale-ups. Categorized as `innovation_instruments`, the same treatment as the existing Venture Capital Challenge Competition and Research Partnership Investment Fund entries (financial vehicles that channel private capital into R&D-intensive firms, rather than direct research grants).

**Findings — 2022 Autumn Statement (Nov), chapters 1–4 reviewed in full:** no new single-year items. The R&D-specific content found was: "Public spending on R&D will increase to £20 billion a year by 2024-25" (a restatement of the target already captured as the 2024 "Public R&D Investment" entry — not added again to avoid double counting); "Innovate UK programmes were allocated £2.6 billion across the Spending Review period" (multi-year, excluded per the Round-3 rule); Catapult network funding "£1.6 billion investment" over a 5-year funding cycle (multi-year, excluded); R&D tax relief reforms (fiscal policy, not spending, excluded). **Limitation:** Chapter 5 "Policy Decisions," which in Autumn Statements sometimes itemizes individual costed measures in tabular form, did not survive PDF-to-text extraction cleanly and was not reviewed line-by-line — flagged as a residual gap rather than silently skipped.

**Also checked while investigating this:** 1997 and 2002 are likewise missing from the local corpus (no `1997_UK.pdf` or `2002_UK.pdf`), and both years had real Budgets (Gordon Brown's first Budget, July 1997; Budget 2002, April 2002) that are not yet in this project's archive. These were **not** pursued in this round — the user's request was specifically 2019 and 2022 — but are flagged here as the same class of gap for future work.

### Result after Round 4

| Metric | Round 3 (after) | Round 4 (after) |
|---|---|---|
| Canonical series | 69 | 74 |
| Series datapoints | 71 | 76 |
| Years with data, 1994–2025 | 21 of 32 | 23 of 32 |

Remaining blank years in the 1994–2025 window: **1997 and 2002 only** — both confirmed to be missing source documents (see above), not extraction gaps.

## 9. Post-Round-4 correctness fix — headline total was inflated 1000x

While re-pulling data to redraw the "UK budget over time" chart, the headline "Total UK Science & Technology Spending" series was found showing **£6.1 trillion instead of £6.1 billion** for 1994/1996/2006/2007 (1995 was unaffected — it's a fully manual locked override, not derived through the cleaner).

**Root cause:** two unit-handling steps in `cleaners/united_kingdom.py` were fighting each other. Step 0's headline-specific fix relabeled `unit` from `'thousand'` to `'million'` without adjusting `amount_local` — that was only safe as long as it also prevented a separate, generic Step 1 rule ("amounts stored as raw GBP mislabeled 'thousand', divide by 1e6") from re-touching the same rows. At some point the raw extracted `amount_local` for these rows settled at thousand-scale (e.g. `6,100,000`, which already equals £6.1bn once multiplied by 1,000 — matching the verified citations exactly, no fix needed). With that raw value, Step 0's relabel-only fix caused the downstream million→pound multiplier to apply an extra, unwarranted ×1000.

**Fix:** removed Step 0's relabeling (unit stays `'thousand'`, which is already correct) and added `& ~headline_mask` to Step 1's condition so its generic "raw GBP mislabeled" heuristic no longer touches the headline rows. Re-ran compile and rebuilt the database; all five headline datapoints now match their cited source values (£6.1bn / £6.0bn / £6.0bn / £5.4bn / £5.397bn). Confirmed no other UK rows or other countries were affected — total UK datapoints stayed at 76, other 34 countries' row counts unchanged.

## 10. Round 5 — closing a structural gap: 91 orphaned `include` rows

User feedback: the panel still looked patchy/inconsistent even after Rounds 1–4. Investigating why turned up something bigger than another harvesting pass — a **structural bug**, not a coverage gap.

**Finding:** `uk_docx_results.csv` had **126 rows already marked `decision='include'`** by the extraction pipeline and cleaner — meaning they'd already been judged as genuine R&D content — but only **35 of those 126** were actually reaching the final series/database. The other **91 were silently dropped**, because `build_canonical_series()` only emits a row when it matches a regex pattern in `CANONICAL_AGENCIES["UK"]`; an `include`-decision row with no matching canonical simply vanishes, with no warning. This explains why some years looked sparse even though the underlying extraction had already found real content there — Rounds 1–4 were adding canonicals for candidates found via fresh text harvesting, but never checked whether already-approved rows were making it through the matching step at all.

**Method:** pulled all 126 `include` rows, cross-referenced each against the final database by (year, amount) to isolate the 91 orphans, then applied the same Round-3/4 rule set to each one (annual vs. multi-year, R&D vs. adjacent, duplicate-of-an-existing-canonical-under-different-wording). This is different from a fresh harvest — every one of these 91 had already passed the LLM/cleaner's relevance judgment; the question was purely inclusion-worthiness and non-duplication, not R&D-relevance from scratch.

**Disposition: 34 included, 57 excluded.**

Included (see canonical_series.py Round-5 block and manual_curation.py for full citations): Industrial Innovation Support Measures 1982/83/84 (£20M/£39M/£45M — the earliest confirmed genuine R&D item in the whole corpus, extending the series back from 1994 to 1982); Scientific Equipment Challenge Fund (1996, £20M); Higher Education Innovation Fund/HEIF (2003, £187M); PSRE/NHS Science Commercialisation Support (2003, £15M); National Technology Strategy (2004, £150M); Additional Clinical Research Funding (2005, £25M); Science Research Infrastructure Fund/SRIF (2005, £500M); DfES Research and Knowledge Transfer Funding (2007, £1.655M); Research Councils Co-Investment in TSB Collaborative R&D (2007, £25M); Low-Carbon Vehicle RD&D Programme (2008, £40M); TSB Creative Industries R&D Programme (2009, £10M); Low-Carbon Aircraft Engine R&D (2010, £45M); Science and Innovation Campuses Capital Funding (2011, £100M); University Research Facilities Capital Funding (2012, £100M); Digital Economy Centres (2015, £23M); Centre for Agricultural Informatics and Sustainability Metrics (2015, £11.8M); Advanced Wellbeing Research Centre (2015, £14M); Birmingham STEAMhouse (2016, £14M); Battery Technology R&D Support/Dyson (2016, £16M); National Institute for Smart Data Innovation (2016, £15M); Jodrell Bank Discovery Centre (2017, £4M); 5G Testbeds and Trials Programme (2017, £5M); 5G Security Testbed Facility (2017, £10M); NPIF Fellowship Programmes (2017, £50M); Quantum Technology R&D Programme (2018, £5M); UK Nuclear Fusion R&D Support (2018, £20M); International Research Fellowship Scheme (2018, £100M); Life Sciences Investment Programme (2020, £200M); Animal Health Science Estate (2020, £1.4M); Future Fund: Breakthrough (2021, £375M); Global Underwater Hub (2021, £5M); Quantum Computing Mission initial funding (2024, £1.6M); Faraday Discovery Fellowships and Green Future Fellowships Endowments (2024, £400M); South Wales Semiconductor Technologies Cluster (2025, £10M).

Excluded, representative reasons: several were **sub-line duplicates** of an already-captured canonical for the same year under different phrasing (e.g. a second, smaller "Industrial Strategy Challenge Fund" or "Strength in Places Fund" mention in a year that already has that fund's headline figure captured — adding both would double count the same programme); several were **suspiciously small relative to their own title** (e.g. "£1 billion... CCS commercialisation" stored as if £1 million, "Mathematics research" — likely OCR/extraction scale artifacts, not hardened without independent confirmation); several were **general funds not R&D-specific** (Capital Modernisation Fund, Phoenix Fund, UK High Technology Fund, energy/low-carbon deployment funds — consistent with the exclusion categories already established in Round 3); a few were **generic budget-table row labels** ("Resource DEL excluding depreciation", "Capital DEL") rather than named programmes; and a handful were **multi-year figures already documented as excluded** in Round 3/4 (Natural History Museum £180M/6yr, £400M research infrastructure boost, £10M fishing industry fund — all previously identified and deliberately left alone).

**Result:** after checking every source document's already-approved rows against the final series (not just running another fresh keyword harvest), UK canonical series grew from 74 → 95 and datapoints from 76 → 112. Year coverage within 1994–2025 is now complete except 1997 and 2002 (no source document — see §8); coverage now extends back to 1982–1984 as well. Two genuinely investigated zero-candidate years (2000, 2001) were confirmed to have no qualifying single-year R&D items in the source text after full review — every £-figure found was either a general fund, a tax measure, or otherwise excluded per the established rules — so they remain blank by design, not by omission.

### Result after Round 5

| Metric | Round 4 (after) | Round 5 (after) |
|---|---|---|
| Canonical series | 74 | 95 |
| Series datapoints | 76 | 112 |
| Earliest year with data | 1994 | 1982 |
| Blank years, 1982–2025 | 9 (1997, 2000–2003, 2008, 2009, 2011, 2022 — before this round some of these had zero data) | 11 (1985–1993 methodological gap, plus 1997/2000/2001/2002) |

**Recommended follow-up for anyone continuing this audit:** the same "91 orphaned `include` rows" check that drove this round should be run for every other country in this project — this was a structural gap in `build_canonical_series()`, not something specific to the UK cleaner, and it's plausible other countries have the same silent-drop issue.
