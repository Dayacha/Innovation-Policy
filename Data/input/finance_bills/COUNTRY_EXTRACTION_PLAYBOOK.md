# Country Extraction Playbook

This is the operational checklist for bringing a new budget country into the
pipeline, based on what worked for Canada and the other heavily-audited cases.

## Goal

Run the expensive extraction once, capture as much usable raw material as
possible, and avoid avoidable reruns by doing the prep work up front.

## Canada Lessons To Reuse

Canada became reliable because the pipeline combined:

1. Strong country context
- Good `config.py` / `country_profiles.py` priors
- Explicit known agencies
- Clear unit rules
- Clear document-type guidance

2. A country cleaner that removed predictable noise
- bilingual duplicate rows
- section totals / bare totals
- generic grant buckets
- obvious non-R&D bodies

3. Compile-side safety nets
- canonical aliases for historical / bilingual names
- within-file sibling outlier trimming
- selective verified overrides when the original source proved the amount

4. Cheap post-extraction recovery
- `python -m budget.pipeline --targeted-recovery-only --countries Canada`
- compile-side ingestion of targeted recovery rows

5. Gaps only after base compile was already good
- `--fill-gaps`
- `gap_review`
- `gap_review_apply`

The key principle: do not run `fill-gaps` or `gap_review` on top of a noisy
base. First make the country compile defensible.

## Files To Prepare For A New Country

For a first serious extraction, check or create:

1. `budget/config.py`
- `COUNTRY_CONTEXT` entry
- currency
- currency symbol
- language
- explicit `unit_hint`
- `known_agencies`
- `mixed_ministries`
- `doc_type_hint`

2. `budget/country_profiles.py`
- `skip_if`
- `include_note`
- `year_notes`
- note major structural breaks by era

3. `budget/cleaners/<country>.py`
- start conservative
- demote obvious non-R&D
- tag section totals
- flag unit outliers
- add `cleaning_notes`

4. `budget/canonical_series.py`
- add a short hardcoded canonical seed list
- do not overfit on day 1

5. `Data/input/finance_bills/<Country>/SOURCE_NOTES.md`
- source family
- year coverage
- missing years
- multi-volume years
- likely R&D actors
- likely false positives
- unit / number-format notes

## Pre-Extraction Audit

Before the overnight run, manually inspect 3-5 source files:

- earliest year
- mid-period year
- recent year
- one visually messy year
- one multi-volume year, if applicable

Confirm:
- actual unit used in the tables
- currency era breaks
- whether the source is line-item friendly or mostly narrative
- whether R&D appears as:
  - agencies
  - programmes
  - ministry chapter totals
  - grants / subsidies

## Overnight Extraction Strategy

For the first expensive run:

```bash
python -m budget.pipeline --countries <CountryA> <CountryB> ... --no-panel
```

Use `--dry-run` first if needed:

```bash
python -m budget.pipeline --countries <CountryA> <CountryB> ... --dry-run --no-panel
```

Do not run `fill-gaps`, `gap_review`, or `targeted-recovery-only` in the same
overnight pass unless the country already had a previously-audited compile.

## Morning-After Sequence

Run only the cheap follow-up:

```bash
python -m budget.pipeline --postprocess-only
python main.py --budget --country <CountryA>
python main.py --budget --country <CountryB>
python main.py --budget --build-database
```

Then inspect:

- `Data/output/budget/results.csv`
- `Data/output/budget/<Country>/<country>_docx_results.csv`
- `Data/output/budget/<Country>/<country>_docx_series.csv`
- `Data/output/budget/<Country>/<country>_gap_report.csv`
- `Data/output/budget/rd_database.csv`

## When To Use Targeted Recovery

Use targeted recovery only after you know the base extraction missed named rows
that are actually present in the original files:

```bash
python -m budget.pipeline --targeted-recovery-only --countries <Country>
python -m budget.pipeline --postprocess-only
python main.py --budget --country <Country>
python main.py --budget --build-database
```

If only a specific file is bad:

```bash
python -m budget.pipeline --targeted-recovery-only --countries <Country> --years <YYYY> --source-file "<filename substring>"
```

## When To Use Fill-Gaps

Only after:
- units are sane
- canonical mapping is sane
- outliers are mostly explained

Then:

```bash
python main.py --budget --country <Country> --fill-gaps
python -m budget.gap_review --country <Country>
python -m budget.gap_review_apply --country <Country>
python main.py --budget --build-database
```

## Signs A Country Is Not Ready For Fill-Gaps

- 1000x unit mistakes still visible
- same amount copied across many institutions from one summary page
- too many canonicals
- source is mainly speeches / narrative text rather than budgets
- zero non-null series rows despite rich raw extraction

## First-Pass Deliverable

A country is ready for deeper iteration when:

- the expensive extraction has run once
- source notes exist
- config/profile/cleaner scaffolding is present
- the first compile produces non-trivial series rows
- the main errors are now understandable as:
  - unit bugs
  - alias splits
  - page-summary ghosts
  - wrong aggregation level

