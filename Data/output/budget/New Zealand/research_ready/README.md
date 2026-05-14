# New Zealand Research-Ready Budget Pack

This folder is generated from the curated New Zealand budget outputs and is intended for downstream research use.

## Files

- `new_zealand_research_panel_institutional.csv`
  - The conservative institutional panel.
  - Use this when you want document-traceable agency, vote, and fund observations.
- `new_zealand_research_panel_analytical.csv`
  - A separate annual rollup that improves longitudinal interpretability.
  - Use this for time-series plotting or macro trend analysis across eras.
- `new_zealand_research_yearbook.csv`
  - One row per year combining the analytical rollup and the sum of observed institutional rows.
- `new_zealand_research_series_catalog.csv`
  - Series-level metadata: first year, last year, role, dominant item type, and gap years.

## Recommended Use

- For institution-level research, start with `new_zealand_research_panel_institutional.csv`.
- For trend analysis across `1975-2025`, start with `new_zealand_research_panel_analytical.csv`.
- Treat `coverage_status = anchor` as the strongest annual evidence.
- Treat `coverage_status = broad_proxy` as a strong but constructed portfolio proxy.
- Treat `coverage_status = partial_proxy` as analytically useful but narrower than a full science-budget anchor.
- Treat `coverage_status = missing` as a true evidence gap, not zero spending.

## Current Counts

- Institutional rows: 118
- Institutional observed rows: 111
- Analytical annual rows: 51
- Analytical non-null years: 41
- Catalog series: 11
- Yearbook years: 51

## Rebuild

```bash
./venv/bin/python scripts/build_new_zealand_traceability.py
./venv/bin/python scripts/build_new_zealand_rollup.py
./venv/bin/python scripts/build_new_zealand_research_pack.py
```
