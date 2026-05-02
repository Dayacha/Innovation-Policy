# Runbook — Colombia, Costa Rica, Czech Republic

## Goal
Run the expensive first-stage LLM extraction for:
- `Colombia`
- `Costa Rica`
- `Czech Republic`

while capturing as much raw R&D budget material as possible for later compile-side cleaning.

## Pre-Run Status
- Country source notes created:
  - `Colombia/SOURCE_NOTES.md`
  - `Costa Rica/SOURCE_NOTES.md`
  - `Czech Republic/SOURCE_NOTES.md`
- These countries are higher-complexity than the earlier PDF-only batch.

## Country-Specific Risks

### Colombia
- Mixed `pdf + doc`
- Some `.doc` files are HTML-like text wrappers rather than classic Word binaries
- Some early laws omit annex detail from the published text
- Modern PDF years should still be useful

### Costa Rica
- Multi-volume years (`Tomo 1`, `Tomo 2`, etc.)
- One `.xls` file exists, but pipeline discovery ignores `.xls`
- Very large modern PDFs

### Czech Republic
- Mixed `pdf + docx`
- Annex files (`Přílohy`) are likely essential for real detail
- Some main law PDFs are highly aggregated
- At least one visible filename looks misfiled (`1993` pointing to a `2024` law filename)

## Recommended Expensive First-Stage Run

Dry run first:
```bash
python -m budget.pipeline --countries Colombia "Costa Rica" "Czech Republic" --dry-run --no-panel
```

If sane, overnight extraction:
```bash
python -m budget.pipeline --countries Colombia "Costa Rica" "Czech Republic" --no-panel
```

## Next-Morning Cheap Follow-Up
```bash
python -m budget.pipeline --postprocess-only
python main.py --budget --country Colombia
python main.py --budget --country "Costa Rica"
python main.py --budget --country "Czech Republic"
python main.py --budget --build-database
```

## What NOT To Do Immediately
- Do not run `targeted-recovery-only` yet
- Do not run `--fill-gaps` yet
- Do not run `gap_review` yet

First inspect:
- `results.csv`
- each country’s `<country>_docx_results.csv`
- early coverage by year
- whether the annex-heavy years in Czech Republic and the multi-volume years in Costa Rica are materially richer

## Audit Priority After First Compile
1. `Costa Rica`
   - check whether multi-volume years are undercaptured
2. `Czech Republic`
   - compare annex years vs main-law-only years
3. `Colombia`
   - inspect early years where the law text may omit the full annex

## Working Assumption
This batch is being run to maximize raw recall first, then clean later. For these 3 countries, that is the correct sequencing.
