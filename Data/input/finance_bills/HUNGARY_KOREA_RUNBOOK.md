# Hungary + Korea runbook

## Hungary

- Source family quality: good enough for a first expensive run.
- Expected output type: mixed institutional + programme R&D rows.
- Prep now added:
  - `budget/config.py`
  - `budget/country_profiles.py`
  - `budget/cleaners/hungary.py`
  - `budget/canonical_series.py`
  - `Data/input/finance_bills/Hungary/SOURCE_NOTES.md`

### Recommended first pass

```bash
python -m budget.pipeline --countries Hungary --no-panel
python -m budget.pipeline --postprocess-only
python main.py --budget --country Hungary
```

## Korea

- Source family quality: weaker; mostly summary PDFs, not classic appropriation laws.
- Expected output type: conservative programme-level R&D signals, not a strong institutional panel.
- Prep now added:
  - `budget/config.py`
  - `budget/country_profiles.py`
  - `budget/cleaners/korea.py`
  - `budget/canonical_series.py`
  - `Data/input/finance_bills/Korea/SOURCE_NOTES.md`

### Recommended first pass

```bash
python -m budget.pipeline --countries Korea --no-panel
python -m budget.pipeline --postprocess-only
python main.py --budget --country Korea
```

## Important limitation

- The current pipeline still ignores `.hwp` files.
- So Korea first-pass coverage is limited to supported PDFs already in the folder.
