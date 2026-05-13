# Lithuania + New Zealand runbook

## Lithuania

- Source family quality: mixed but usable.
- Expected output type: hybrid/programmatic, not a perfectly homogeneous institutional panel.
- Prep added:
  - `budget/config.py`
  - `budget/country_profiles.py`
  - `budget/cleaners/lithuania.py`
  - `budget/canonical_series.py`
  - `Data/input/finance_bills/Lithuania/SOURCE_NOTES.md`
  - `budget/pdf_reader.py` fallback that materially improves Lithuanian `.docx` extraction

### Recommended first pass

```bash
python -m budget.pipeline --countries Lithuania --no-panel
python -m budget.pipeline --postprocess-only
python main.py --budget --country Lithuania
```

## New Zealand

- Source family quality: good.
- Expected output type: hybrid but materially cleaner than Lithuania, with a strong modern science-fund era.
- Prep added:
  - `budget/country_profiles.py`
  - `budget/cleaners/new_zealand.py`
  - `budget/canonical_series.py`
  - `Data/input/finance_bills/New Zealand/SOURCE_NOTES.md`

### Recommended first pass

```bash
python -m budget.pipeline --countries \"New Zealand\" --no-panel
python -m budget.pipeline --postprocess-only
python main.py --budget --country \"New Zealand\"
```

## Practical note

- Lithuania benefited from a `.docx` fallback through `textutil`; without that fix, several audited files would have appeared empty.
- New Zealand's main risk is over-extracting generic `Development` lines.
