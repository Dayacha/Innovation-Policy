# Latvia runbook

## Prep added

- `budget/config.py`
- `budget/country_profiles.py`
- `budget/cleaners/latvia.py`
- `budget/canonical_series.py`
- `Data/input/finance_bills/Latvia/SOURCE_NOTES.md`
- `budget/pdf_reader.py` legacy `.doc` support

## Source-family assessment

- Latvia is suitable for a first large extraction.
- The best expected output is hybrid/programmatic:
  - science programme totals
  - science base funding
  - Latvian Science Council
  - Latvian Academy of Sciences
  - university science development / infrastructure lines

## Recommended first pass

```bash
python -m budget.pipeline --countries Latvia --no-panel
python -m budget.pipeline --postprocess-only
python main.py --budget --country Latvia
```

## Caveat

- Some years are short legal-wrapper documents and may legitimately produce sparse output.
