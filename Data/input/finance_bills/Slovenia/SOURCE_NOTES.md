# Slovenia Source Notes

## Review Scope
- Reviewed against `slovenia_gap_report.csv`, `slovenia_reextract_queue.csv`, `slovenia_docx_results.csv`, `slovenia_docx_audit.csv`, `slovenia_docx_series.csv`, `slovenia_verified_overrides.csv`, `slovenia_series_traceability.csv`, and `run_log.jsonl`.
- This note documents why Slovenia still shows unresolved gaps after the conservative source audit.

## Main Findings
- Most unresolved Slovenia gaps are not simple parser failures.
- The current final panel intentionally keeps only audited, institution-explicit, or programme-explicit rows that remain comparable across years.
- Many rows do get extracted for the missing years, but they are later dropped because they are:
  - zero totals
  - tiny sub-lines instead of the full programme total
  - generic support bundles rather than the defendable target series
  - SAZU support/activity sub-lines rather than an institution total
  - ARRS project/programme lines rather than the agency operating appropriation

## Series-Specific Conclusions
- `Programme 0502 — Znanstveno raziskovalna dejavnost`
  - `1995–2000` remain missing in the final panel.
  - The extractor finds research-related rows in several of these years, but they are zero totals or sub-lines rather than a defendable full `0502` annual total.
  - `2001+` is materially stronger and already represented in the final series.

- `Programme 0503 — Mladi raziskovalci / Človeški viri v podporo znanosti`
  - The long gap is mostly a comparability problem, not just an extraction problem.
  - Manual PDF verification now supports `2016–2018` via explicit `050302 Mladi raziskovalci, mobilnost in spodbude najboljšim raziskovalcem` rows, and those years have been promoted into the locked final series.
  - Pre-2019 source hits often resolve to generic `0503` support bundles, SAZU support activity, electronic-communications spillover, or implausible aggregates.
  - `2004 2005 u2013102.pdf` is misfiled and should not be treated as valid 2004/2005 budget evidence.
  - `2025` contains generic `0503` totals, but the file does not preserve a clearly defendable `050302` young-researchers line comparable with the retained modern series.

- `ARRS — Agencija za raziskovalno dejavnost Republike Slovenije`
  - `2010` remains unresolved because the extracted survivor is `CRP za ARRS`, not the audited `Delovanje ARRS/ARIS` operating appropriation used for the comparable ARRS series.
  - This year would require manual verification from the original budget table, not an automatic reclassification of the currently extracted row.

- `SAZU — Slovenska akademija znanosti in umetnosti`
  - `2011` has no defendable institution-total row in the current extracted evidence.
  - `2012` extracts only sub-lines such as donations, investments, and support activities; these are not equivalent to the retained institution-total SAZU series.

## Practical Interpretation
- Slovenia should be read as a conservative, traceable institutional R&D panel.
- The remaining gaps should not be interpreted as “the parser missed obvious usable rows” without checking the original PDFs.
- If the project later wants a higher-recall Slovenia panel, that should be a deliberate methodology change rather than an implicit relaxation of the current audit rules.
