# Slovenia Quality Note

## Bottom Line
- Slovenia is usable as a traceable R&D budget time series, but not as a complete ledger of all Slovenian public R&D spending.
- The current panel is intentionally conservative: it keeps only audited, document-traceable observations.
- Sparse coverage outside the core series is mostly a methodological choice, not just a parser limitation.

## Main Comparability Problem
- The source corpus changes structure across time:
  - early scanned Uradni list budget PDFs
  - `SIT` before 2007 and `EUR` from 2007 onward
  - multi-file and companion-file budget releases (`RS_...P001/P002/P003`)
  - some biennial budgets and legal-wrapper files that do not behave like clean numeric annexes
- A high-recall extraction therefore overstates comparability if every detected research-like row is treated as a clean annual institutional observation.

## Conservative Audit Rule
- Keep only observations that are explicit, traceable, and reasonably comparable as annual R&D appropriations.
- Prefer:
  - programme `0502` totals when they are explicit and auditable
  - named institutional rows for `SAZU`
  - explicit `ARRS` appropriations from 2004 onward
  - clearly separate modern series such as `European Space Agency Programs` and `Development of Research and Innovation Capacities`
- Exclude:
  - legal-wrapper text without recoverable appropriations
  - misfiled or duplicate budget documents
  - opportunistic discovery rows that look research-related but do not form a stable institutional series
  - sub-lines that appear to duplicate the broader audited science block

## Conservative Comparable Backbone
- `SAZU — Slovenska akademija znanosti in umetnosti`: strongest long-run institutional anchor, `1992–2025` with audited gaps still respected
- `Programme 0502 — Znanstveno raziskovalna dejavnost`: main long-run science appropriation backbone
- `ARRS — Agencija za raziskovalno dejavnost Republike Slovenije`: explicit from `2004` onward
- `Programme 0503 — Mladi raziskovalci / Človeški viri v podporo znanosti`: included only where a defendable annual line survives
- `European Space Agency Programs`: `2023–2025`
- `Development of Research and Innovation Capacities`: `2024–2025`

## Explicit Source Limitations
- `2014` in the current repo behaves like legal text rather than a usable budget annex and is treated as a genuine source limitation.
- `2004 2005 u2013102.pdf` is misfiled in the source set and is explicitly excluded from evidentiary use.
- The current final panel contains `87` verified positive observations and `32` genuine missing series-year combinations.

## Interpretation Guidance
- Read Slovenia as a conservative institutional R&D panel, not as a full map of all Slovenian science and innovation spending.
- Do not compare `SIT` and `EUR` years as if they were on the same nominal scale without explicit currency conversion.
- The modern series is substantially stronger than the raw discovery universe because many detected rows are programmes, projects, or duplicate sub-lines rather than stable institutions.
- If Slovenia still looks sparse relative to aggregate official R&D statistics, that reflects remaining coverage limits, not obvious overstatement in the audited panel.
