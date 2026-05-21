# QUALITY_NOTE

- Netherlands has a structural comparability break between the pre-2002 single-file `Miljoenennota` budgets and the 2002+ per-ministry `Rijksbegroting` files.
- For many years, `ministry8` exposes only the aggregate `OCW Art. 16 Onderzoek en wetenschapsbeleid` total. That total bundles NWO, KNAW, and related appropriations, so agency-level backfilling for NWO/KNAW would require an audited split that is not present in the current files.
- `ministry13` and `ministry14` both contain innovation-related aggregates, but they are not interchangeable institutional series. `EZ Art. 02` should come from the named EZ innovation block when present; LNV `Kennis en innovatie` should not be used as a silent fallback for EZ.
- Remaining gaps in NWO, KNAW, TNO, and older KNAW/NWO years are therefore mostly document-structure or comparability problems, not simple missing-file problems.
- Code fix applied in this review: for Netherlands, canonical selection now prefers surviving `include` rows over looser `review` fallbacks once plausibility filters have run. This restored valid rows such as `NWO 1984`, `RIVM 2001`, `KNMI 2021`, and the correct `EZ Art. 02` source in 2021-2025.
