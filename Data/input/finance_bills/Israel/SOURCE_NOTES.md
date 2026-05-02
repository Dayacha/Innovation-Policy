# Israel Source Notes

## Source Family
- Annual Israel budget PDFs by year
- Also contains `Comments.txt`, which may include source provenance notes
- Good first-pass candidate because the files are already normalized by year name

## File Coverage
- Current folder begins at `1975`
- Continuous yearly naming pattern in the visible files

## Language / Number Format
- Likely Hebrew originals, possibly OCR/transliterated output in the pipeline
- Confirm actual digit and separator conventions from the first files

## Likely R&D Actors
- Ministry of Science / science authority lines
- Innovation authority / industrial R&D support
- universities and national research institutes
- health, agriculture, and defence-adjacent research bodies

## Likely False Positives
- broad ministry totals
- defence and security spending unless explicitly research-labelled
- social programmes
- infrastructure and settlement-development funding

## First Audit Targets
- `1975_Israel.pdf`
- `1985_Israel.pdf`
- `1995_Israel.pdf`
- one 2000s file
- one recent file

## Extraction Notes
- Read `Comments.txt` before the run; it may already encode important caveats
- Confirm whether the budget is agency-friendly or mainly ministry-chapter style
- If OCR quality is weak, be ready to use targeted recovery after the first compile

