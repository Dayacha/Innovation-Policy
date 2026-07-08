import sys, pdfplumber

path = sys.argv[1]
with pdfplumber.open(path) as pdf:
    print(f"pages: {len(pdf.pages)}")
    for i, page in enumerate(pdf.pages):
        t = page.extract_text() or ""
        print(f"--- page {i} ---")
        print(t[:300])
