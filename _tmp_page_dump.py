import sys, pdfplumber

path = sys.argv[1]
pages = [int(x) for x in sys.argv[2:]]
with pdfplumber.open(path) as pdf:
    for i in pages:
        print(f"=== page {i} (of {len(pdf.pages)}) ===")
        print(pdf.pages[i].extract_text())
        print()
