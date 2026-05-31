from PyPDF2 import PdfReader
from pathlib import Path

pdf_path = Path("docs/plano_proj_SD-26_1 (2).pdf")
if not pdf_path.exists():
    print("PDF not found", pdf_path)
    raise SystemExit(2)
reader = PdfReader(str(pdf_path))
text = "\n".join([(p.extract_text() or "") for p in reader.pages])
low = text.lower()
terms = ["master", "status", "election", "workers", "result", "task_id", "task"]
for term in terms:
    print("\n----", term.upper(), "----")
    idx = 0
    found = 0
    while True:
        i = low.find(term, idx)
        if i == -1:
            break
        start = max(0, i - 140)
        end = min(len(low), i + 140)
        snippet = text[start:end].replace("\n", " ")
        print("...", snippet, "...")
        found += 1
        idx = i + 1
    if not found:
        print("None found")
print("\nDone")
