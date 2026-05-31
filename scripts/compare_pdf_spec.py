from PyPDF2 import PdfReader
from pathlib import Path

pdf_path = Path("docs/plano_proj_SD-26_1 (2).pdf")
if not pdf_path.exists():
    print("PDF not found:", pdf_path)
    raise SystemExit(2)

reader = PdfReader(str(pdf_path))
text = []
for p in reader.pages:
    try:
        text.append(p.extract_text() or "")
    except Exception:
        text.append("")
pdf_text = "\n".join(text).lower()

spec_path = Path("docs/WIRE_SPEC.md")
if not spec_path.exists():
    print("WIRE_SPEC.md not found")
    raise SystemExit(2)
spec_text = spec_path.read_text(encoding="utf-8").lower()

# Phrases to check
checks = {
    "master_envelope_master": '"master":',
    "master_envelope_request_id": '"request_id":',
    "master_envelope_payload": '"payload":',
    "spec_master_keys_upper": "master",  # presence of 'MASTER' word
    "tcp_task_query": '"task": "query"',
    "tcp_user_field": '"user":',
    "tcp_status_ok": '"status": "ok"',
    "tcp_worker_uuid": '"worker_uuid":',
    "udp_election_election": '"election":',
    "udp_election_request_id": '"request_id":',
    "udp_election_payload": '"payload":',
    # Forbidden fields
    "forbid_auth_token": "auth_token",
    "forbid_task_id": "task_id",
    "forbid_workers": "workers",
    "forbid_result": "result",
}

report = {}
for k, phrase in checks.items():
    in_pdf = phrase in pdf_text
    in_spec = phrase in spec_text
    report[k] = {"in_pdf": in_pdf, "in_wire_spec": in_spec}

# Print concise report
print("PDF vs WIRE_SPEC.md comparison report:\n")
missing_in_pdf = []
missing_in_spec = []
for k, r in report.items():
    status = f"{k}: PDF={'YES' if r['in_pdf'] else 'NO '}, SPEC={'YES' if r['in_wire_spec'] else 'NO '}"
    print(status)
    if not r["in_pdf"]:
        missing_in_pdf.append(k)
    if not r["in_wire_spec"]:
        missing_in_spec.append(k)

print("\nSummary:")
if not missing_in_pdf:
    print("- All checked phrases present in PDF (approx).")
else:
    print("- Missing in PDF:", missing_in_pdf)
if not missing_in_spec:
    print("- All checked phrases present in WIRE_SPEC.md.")
else:
    print("- Missing in WIRE_SPEC.md:", missing_in_spec)

# If any critical mismatches (e.g., forbidden fields present in PDF), show context
print("\nDetail: show contexts for forbidden fields present in PDF:")
for f in ("forbid_auth_token", "forbid_task_id", "forbid_workers", "forbid_result"):
    phrase = checks[f]
    if phrase in pdf_text:
        i = pdf_text.find(phrase)
        start = max(0, i - 80)
        end = min(len(pdf_text), i + 80)
        print(f"\nFound '{phrase}' in PDF context:\n...{pdf_text[start:end]}...")

# Exit code 0 for success
print("\nDone.")
