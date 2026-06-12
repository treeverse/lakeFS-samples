"""Multimodal extraction: read a receipt image/PDF and pull out structured fields.

A single vision call per file does double duty:
  - **classification** (``is_receipt``) — used by Phase-1 triage to drop non-receipts
  - **extraction** (vendor, date, invoice no, currency, line items, total)

Images are read with ordinary file I/O (the files live on the mounted lakeFS branch),
re-encoded to PNG to validate readability, and PDFs are rasterized with PyMuPDF.
A file that cannot be decoded returns ``None`` from :func:`load_image_png`, which
Phase-1 treats as a corrupt-file drop — no model call needed.
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import os

EXTRACTION_SYSTEM_PROMPT = (
    "You are a precise receipt and invoice data extractor. You are shown one image. "
    "Return ONLY a JSON object with exactly these keys:\n"
    '  "is_receipt": boolean — true only if the image is a purchase receipt or invoice,\n'
    '  "vendor": string — merchant/business name, or "" if unknown,\n'
    '  "date": string — the transaction date exactly as printed (any format), or "",\n'
    '  "invoice_no": string — invoice/receipt number, or "",\n'
    '  "currency": string — ISO 4217 code (e.g. USD, EUR), or "",\n'
    '  "line_items": array of objects {"name": string, "amount": number},\n'
    '  "total": number or null — the grand total.\n'
    "Use numeric values (not strings) for amounts and total. If the image is not a "
    "receipt/invoice (e.g. a photo), set is_receipt=false and leave the other fields empty/null. "
    "Do not guess values that are not visible."
)

REQUIRED_FIELDS = ("vendor", "date", "currency", "total")


def sha256_file(path: str) -> str:
    """Content hash of a file, for exact-duplicate detection."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def load_image_png(path: str) -> bytes | None:
    """Return PNG bytes for an image or the first page of a PDF; ``None`` if undecodable."""
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".pdf":
            import fitz  # PyMuPDF
            from PIL import Image

            doc = fitz.open(path)
            if doc.page_count == 0:
                return None
            pages = []
            for i in range(doc.page_count):
                pix = doc.load_page(i).get_pixmap(dpi=150)
                pages.append(Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB"))
            if len(pages) == 1:
                out = pages[0]
            else:  # stack pages vertically so a total on a later page is still read
                w = max(p.width for p in pages)
                out = Image.new("RGB", (w, sum(p.height for p in pages)), (255, 255, 255))
                y = 0
                for p in pages:
                    out.paste(p, (0, y))
                    y += p.height
            buf = io.BytesIO()
            out.save(buf, "PNG")
            return buf.getvalue()
        else:
            from PIL import Image

            # PIL natively decodes jpeg/png/webp/tiff/bmp/gif; unsupported files (txt,
            # zip, …) raise and become a triage "corrupt / unreadable" drop.
            with Image.open(path) as im:
                im.load()
                buf = io.BytesIO()
                im.convert("RGB").save(buf, "PNG")
                return buf.getvalue()
    except Exception:
        return None


def _coerce(raw: dict) -> dict:
    """Normalise a model response into the canonical record shape."""
    items = []
    for it in raw.get("line_items") or []:
        if isinstance(it, dict):
            items.append({"name": str(it.get("name", "")).strip(), "amount": it.get("amount")})
    return {
        "is_receipt": bool(raw.get("is_receipt", False)),
        "vendor": (raw.get("vendor") or "").strip(),
        "date": (raw.get("date") or "").strip(),
        "invoice_no": (raw.get("invoice_no") or "").strip(),
        "currency": (raw.get("currency") or "").strip().upper(),
        "line_items": items,
        "total": raw.get("total"),
    }


def missing_required(record: dict) -> list[str]:
    """Required fields that are empty/None (Phase-2 extraction completeness)."""
    miss = []
    for f in REQUIRED_FIELDS:
        v = record.get(f)
        if v is None or (isinstance(v, str) and not v.strip()):
            miss.append(f)
    return miss


def extract(png_bytes: bytes, *, model: str, client=None, api_key: str | None = None) -> dict:
    """Run the vision model on PNG bytes and return a canonical record dict."""
    if client is None:
        from openai import OpenAI

        client = OpenAI(api_key=api_key or os.environ.get("OPENAI_API_KEY"))

    b64 = base64.b64encode(png_bytes).decode("ascii")
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract the receipt fields as JSON."},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                ],
            },
        ],
    )
    content = resp.choices[0].message.content or "{}"
    try:
        raw = json.loads(content)
    except json.JSONDecodeError:
        raw = {}
    return _coerce(raw)
