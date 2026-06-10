"""Generate a messy synthetic ``inbox/`` of receipts & invoices for the demo.

The output is intentionally dirty so each of the three agent phases has something
to catch:

- Phase 1 (Triage)   : a corrupt file, an exact duplicate, and a non-receipt image
- Phase 2 (Extract)  : a low-contrast / rotated receipt that still must be read
- Phase 3 (Validate) : math mismatch, future date, non-USD currency, duplicate
                       invoice number, and an over-policy-cap total

Receipts are rendered as images (PNG/JPG) and a couple as PDF using Pillow only
(no extra dependencies). The data is fully deterministic so the demo is reproducible.

Run directly to write the inbox to a local directory:
    uv run python -m mount_receipts.sample_data ./sample_inbox
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field

from PIL import Image, ImageDraw, ImageFont

# Business-rule constant the agent's Phase 3 enforces (kept in sync with validation).
POLICY_CAP_USD = 500.00


@dataclass
class Receipt:
    filename: str
    vendor: str
    date: str                       # ISO YYYY-MM-DD
    invoice_no: str
    items: list[tuple[str, float]]
    currency: str = "USD"
    total: float | None = None      # if None, computed as sum(items); set explicitly to force a mismatch
    fmt: str = "JPEG"               # JPEG | PNG | PDF
    rotate: int = 0
    low_contrast: bool = False
    # demo bookkeeping (not rendered): what should happen to this file, and why
    expected: str = "accept"        # accept | drop | reject
    reason: str = ""

    def computed_total(self) -> float:
        return round(sum(amt for _, amt in self.items), 2) if self.total is None else self.total


# --- clean receipts: pass all three phases (varied file formats) ------------
CLEAN: list[Receipt] = [
    Receipt("receipt_coffee.webp", "Blue Bottle Coffee", "2026-03-12", "BB-1001",
            [("Latte", 5.50), ("Croissant", 4.25)], fmt="WEBP", low_contrast=True),
    Receipt("receipt_office.bmp", "Staples", "2026-01-20", "ST-2002",
            [("Printer Paper", 12.99), ("Gel Pens", 6.50), ("Stapler", 14.00)], fmt="BMP"),
    Receipt("receipt_lunch.pdf", "Sweetgreen", "2025-11-05", "SG-3003",
            [("Harvest Bowl", 13.95), ("Lemonade", 3.50)], fmt="PDF"),
    # multi-page PDF: items on page 1, total on page 2 — exercises page stacking
    Receipt("receipt_hardware.pdf", "Home Depot", "2026-02-28", "HD-4004",
            [("Cordless Drill", 89.00), ("Wood Screws", 7.49)], fmt="PDF2"),
]

# --- business-rule failures: extracted in Phase 2, rejected in Phase 3 ------
REJECTS: list[Receipt] = [
    Receipt("receipt_mathbad.tiff", "Quick Mart", "2026-03-01", "QM-5005",
            [("Soda", 2.00), ("Chips", 3.00)], total=12.00, fmt="TIFF",
            expected="reject", reason="total != sum(line items)"),
    Receipt("receipt_future.jpg", "Cafe Tomorrow", "2027-08-01", "CF-6006",
            [("Green Tea", 4.00)], fmt="JPEG",
            expected="reject", reason="future-dated"),
    Receipt("receipt_euro.jpg", "Paris Bistro", "2026-04-10", "PB-7007",
            [("Plat du Jour", 24.00)], currency="EUR", fmt="JPEG",
            expected="reject", reason="non-USD currency"),
    Receipt("receipt_dupinv.jpg", "Quick Mart", "2026-03-02", "QM-5005",
            [("Bottled Water", 1.50)], fmt="JPEG",
            expected="reject", reason="duplicate invoice number (QM-5005)"),
    Receipt("receipt_lux.jpg", "Lux Grand Hotel", "2026-05-01", "LH-8008",
            [("Executive Suite", 600.00)], fmt="JPEG",
            expected="reject", reason=f"total exceeds policy cap ${POLICY_CAP_USD:.0f}"),
]


def _font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    try:
        return ImageFont.load_default(size=size)
    except TypeError:  # very old Pillow
        return ImageFont.load_default()


def render_receipt(r: Receipt) -> Image.Image:
    """Render a receipt-like image from a Receipt spec."""
    W, H = 520, 720
    bg = 210 if r.low_contrast else 255
    fg = (120, 120, 120) if r.low_contrast else (10, 10, 10)
    img = Image.new("RGB", (W, H), (bg, bg, bg))
    d = ImageDraw.Draw(img)

    head, body, small = _font(30), _font(22), _font(18)
    y = 28
    d.text((W // 2 - len(r.vendor) * 8, y), r.vendor, fill=fg, font=head); y += 50
    d.text((30, y), f"Invoice: {r.invoice_no}", fill=fg, font=small); y += 28
    d.text((30, y), f"Date:    {r.date}", fill=fg, font=small); y += 40
    d.line([(30, y), (W - 30, y)], fill=fg, width=2); y += 20

    for name, amt in r.items:
        d.text((40, y), name, fill=fg, font=body)
        d.text((W - 150, y), f"{r.currency} {amt:,.2f}", fill=fg, font=body)
        y += 34
    y += 10
    d.line([(30, y), (W - 30, y)], fill=fg, width=2); y += 20
    d.text((40, y), "TOTAL", fill=fg, font=head)
    d.text((W - 200, y), f"{r.currency} {r.computed_total():,.2f}", fill=fg, font=head)

    d.text((30, H - 50), "Thank you for your business!", fill=fg, font=small)
    if r.rotate:
        img = img.rotate(r.rotate, expand=True, fillcolor=(bg, bg, bg))
    return img


def _save(img: Image.Image, path: str, fmt: str) -> None:
    if fmt == "PDF":
        img.convert("RGB").save(path, "PDF", resolution=150.0)
    elif fmt == "PDF2":
        # split into two pages -> multi-page PDF (total lands on page 2)
        w, h = img.size
        top, bottom = img.crop((0, 0, w, h // 2)), img.crop((0, h // 2, w, h))
        top.convert("RGB").save(
            path, "PDF", save_all=True, append_images=[bottom.convert("RGB")], resolution=150.0
        )
    elif fmt == "PNG":
        img.save(path, "PNG")
    elif fmt in ("WEBP", "TIFF", "BMP"):
        img.convert("RGB").save(path, fmt)
    else:
        img.convert("RGB").save(path, "JPEG", quality=88)


def generate(out_dir: str) -> dict:
    """Write the messy inbox to ``out_dir`` and return a manifest of expected outcomes."""
    os.makedirs(out_dir, exist_ok=True)
    manifest: list[dict] = []

    def record(filename: str, expected: str, reason: str) -> None:
        manifest.append({"file": filename, "expected": expected, "reason": reason})

    for r in CLEAN + REJECTS:
        _save(render_receipt(r), os.path.join(out_dir, r.filename), r.fmt)
        record(r.filename, r.expected, r.reason)

    # Phase 1 triage targets:
    # 1) exact duplicate of a clean receipt
    dup_src, dup_dst = "receipt_coffee.webp", "receipt_coffee_copy.webp"
    with open(os.path.join(out_dir, dup_src), "rb") as f:
        data = f.read()
    with open(os.path.join(out_dir, dup_dst), "wb") as f:
        f.write(data)
    record(dup_dst, "drop", f"exact duplicate of {dup_src}")

    # 4) an unsupported / non-image file (can't be decoded -> dropped in triage)
    with open(os.path.join(out_dir, "notes.txt"), "w", encoding="utf-8") as f:
        f.write("remember to file the Q1 expense report\n")
    record("notes.txt", "drop", "unsupported / non-image file")

    # 2) a corrupt/unreadable "image"
    with open(os.path.join(out_dir, "receipt_corrupt.jpg"), "wb") as f:
        f.write(b"\xff\xd8\xff\xe0NOT_A_REAL_JPEG\x00\x01\x02broken")
    record("receipt_corrupt.jpg", "drop", "corrupt / unreadable file")

    # 3) a non-receipt photo
    photo = Image.new("RGB", (640, 420), (90, 150, 220))
    pd = ImageDraw.Draw(photo)
    pd.ellipse([(420, 40), (560, 180)], fill=(250, 230, 120))          # "sun"
    pd.rectangle([(0, 300), (640, 420)], fill=(230, 210, 150))         # "beach"
    pd.text((40, 40), "Beach Vacation 2026", fill=(255, 255, 255), font=_font(34))
    photo.save(os.path.join(out_dir, "beach_photo.png"), "PNG")
    record("beach_photo.png", "drop", "not a receipt")

    summary = {
        "policy_cap_usd": POLICY_CAP_USD,
        "total_files": len(manifest),
        "accept": sum(1 for m in manifest if m["expected"] == "accept"),
        "drop": sum(1 for m in manifest if m["expected"] == "drop"),
        "reject": sum(1 for m in manifest if m["expected"] == "reject"),
        "files": manifest,
    }
    with open(os.path.join(out_dir, "_expected_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return summary


def main() -> None:
    out = sys.argv[1] if len(sys.argv) > 1 else "./sample_inbox"
    summary = generate(out)
    print(f"Wrote {summary['total_files']} files to {out}")
    print(f"  accept={summary['accept']}  drop={summary['drop']}  reject={summary['reject']}")
    for m in summary["files"]:
        tag = m["expected"].upper()
        print(f"  [{tag:<6}] {m['file']}" + (f"  — {m['reason']}" if m["reason"] else ""))


if __name__ == "__main__":
    main()
