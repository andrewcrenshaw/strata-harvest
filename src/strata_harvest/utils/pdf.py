import io
from pathlib import Path


def pdf_to_images(pdf_path: Path, dpi: int = 150) -> list[bytes]:
    """Render each PDF page to PNG bytes via pypdfium2 (Apple Silicon native)."""
    try:
        import pypdfium2 as pdfium  # type: ignore[import-untyped]
    except ImportError as e:
        raise ImportError(
            "pypdfium2 is required for PDF rendering. "
            "Install with `pip install strata-harvest[ocr]`"
        ) from e

    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    images = []

    # Load the document
    pdf = pdfium.PdfDocument(str(pdf_path))

    try:
        for i in range(len(pdf)):
            page = pdf[i]
            # render with given dpi (72 is pypdfium2 default/base, so scale = dpi / 72)
            scale = dpi / 72.0

            bitmap = page.render(scale=scale)
            # Use to_pil() to get a Pillow Image, then save it to a byte buffer
            pil_image = bitmap.to_pil()

            buffer = io.BytesIO()
            pil_image.save(buffer, format="PNG")
            images.append(buffer.getvalue())

    finally:
        pdf.close()

    return images
