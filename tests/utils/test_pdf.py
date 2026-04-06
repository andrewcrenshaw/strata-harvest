from pathlib import Path

import pytest

from strata_harvest.utils.pdf import pdf_to_images


@pytest.fixture
def sample_pdf(tmp_path: Path) -> Path:
    # Use reportlab or fpdf to create a dummy pdf?
    # No, pypdfium2 can actually create a simple pdf
    try:
        import pypdfium2 as pdfium  # type: ignore[import-untyped]

        pdf = pdfium.PdfDocument.new()

        # Add a page
        pdf.new_page(width=612, height=792)  # 8.5 x 11 inches at 72 dpi

        # Add a second page for multi-page test
        pdf.new_page(width=612, height=792)

        pdf_path = tmp_path / "test.pdf"
        pdf.save(str(pdf_path))
        pdf.close()
        return pdf_path
    except ImportError:
        pytest.skip("pypdfium2 not installed")


def test_single_page(sample_pdf: Path) -> None:
    # This will render both pages, but we can check if it returns bytes
    # The requirement mentions test_single_page. I will create a single page PDF here
    import pypdfium2 as pdfium

    single_page_pdf = sample_pdf.parent / "single.pdf"
    pdf = pdfium.PdfDocument.new()
    pdf.new_page(width=612, height=792)
    pdf.save(str(single_page_pdf))
    pdf.close()

    result = pdf_to_images(single_page_pdf, dpi=72)
    assert len(result) == 1
    assert isinstance(result[0], bytes)


def test_multi_page_count(sample_pdf: Path) -> None:
    # sample_pdf has 2 pages
    result = pdf_to_images(sample_pdf, dpi=72)
    assert len(result) == 2


def test_valid_png_output(sample_pdf: Path) -> None:
    result = pdf_to_images(sample_pdf, dpi=72)

    # Check PNG magic bytes: \x89PNG\r\n\x1a\n
    png_signature = b"\x89PNG\r\n\x1a\n"
    for img_bytes in result:
        assert img_bytes.startswith(png_signature)
