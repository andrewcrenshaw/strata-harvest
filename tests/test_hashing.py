"""Tests for content hashing utilities."""

import pytest

from strata_harvest.utils.hashing import content_hash, stable_listing_key


@pytest.mark.verification
class TestContentHash:
    def test_deterministic(self) -> None:
        assert content_hash("hello world") == content_hash("hello world")

    def test_whitespace_normalization(self) -> None:
        assert content_hash("hello  world") == content_hash("hello world")
        assert content_hash("hello\n\nworld") == content_hash("hello world")

    def test_different_content_differs(self) -> None:
        assert content_hash("hello") != content_hash("world")

    def test_returns_hex_string(self) -> None:
        h = content_hash("test")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


@pytest.mark.verification
class TestStableListingKey:
    def test_prefers_external_id(self) -> None:
        key = stable_listing_key(external_id="abc", url="https://example.com")
        assert key == "id:abc"

    def test_falls_back_to_url(self) -> None:
        key = stable_listing_key(url="https://example.com/job/1")
        assert key == "url:https://example.com/job/1"

    def test_falls_back_to_title(self) -> None:
        key = stable_listing_key(title="Senior Engineer")
        assert key.startswith("title:")

    def test_falls_back_to_content(self) -> None:
        key = stable_listing_key(content="Some job description")
        assert key.startswith("content:")

    def test_unknown_when_empty(self) -> None:
        assert stable_listing_key() == "unknown"
