"""
test_translator_contract.py
===========================
Tests for snf_peirce.translator_contract

Run with:
    pytest tests/test_translator_contract.py -v

These tests cover:
    1. MediaType vocabulary
    2. InputMode vocabulary
    3. TranslatorCandidate validation
    4. TranslatorContract structural validation (via validate())
    5. Typed error hierarchy
    6. A mock search translator end-to-end
    7. A mock file translator end-to-end
    8. A mock BOTH translator end-to-end
    9. ready() contract behavior
    10. Edge cases and failure modes
"""

import pytest
from unittest.mock import MagicMock, patch

from snf_peirce.translator_contract import (
    TranslatorContract,
    TranslatorCandidate,
    MediaType,
    InputMode,
    TranslatorError,
    SearchError,
    IngestError,
    TranslationError,
    ConfigurationError,
    CONTRACT_VERSION,
)


# ---------------------------------------------------------------------------
# Fixtures — reusable mock translators and candidates
# ---------------------------------------------------------------------------

def make_candidate(**overrides):
    """Return a valid TranslatorCandidate with optional field overrides."""
    defaults = dict(
        external_id   = "12345",
        title         = "Blast of Silence",
        creator       = "Allen Baron",
        year          = "1961",
        media_type    = MediaType.FILM,
        secondary     = "Crime, Drama · US · 77 min",
        thumbnail_url = None,
        raw           = {"id": 12345, "title": "Blast of Silence"}
    )
    defaults.update(overrides)
    return TranslatorCandidate(**defaults)


class MinimalSearchTranslator(TranslatorContract):
    """
    Minimal compliant search translator for testing.
    Returns a fixed candidate. Translates to a dict standing in for SRFRecord.
    """
    id           = "test_search"
    display_name = "Test Search Source"
    media_types  = [MediaType.FILM]
    input_mode   = InputMode.SEARCH
    requires_key = False

    def ready(self) -> bool:
        return True

    def search(self, query: str) -> list:
        return [make_candidate(title=f"Result for: {query}")]

    def translate(self, candidate: TranslatorCandidate) -> dict:
        # Returns a dict as a stand-in for SRFRecord in unit tests
        return {
            "nucleus":      f"test://{candidate.external_id}",
            "nucleus_type": "film",
            "WHO":          {},
            "WHAT":         {"title": candidate.title},
            "WHEN":         {"year": candidate.year},
            "WHERE":        {},
            "HOW":          {},
        }


class MinimalFileTranslator(TranslatorContract):
    """Minimal compliant file translator for testing."""
    id           = "test_file"
    display_name = "Test File Source"
    media_types  = [MediaType.CASE]
    input_mode   = InputMode.FILE
    requires_key = False

    def ready(self) -> bool:
        return True

    def ingest(self, filepath: str) -> list:
        # Simulates parsing a file — returns fixed candidates
        return [
            make_candidate(
                external_id = "case-001",
                title       = "Smith v. Jones",
                creator     = "Judge Williams",
                year        = "2019",
                media_type  = MediaType.CASE,
                secondary   = "Court of Appeals · WA · 2019",
                raw         = {"citation": "2019 WL 12345"}
            )
        ]

    def translate(self, candidate: TranslatorCandidate) -> dict:
        return {
            "nucleus":      f"test://{candidate.external_id}",
            "nucleus_type": "case",
            "WHO":          {"court": "Court of Appeals"},
            "WHAT":         {"citation": candidate.raw.get("citation", "")},
            "WHEN":         {"decided": candidate.year},
            "WHERE":        {},
            "HOW":          {},
        }


class MinimalBothTranslator(TranslatorContract):
    """Minimal compliant BOTH translator for testing."""
    id           = "test_both"
    display_name = "Test Both Source"
    media_types  = [MediaType.RECORDING, MediaType.ALBUM]
    input_mode   = InputMode.BOTH
    requires_key = True

    def ready(self) -> bool:
        import os
        return bool(os.environ.get("TEST_API_KEY"))

    def search(self, query: str) -> list:
        return [make_candidate(media_type=MediaType.RECORDING)]

    def ingest(self, filepath: str) -> list:
        return [make_candidate(media_type=MediaType.RECORDING)]

    def translate(self, candidate: TranslatorCandidate) -> dict:
        return {"nucleus": f"test://{candidate.external_id}"}


class BrokenTranslator(TranslatorContract):
    """Translator that raises errors — for testing error handling."""
    id           = "test_broken"
    display_name = "Broken Source"
    media_types  = [MediaType.FILM]
    input_mode   = InputMode.SEARCH
    requires_key = False

    def ready(self) -> bool:
        return True

    def search(self, query: str) -> list:
        raise SearchError("API is down")

    def translate(self, candidate: TranslatorCandidate) -> dict:
        raise TranslationError("Cannot translate this candidate")


# ---------------------------------------------------------------------------
# 1. MediaType vocabulary
# ---------------------------------------------------------------------------

class TestMediaType:

    def test_all_expected_types_exist(self):
        expected = [
            "film", "tv", "recording", "album", "book",
            "article", "case", "statute", "artwork",
            "dataset", "person", "organization"
        ]
        vocab = vars(MediaType)
        for t in expected:
            assert t in vocab.values(), f"MediaType missing: {t}"

    def test_values_are_strings(self):
        for key, val in vars(MediaType).items():
            if not key.startswith("_"):
                assert isinstance(val, str), \
                    f"MediaType.{key} should be str, got {type(val)}"

    def test_no_duplicate_values(self):
        values = [
            v for k, v in vars(MediaType).items()
            if not k.startswith("_") and isinstance(v, str)
        ]
        assert len(values) == len(set(values)), \
            "MediaType has duplicate values"


# ---------------------------------------------------------------------------
# 2. InputMode vocabulary
# ---------------------------------------------------------------------------

class TestInputMode:

    def test_three_modes_exist(self):
        assert InputMode.SEARCH == "search"
        assert InputMode.FILE   == "file"
        assert InputMode.BOTH   == "both"

    def test_values_are_strings(self):
        assert isinstance(InputMode.SEARCH, str)
        assert isinstance(InputMode.FILE, str)
        assert isinstance(InputMode.BOTH, str)


# ---------------------------------------------------------------------------
# 3. TranslatorCandidate validation
# ---------------------------------------------------------------------------

class TestTranslatorCandidate:

    def test_valid_candidate_creates_successfully(self):
        c = make_candidate()
        assert c.external_id == "12345"
        assert c.title == "Blast of Silence"
        assert c.media_type == MediaType.FILM

    def test_rejects_invalid_media_type(self):
        with pytest.raises(ValueError, match="not in the controlled vocabulary"):
            make_candidate(media_type="movie")  # should be MediaType.FILM

    def test_rejects_empty_external_id(self):
        with pytest.raises(ValueError, match="external_id must not be empty"):
            make_candidate(external_id="")

    def test_rejects_empty_title(self):
        with pytest.raises(ValueError, match="title must not be empty"):
            make_candidate(title="")

    def test_thumbnail_url_is_optional(self):
        c = make_candidate(thumbnail_url=None)
        assert c.thumbnail_url is None

    def test_raw_defaults_to_empty_dict(self):
        c = TranslatorCandidate(
            external_id   = "1",
            title         = "Test",
            creator       = "Someone",
            year          = "2020",
            media_type    = MediaType.BOOK,
            secondary     = "Publisher",
            thumbnail_url = None
        )
        assert c.raw == {}

    def test_all_media_types_accepted(self):
        valid_types = [
            v for k, v in vars(MediaType).items()
            if not k.startswith("_") and isinstance(v, str)
        ]
        for mt in valid_types:
            c = make_candidate(media_type=mt)
            assert c.media_type == mt

    def test_year_as_string(self):
        c = make_candidate(year="1961")
        assert isinstance(c.year, str)

    def test_raw_carries_full_source_data(self):
        raw = {"id": 99, "title": "Test", "extra": "data"}
        c = make_candidate(raw=raw)
        assert c.raw["extra"] == "data"


# ---------------------------------------------------------------------------
# 4. TranslatorContract structural validation
# ---------------------------------------------------------------------------

class TestTranslatorValidation:

    def test_valid_search_translator_passes_validation(self):
        t = MinimalSearchTranslator()
        errors = t.validate()
        assert errors == [], f"Unexpected errors: {errors}"

    def test_valid_file_translator_passes_validation(self):
        t = MinimalFileTranslator()
        errors = t.validate()
        assert errors == [], f"Unexpected errors: {errors}"

    def test_valid_both_translator_passes_validation(self):
        t = MinimalBothTranslator()
        errors = t.validate()
        assert errors == [], f"Unexpected errors: {errors}"

    def test_missing_id_fails_validation(self):
        class NoId(TranslatorContract):
            display_name = "No ID"
            media_types  = [MediaType.FILM]
            input_mode   = InputMode.SEARCH
            requires_key = False
            def ready(self): return True
            def translate(self, c): return {}

        t = NoId()
        errors = t.validate()
        assert any("id" in e for e in errors)

    def test_missing_display_name_fails_validation(self):
        class NoName(TranslatorContract):
            id           = "no_name"
            media_types  = [MediaType.FILM]
            input_mode   = InputMode.SEARCH
            requires_key = False
            def ready(self): return True
            def translate(self, c): return {}

        t = NoName()
        errors = t.validate()
        assert any("display_name" in e for e in errors)

    def test_invalid_input_mode_fails_validation(self):
        class BadMode(TranslatorContract):
            id           = "bad_mode"
            display_name = "Bad Mode"
            media_types  = [MediaType.FILM]
            input_mode   = "api"   # not a valid InputMode
            requires_key = False
            def ready(self): return True
            def translate(self, c): return {}

        t = BadMode()
        errors = t.validate()
        assert any("input_mode" in e for e in errors)

    def test_invalid_media_type_in_list_fails_validation(self):
        class BadMedia(TranslatorContract):
            id           = "bad_media"
            display_name = "Bad Media"
            media_types  = ["movie"]   # should be MediaType.FILM
            input_mode   = InputMode.SEARCH
            requires_key = False
            def ready(self): return True
            def translate(self, c): return {}

        t = BadMedia()
        errors = t.validate()
        assert any("media_type" in e for e in errors)

    def test_multiple_missing_attributes_all_reported(self):
        class Skeleton(TranslatorContract):
            def ready(self): return True
            def translate(self, c): return {}

        t = Skeleton()
        errors = t.validate()
        # Should report all five missing attributes
        assert len(errors) >= 5


# ---------------------------------------------------------------------------
# 5. Typed error hierarchy
# ---------------------------------------------------------------------------

class TestErrorHierarchy:

    def test_all_errors_inherit_from_translator_error(self):
        assert issubclass(SearchError, TranslatorError)
        assert issubclass(IngestError, TranslatorError)
        assert issubclass(TranslationError, TranslatorError)
        assert issubclass(ConfigurationError, TranslatorError)

    def test_translator_error_is_exception(self):
        assert issubclass(TranslatorError, Exception)

    def test_search_error_is_catchable_as_translator_error(self):
        with pytest.raises(TranslatorError):
            raise SearchError("test")

    def test_ingest_error_is_catchable_as_translator_error(self):
        with pytest.raises(TranslatorError):
            raise IngestError("test")

    def test_translation_error_is_catchable_as_translator_error(self):
        with pytest.raises(TranslatorError):
            raise TranslationError("test")

    def test_configuration_error_is_catchable_as_translator_error(self):
        with pytest.raises(TranslatorError):
            raise ConfigurationError("test")

    def test_errors_carry_message(self):
        err = SearchError("API is down")
        assert "API is down" in str(err)


# ---------------------------------------------------------------------------
# 6. Mock search translator end-to-end
# ---------------------------------------------------------------------------

class TestSearchTranslatorEndToEnd:

    def setup_method(self):
        self.t = MinimalSearchTranslator()

    def test_ready_returns_true(self):
        assert self.t.ready() is True

    def test_validate_passes(self):
        assert self.t.validate() == []

    def test_search_returns_list(self):
        results = self.t.search("christmas noir")
        assert isinstance(results, list)

    def test_search_returns_candidates(self):
        results = self.t.search("christmas noir")
        assert all(isinstance(r, TranslatorCandidate) for r in results)

    def test_search_result_title_reflects_query(self):
        results = self.t.search("blast of silence")
        assert "blast of silence" in results[0].title.lower()

    def test_translate_returns_result(self):
        candidate = make_candidate()
        result = self.t.translate(candidate)
        assert result is not None

    def test_translate_nucleus_contains_external_id(self):
        candidate = make_candidate(external_id="99999")
        result = self.t.translate(candidate)
        assert "99999" in result["nucleus"]

    def test_translate_preserves_title(self):
        candidate = make_candidate(title="Cash on Demand")
        result = self.t.translate(candidate)
        assert result["WHAT"]["title"] == "Cash on Demand"

    def test_translate_preserves_year(self):
        candidate = make_candidate(year="1961")
        result = self.t.translate(candidate)
        assert result["WHEN"]["year"] == "1961"

    def test_search_then_translate_full_flow(self):
        results = self.t.search("christmas noir")
        assert len(results) > 0
        record = self.t.translate(results[0])
        assert record is not None
        assert "nucleus" in record


# ---------------------------------------------------------------------------
# 7. Mock file translator end-to-end
# ---------------------------------------------------------------------------

class TestFileTranslatorEndToEnd:

    def setup_method(self):
        self.t = MinimalFileTranslator()

    def test_ready_returns_true(self):
        assert self.t.ready() is True

    def test_validate_passes(self):
        assert self.t.validate() == []

    def test_ingest_returns_list(self):
        results = self.t.ingest("/fake/path/export.docx")
        assert isinstance(results, list)

    def test_ingest_returns_candidates(self):
        results = self.t.ingest("/fake/path/export.docx")
        assert all(isinstance(r, TranslatorCandidate) for r in results)

    def test_ingest_candidate_has_case_media_type(self):
        results = self.t.ingest("/fake/path/export.docx")
        assert all(r.media_type == MediaType.CASE for r in results)

    def test_translate_produces_case_nucleus_type(self):
        results = self.t.ingest("/fake/path/export.docx")
        record = self.t.translate(results[0])
        assert record["nucleus_type"] == "case"

    def test_translate_carries_citation_from_raw(self):
        results = self.t.ingest("/fake/path/export.docx")
        record = self.t.translate(results[0])
        assert "2019 WL 12345" in record["WHAT"]["citation"]

    def test_ingest_then_translate_full_flow(self):
        results = self.t.ingest("/fake/path/export.docx")
        assert len(results) > 0
        record = self.t.translate(results[0])
        assert "nucleus" in record

    def test_search_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            self.t.search("some query")


# ---------------------------------------------------------------------------
# 8. Mock BOTH translator end-to-end
# ---------------------------------------------------------------------------

class TestBothTranslatorEndToEnd:

    def test_search_path_works(self, monkeypatch):
        monkeypatch.setenv("TEST_API_KEY", "fake-key")
        t = MinimalBothTranslator()
        assert t.ready() is True
        results = t.search("test")
        assert len(results) > 0

    def test_ingest_path_works(self, monkeypatch):
        monkeypatch.setenv("TEST_API_KEY", "fake-key")
        t = MinimalBothTranslator()
        results = t.ingest("/fake/path/export.csv")
        assert len(results) > 0

    def test_ready_false_without_key(self, monkeypatch):
        monkeypatch.delenv("TEST_API_KEY", raising=False)
        t = MinimalBothTranslator()
        assert t.ready() is False

    def test_both_paths_return_candidates(self, monkeypatch):
        monkeypatch.setenv("TEST_API_KEY", "fake-key")
        t = MinimalBothTranslator()
        search_results = t.search("test")
        ingest_results = t.ingest("/fake/path/file.csv")
        assert all(isinstance(r, TranslatorCandidate) for r in search_results)
        assert all(isinstance(r, TranslatorCandidate) for r in ingest_results)


# ---------------------------------------------------------------------------
# 9. ready() contract behavior
# ---------------------------------------------------------------------------

class TestReadyContract:

    def test_ready_returns_bool(self):
        t = MinimalSearchTranslator()
        result = t.ready()
        assert isinstance(result, bool)

    def test_ready_true_when_configured(self):
        t = MinimalSearchTranslator()
        assert t.ready() is True

    def test_ready_false_when_key_missing(self, monkeypatch):
        monkeypatch.delenv("TEST_API_KEY", raising=False)
        t = MinimalBothTranslator()
        assert t.ready() is False

    def test_ready_true_when_key_present(self, monkeypatch):
        monkeypatch.setenv("TEST_API_KEY", "anything")
        t = MinimalBothTranslator()
        assert t.ready() is True


# ---------------------------------------------------------------------------
# 10. Error handling and failure modes
# ---------------------------------------------------------------------------

class TestErrorHandling:

    def setup_method(self):
        self.t = BrokenTranslator()

    def test_search_raises_search_error(self):
        with pytest.raises(SearchError):
            self.t.search("anything")

    def test_search_error_is_translator_error(self):
        with pytest.raises(TranslatorError):
            self.t.search("anything")

    def test_translate_raises_translation_error(self):
        candidate = make_candidate()
        with pytest.raises(TranslationError):
            self.t.translate(candidate)

    def test_translation_error_is_translator_error(self):
        candidate = make_candidate()
        with pytest.raises(TranslatorError):
            self.t.translate(candidate)

    def test_search_error_message_is_preserved(self):
        with pytest.raises(SearchError, match="API is down"):
            self.t.search("anything")

    def test_translation_error_message_is_preserved(self):
        with pytest.raises(TranslationError, match="Cannot translate"):
            self.t.translate(make_candidate())

    def test_empty_search_returns_empty_list_not_none(self):
        class EmptyTranslator(TranslatorContract):
            id           = "empty"
            display_name = "Empty"
            media_types  = [MediaType.FILM]
            input_mode   = InputMode.SEARCH
            requires_key = False
            def ready(self): return True
            def search(self, query): return []
            def translate(self, c): return {}

        t = EmptyTranslator()
        results = t.search("nothing")
        assert results == []
        assert results is not None

    def test_contract_version_is_set(self):
        t = MinimalSearchTranslator()
        assert hasattr(t, "contract_version")
        assert t.contract_version == CONTRACT_VERSION


# ---------------------------------------------------------------------------
# 11. Contract version
# ---------------------------------------------------------------------------

class TestContractVersion:

    def test_contract_version_exists(self):
        assert CONTRACT_VERSION is not None

    def test_contract_version_is_string(self):
        assert isinstance(CONTRACT_VERSION, str)

    def test_contract_version_is_semver_shaped(self):
        parts = CONTRACT_VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)

    def test_translator_inherits_contract_version(self):
        t = MinimalSearchTranslator()
        assert t.contract_version == CONTRACT_VERSION
