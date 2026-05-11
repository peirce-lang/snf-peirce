"""
translator_contract.py
======================
Semantic Normalized Form — Translator Plugin Contract
snf-peirce v0.2.0+

This module defines the interface that all SNF translators must implement
to be recognized and loaded by the Translator Workbench.

A translator is a single Python file placed in the workbench's translators/
directory. If it implements TranslatorContract correctly and ready() returns
True, it will appear automatically in the source selector. No other
registration is required.

USAGE
-----
To write a new translator:

    1. Create a file in translators/  e.g. translators/discogs.py
    2. Define a class that inherits from TranslatorContract
    3. Declare the required class attributes (id, display_name, etc.)
    4. Implement translate() and ready()
    5. Implement search() if input_mode is "search" or "both"
    6. Implement ingest() if input_mode is "file" or "both"
    7. Drop the file in. The workbench picks it up on next start.

The test for a correct implementation:
    translator = MyTranslator()
    assert translator.ready()
    candidates = translator.search("some query")
    assert len(candidates) > 0
    record = translator.translate(candidates[0])
    assert isinstance(record, SRFRecord)

VERSIONING
----------
This contract is versioned. The CONTRACT_VERSION constant below reflects
the current interface version. Translators may declare which contract
version they implement via the optional contract_version class attribute.
Breaking changes to the contract will increment the major version.

V1 SCOPE
--------
V1 does not require:
    - Pagination support (search returns all results in one call)
    - Authentication beyond API keys in env vars
    - Caching or rate limiting (translator's responsibility if needed)
    - Streaming or async support

These may be added in v2 as optional extensions to the contract.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

# Contract version — translators may reference this
CONTRACT_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Controlled vocabulary — media types
# ---------------------------------------------------------------------------

class MediaType:
    """
    Controlled vocabulary for media types.

    All translators MUST use these constants for media_type fields.
    Do not use free strings. If your source produces a type not listed
    here, propose an addition to the vocabulary rather than inventing
    a new string.

    The workbench uses media_type to group cart items and build
    expedition section headings. Inconsistent strings break grouping
    silently.
    """
    FILM        = "film"
    TV          = "tv"
    RECORDING   = "recording"
    ALBUM       = "album"
    BOOK        = "book"
    ARTICLE     = "article"
    CASE        = "case"
    STATUTE     = "statute"
    ARTWORK     = "artwork"
    DATASET     = "dataset"
    PERSON      = "person"       # for biographical corpora
    ORGANIZATION = "organization"


# ---------------------------------------------------------------------------
# Input modes
# ---------------------------------------------------------------------------

class InputMode:
    """
    Declares how a translator receives its input.

    SEARCH  — translator accepts a free-text query string and calls an
              external API or service. The workbench shows a search bar.

    FILE    — translator accepts a file path and parses a local file
              (DOCX, CSV, JSON, MARC, etc.). The workbench shows a
              file picker.

    BOTH    — translator supports both paths. The workbench shows a
              toggle between search bar and file picker.
    """
    SEARCH  = "search"
    FILE    = "file"
    BOTH    = "both"


# ---------------------------------------------------------------------------
# TranslatorCandidate — the unit of selection
# ---------------------------------------------------------------------------

@dataclass
class TranslatorCandidate:
    """
    A single result returned by search() or ingest().

    Candidates are displayed as result cards in the workbench. The user
    browses candidates, previews them, and adds selected ones to the cart.
    Adding to cart triggers translate(), which converts the candidate to
    an SRFRecord.

    Fields
    ------
    external_id : str
        The source's own identifier for this item. TMDB ID, MBID, ISBN,
        citation string, etc. Must be stable and sufficient to identify
        the item uniquely within the source.

    title : str
        Display title. Used in result cards and cart items.

    creator : str
        Primary creator. Director, artist, author, judge, etc.
        Single string. Multi-creator sources should join with commas
        or use the most significant contributor.

    year : str
        Production, release, or publication year as a four-digit string.
        Use empty string if unknown. Do not use None.

    media_type : str
        Must be a value from MediaType. Used for cart grouping and
        expedition section headings.

    secondary : str
        One line of contextual detail. Genre + country + runtime for
        films. Publisher + page count for books. Court + jurisdiction
        for cases. Keep it short — this is a single display line.

    thumbnail_url : str, optional
        URL to a thumbnail image. Used in preview panels. Optional in v1.
        Pass None if not available.

    raw : dict
        The complete source response for this item. Passed directly to
        translate() so the translation step does not require a second
        API call. Store everything the source returned here.
    """
    external_id:    str
    title:          str
    creator:        str
    year:           str
    media_type:     str
    secondary:      str
    thumbnail_url:  Optional[str]
    raw:            dict = field(default_factory=dict)

    def __post_init__(self):
        if self.media_type not in vars(MediaType).values():
            raise ValueError(
                f"media_type '{self.media_type}' is not in the controlled "
                f"vocabulary. Use a MediaType constant."
            )
        if not self.external_id:
            raise ValueError("external_id must not be empty.")
        if not self.title:
            raise ValueError("title must not be empty.")


# ---------------------------------------------------------------------------
# TranslatorError — typed errors for the workbench
# ---------------------------------------------------------------------------

class TranslatorError(Exception):
    """
    Base class for translator errors.

    Raise a subclass of TranslatorError rather than a generic exception
    so the workbench can display a meaningful message to the user instead
    of a raw traceback.
    """
    pass

class SearchError(TranslatorError):
    """Raised when a search() call fails — API down, bad query, etc."""
    pass

class IngestError(TranslatorError):
    """Raised when an ingest() call fails — bad file, parse error, etc."""
    pass

class TranslationError(TranslatorError):
    """Raised when translate() fails to produce a valid SRFRecord."""
    pass

class ConfigurationError(TranslatorError):
    """Raised when ready() detects a missing or invalid configuration."""
    pass


# ---------------------------------------------------------------------------
# TranslatorContract — the interface
# ---------------------------------------------------------------------------

class TranslatorContract(ABC):
    """
    Base class for all SNF translators.

    Every translator must:
        1. Declare the required class attributes below.
        2. Implement translate() — converts a candidate to SRFRecord.
        3. Implement ready() — returns True if the translator is usable.
        4. Implement search() if input_mode is "search" or "both".
        5. Implement ingest() if input_mode is "file" or "both".

    Class attributes (declare on the subclass, not in __init__)
    ----------------
    id : str
        Unique machine name. snake_case. Used as the translator's key
        throughout the system. Must be stable — changing it breaks
        saved configurations.
        Examples: "tmdb", "musicbrainz", "westlaw_docx"

    display_name : str
        Human-readable name shown in the source selector.
        Examples: "TMDB", "MusicBrainz", "Westlaw (DOCX Export)"

    media_types : list[str]
        List of MediaType values this translator produces.
        Used to filter the source selector by content type in future.
        Examples: ["film", "tv"], ["recording"], ["book"]

    input_mode : str
        One of InputMode.SEARCH, InputMode.FILE, InputMode.BOTH.

    requires_key : bool
        True if this translator requires an API key or credential
        to function. Used by the workbench to show configuration
        warnings when a key is missing.

    contract_version : str  (optional)
        The CONTRACT_VERSION this translator was written against.
        Defaults to "1.0.0". Used for future compatibility checking.
    """

    # Required class attributes — declare on subclass
    id:             str
    display_name:   str
    media_types:    list
    input_mode:     str
    requires_key:   bool

    # Optional
    contract_version: str = CONTRACT_VERSION

    # ------------------------------------------------------------------
    # Search path
    # ------------------------------------------------------------------

    def search(self, query: str) -> list[TranslatorCandidate]:
        """
        Search the source and return a list of candidates.

        Required if input_mode is InputMode.SEARCH or InputMode.BOTH.
        Not called if input_mode is InputMode.FILE.

        Parameters
        ----------
        query : str
            Free-text search string entered by the user. The translator
            decides how to interpret it — title search, keyword search,
            person search, etc.

        Returns
        -------
        list[TranslatorCandidate]
            List of candidates matching the query. May be empty.
            Return an empty list for no results, not None.

        Raises
        ------
        SearchError
            If the search fails for any reason — API unreachable,
            rate limited, invalid response, etc.

        V1 NOTE: No pagination. Return all results in one call.
        Limit to a reasonable number (20-50) if the source returns
        hundreds of results.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement search(). "
            f"Set input_mode to InputMode.FILE if this translator "
            f"only accepts file input."
        )

    # ------------------------------------------------------------------
    # File path
    # ------------------------------------------------------------------

    def ingest(self, filepath: str) -> list[TranslatorCandidate]:
        """
        Parse a local file and return a list of candidates.

        Required if input_mode is InputMode.FILE or InputMode.BOTH.
        Not called if input_mode is InputMode.SEARCH.

        Parameters
        ----------
        filepath : str
            Absolute path to the file to parse. The workbench validates
            that the file exists before calling this method.

        Returns
        -------
        list[TranslatorCandidate]
            List of candidates parsed from the file. May be empty.
            Return an empty list for a valid but empty file, not None.

        Raises
        ------
        IngestError
            If the file cannot be parsed — wrong format, corrupt file,
            missing required fields, etc.

        V1 NOTE: Ingest is synchronous. For large files this may block.
        Async support is deferred to v2.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement ingest(). "
            f"Set input_mode to InputMode.SEARCH if this translator "
            f"only accepts search input."
        )

    # ------------------------------------------------------------------
    # Translation — always required
    # ------------------------------------------------------------------

    @abstractmethod
    def translate(self, candidate: TranslatorCandidate) -> object:
        """
        Translate a candidate to an SRFRecord.

        This is the core of the translator. It receives a candidate
        (which contains the full raw source response in candidate.raw)
        and returns a valid SRFRecord.

        The translate step should NOT make additional API calls in v1.
        All data needed for translation should be present in candidate.raw,
        populated during the search() or ingest() step.

        Parameters
        ----------
        candidate : TranslatorCandidate
            The candidate to translate. candidate.raw contains the full
            source response from the search or ingest step.

        Returns
        -------
        SRFRecord
            A valid, fully populated SRF record. The workbench will
            validate this against the SRFRecord schema before adding
            it to the cart. Validation failures raise TranslationError.

        Raises
        ------
        TranslationError
            If the candidate cannot be translated to a valid SRFRecord.
        """
        pass

    # ------------------------------------------------------------------
    # Health check — always required
    # ------------------------------------------------------------------

    @abstractmethod
    def ready(self) -> bool:
        """
        Return True if this translator is configured and ready to use.

        Called by the workbench at startup for all registered translators.
        Translators that return False are shown as unavailable in the
        source selector with a configuration warning rather than
        silently failing mid-search.

        This method should check:
            - API keys present in environment (if requires_key is True)
            - Required libraries installed (for file-based translators)
            - Any other precondition needed to function

        This method should NOT make network calls. Check configuration
        only. Network availability is checked at search time.

        Returns
        -------
        bool
            True if the translator is ready. False otherwise.
        """
        pass

    # ------------------------------------------------------------------
    # Validation helper — called by workbench, not translator
    # ------------------------------------------------------------------

    def validate(self) -> list[str]:
        """
        Validate that required class attributes are declared correctly.

        Called by the workbench plugin loader. Returns a list of
        validation errors. An empty list means the translator is
        structurally valid.

        Translators do not need to call or override this method.
        """
        errors = []
        required = ["id", "display_name", "media_types", "input_mode", "requires_key"]
        for attr in required:
            if not hasattr(self, attr):
                errors.append(f"Missing required class attribute: {attr}")

        if hasattr(self, "id") and not isinstance(self.id, str):
            errors.append("id must be a string")

        if hasattr(self, "input_mode") and self.input_mode not in [
            InputMode.SEARCH, InputMode.FILE, InputMode.BOTH
        ]:
            errors.append(
                f"input_mode must be one of: "
                f"InputMode.SEARCH, InputMode.FILE, InputMode.BOTH"
            )

        if hasattr(self, "media_types"):
            valid = set(vars(MediaType).values())
            for mt in self.media_types:
                if mt not in valid:
                    errors.append(
                        f"media_type '{mt}' not in controlled vocabulary"
                    )

        return errors
