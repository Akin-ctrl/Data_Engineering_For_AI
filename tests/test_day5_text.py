"""Tests for the Day 5 text chunking helpers.

Chunking cuts long text into smaller pieces. Overlap means each piece repeats
the last few words of the piece before it, so a sentence sitting on a boundary
is not lost.
"""

from __future__ import annotations

import pytest

from day5.pipeline.text import (
    chunk_text,
    normalize_whitespace,
    split_long_sentence,
    split_sentences,
    tail_words,
    word_count,
)


def make_sentences(count: int, words_each: int) -> str:
    """Build predictable text: `count` sentences of `words_each` words each."""

    sentences = []
    for sentence_number in range(count):
        words = [f"w{sentence_number}x{word_number}" for word_number in range(words_each)]
        sentences.append(" ".join(words) + ".")
    return " ".join(sentences)


class TestNormalizeWhitespace:
    """Whitespace is collapsed so chunk text is predictable."""

    def test_collapses_runs_of_spaces(self) -> None:
        assert normalize_whitespace("a   b") == "a b"

    def test_collapses_newlines_and_tabs(self) -> None:
        assert normalize_whitespace("a\n\tb") == "a b"

    def test_trims_the_edges(self) -> None:
        assert normalize_whitespace("  a b  ") == "a b"

    def test_empty_stays_empty(self) -> None:
        assert normalize_whitespace("   ") == ""


class TestSplitSentences:
    """Sentences are the unit chunks are assembled from."""

    def test_splits_on_sentence_endings(self) -> None:
        assert split_sentences("One. Two! Three?") == ["One.", "Two!", "Three?"]

    def test_empty_text_gives_no_sentences(self) -> None:
        assert split_sentences("   ") == []

    def test_text_without_punctuation_is_one_sentence(self) -> None:
        assert split_sentences("no ending punctuation here") == ["no ending punctuation here"]


class TestWordCountAndTailWords:
    """Small helpers the chunker leans on."""

    def test_word_count_counts_whitespace_separated_words(self) -> None:
        assert word_count("one two three") == 3

    def test_word_count_of_empty_text_is_zero(self) -> None:
        assert word_count("") == 0

    def test_tail_words_returns_the_last_n_words(self) -> None:
        assert tail_words("a b c d e", 2) == "d e"

    def test_tail_words_of_zero_is_empty(self) -> None:
        assert tail_words("a b c", 0) == ""

    def test_tail_words_handles_asking_for_more_than_exists(self) -> None:
        assert tail_words("a b", 10) == "a b"


class TestSplitLongSentence:
    """A single sentence longer than the window still has to be cut."""

    def test_short_sentence_is_left_alone(self) -> None:
        sentence = "one two three"
        assert split_long_sentence(sentence, max_words=10, overlap_words=2) == [sentence]

    def test_long_sentence_is_cut_into_windows(self) -> None:
        sentence = " ".join(str(number) for number in range(30))
        pieces = split_long_sentence(sentence, max_words=10, overlap_words=2)
        assert len(pieces) > 1
        assert all(word_count(piece) <= 10 for piece in pieces)


class TestChunkText:
    """The behaviour Day 5 actually teaches."""

    def test_empty_text_gives_no_chunks(self) -> None:
        assert chunk_text("   ", max_words=60, overlap_words=15) == []

    def test_text_shorter_than_the_window_is_one_chunk(self) -> None:
        text = make_sentences(count=2, words_each=5)
        chunks = chunk_text(text, max_words=60, overlap_words=15)
        assert len(chunks) == 1

    def test_text_longer_than_the_window_splits(self) -> None:
        text = make_sentences(count=12, words_each=10)
        chunks = chunk_text(text, max_words=60, overlap_words=15)
        assert len(chunks) > 1

    def test_a_smaller_window_produces_more_chunks(self) -> None:
        text = make_sentences(count=12, words_each=10)
        wide = chunk_text(text, max_words=120, overlap_words=15)
        narrow = chunk_text(text, max_words=40, overlap_words=15)
        assert len(narrow) > len(wide)

    def test_consecutive_chunks_share_words_when_overlap_is_set(self) -> None:
        text = make_sentences(count=12, words_each=10)
        chunks = chunk_text(text, max_words=60, overlap_words=15)
        first_words = set(chunks[0].split())
        second_words = set(chunks[1].split())
        assert first_words & second_words

    def test_no_overlap_means_no_repeated_words_between_chunks(self) -> None:
        text = make_sentences(count=12, words_each=10)
        chunks = chunk_text(text, max_words=60, overlap_words=0)
        first_words = set(chunks[0].split())
        second_words = set(chunks[1].split())
        assert not (first_words & second_words)

    def test_more_overlap_produces_more_chunks(self) -> None:
        text = make_sentences(count=20, words_each=10)
        light = chunk_text(text, max_words=60, overlap_words=5)
        heavy = chunk_text(text, max_words=60, overlap_words=40)
        assert len(heavy) > len(light)

    def test_chunks_are_never_empty(self) -> None:
        text = make_sentences(count=12, words_each=10)
        chunks = chunk_text(text, max_words=60, overlap_words=15)
        assert all(chunk.strip() for chunk in chunks)

    def test_chunking_is_repeatable(self) -> None:
        text = make_sentences(count=12, words_each=10)
        first = chunk_text(text, max_words=60, overlap_words=15)
        second = chunk_text(text, max_words=60, overlap_words=15)
        assert first == second

    @pytest.mark.parametrize("max_words", [30, 60, 120])
    def test_every_word_of_the_source_appears_somewhere(self, max_words: int) -> None:
        text = make_sentences(count=10, words_each=8)
        chunks = chunk_text(text, max_words=max_words, overlap_words=10)
        seen = {word for chunk in chunks for word in chunk.split()}
        assert set(text.split()) <= seen


class TestRealisticAbstractLength:
    """A guard for the reason the window was lowered to 60 words.

    ArXiv abstracts run around 70 words. At a 350 word window nothing ever
    split, so the overlap setting could never be seen. This test fails if the
    window is raised back to a size where abstract-length text stops splitting.
    """

    def test_abstract_length_text_splits_at_the_configured_window(self) -> None:
        from day5.pipeline.constants import DEFAULT_MAX_CHUNK_WORDS

        abstract = make_sentences(count=7, words_each=10)
        assert word_count(abstract) == 70

        chunks = chunk_text(abstract, max_words=DEFAULT_MAX_CHUNK_WORDS, overlap_words=15)
        assert len(chunks) > 1, (
            "Abstract-length text no longer splits. "
            f"DEFAULT_MAX_CHUNK_WORDS is {DEFAULT_MAX_CHUNK_WORDS}, which is too wide "
            "for the roughly 70 word abstracts this lab reads."
        )
