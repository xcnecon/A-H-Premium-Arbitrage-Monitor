"""Tests for A/H pair mapping."""

from src.data.ah_mapping import get_a_code, get_all_pairs, get_hk_code, get_pair_name


def test_get_a_code_known():
    assert get_a_code("00939") == "601939"


def test_get_a_code_unknown():
    assert get_a_code("99999") is None


def test_get_hk_code_reverse():
    assert get_hk_code("601939") == "00939"


def test_get_all_pairs_nonempty():
    pairs = get_all_pairs()
    assert len(pairs) > 50


def test_get_pair_name():
    name = get_pair_name("00939")
    assert name is not None
    assert len(name) > 0


def test_normalize_short_code():
    # "939" should normalize to "00939"
    assert get_a_code("939") == "601939"
