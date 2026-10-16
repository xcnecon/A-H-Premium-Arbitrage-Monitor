import json
import pytest
from src.data import short_eligibility as se


def test_snapshot_matches_all_registered_pairs():
    from src.data.ah_mapping import get_all_pairs
    pairs = get_all_pairs()
    assert all(se.get_short_eligibility(hk, info['a_code']) is not None for hk,info in pairs.items())
    assert se.get_short_eligibility('03308','300308')['a_eligible'] is True
    assert se.get_short_eligibility('03308','300308')['h_eligible'] is True
    assert se.get_short_eligibility('01330','601330')['a_eligible'] is False
    assert se.get_short_eligibility('01330','601330')['h_eligible'] is True


def test_missing_or_mismatched_pair_is_unknown():
    assert se.get_short_eligibility('99999','999999') is None
    assert se.get_short_eligibility('03308','600000') is None


@pytest.mark.parametrize('content', ['not-json', '{"pairs": []}', '{"pairs":[{"hk_code":"03308","a_code":"300308","a_eligible":"false","h_eligible":true}]}'])
def test_invalid_snapshot_never_means_eligible(tmp_path,monkeypatch,content):
    path=tmp_path/'snapshot.json';path.write_text(content,encoding='utf-8')
    monkeypatch.setattr(se,'SNAPSHOT_PATH',path)
    assert se.get_short_eligibility('03308','300308') is None
