import json
import pytest
from src.data import option_info as oi


def test_option_snapshot_coverage_and_contract_metadata():
    from src.data.ah_mapping import get_all_pairs
    pairs=get_all_pairs()
    rows=[oi.get_option_info(hk,p['a_code']) for hk,p in pairs.items()]
    assert all(row is not None for row in rows)
    assert sum(row['h_listed'] for row in rows)==56
    assert sum(row['weekly'] for row in rows)==18
    row=oi.get_option_info('03308','300308')
    assert row['h_listed'] and row['weekly']
    assert row['contract_size']==50 and row['hkats']=='ZJI'
    bank=oi.get_option_info('00939','601939')
    assert bank['h_listed'] and not bank['weekly']
    assert not oi.get_option_info('09980','605499')['h_listed']


def test_missing_or_mismatched_option_pair_is_unknown():
    assert oi.get_option_info('99999','999999') is None
    assert oi.get_option_info('03308','600000') is None


@pytest.mark.parametrize('case', ['missing_file','invalid_json','invalid_flag','invalid_size','missing_date'])
def test_invalid_option_metadata_is_unknown(tmp_path,monkeypatch,case):
    path=tmp_path/'options.json'
    snapshot=json.loads(oi.SNAPSHOT_PATH.read_text(encoding='utf-8'))
    row=next(r for r in snapshot['pairs'] if r['hk_code']=='03308')
    if case=='invalid_flag':row['h_listed']='false'
    if case=='invalid_size':row['contract_size']=0
    if case=='missing_date':snapshot.pop('h_source_date')
    if case!='missing_file':path.write_text('bad json' if case=='invalid_json' else json.dumps(snapshot),encoding='utf-8')
    monkeypatch.setattr(oi,'SNAPSHOT_PATH',path)
    assert oi.get_option_info('03308','300308') is None
