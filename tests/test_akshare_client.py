import pandas as pd
import pytest
from src.data import akshare_client

@pytest.mark.parametrize('modern', [True, False])
def test_tencent_volume_schema_keeps_shares_and_currency_distinct(monkeypatch, modern):
    row = dict(date='2026-10-15', open=110.98, high=114.25, low=110.61, close=113.95)
    if modern:
        row.update(volume=4654600., turnover=0.0069, amount=528272000.)
    else:
        row.update(amount=46546.)
    monkeypatch.setattr(akshare_client, '_with_retry', lambda *args, **kwargs: pd.DataFrame([row]))
    frame = akshare_client.get_a_kline('605499','2026-10-15','2026-10-15')
    assert frame.columns.is_unique
    assert frame.iloc[0]['volume'] == 4654600.
    expected = 528272000. if modern else 4654600. * 113.95
    assert frame.iloc[0]['turnover'] == expected
