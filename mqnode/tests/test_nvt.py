from mqnode.metrics.btc.network.nvt import _calc_row


def test_calc_row_zero_safe():
    transferred_value_usd, market_cap_usd, nvt_raw = _calc_row(0, 1, 1)
    assert transferred_value_usd == 0
    assert market_cap_usd == 0
    assert nvt_raw is None
