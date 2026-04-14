from mqnode.registry.dynamic_loader import load_function


def test_dynamic_loader():
    fn = load_function('mqnode.metrics.btc.network.nvt', '_calc_row')
    assert callable(fn)
