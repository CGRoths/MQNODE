from mqnode.config.settings import get_settings
from mqnode.db.connection import DB


SQL = '''
INSERT INTO metric_registry(
  metric_name, chain, factor, module_path, function_name, interval, enabled, version, output_table, dependencies
) VALUES
  ('nvt_raw', 'BTC', 'NETWORK', 'mqnode.metrics.btc.network.nvt', 'calculate_nvt', '30m', true, 'v1', 'btc_nvt_30m', '["btc_primitive_30m"]'::jsonb),
  ('nvt_raw', 'BTC', 'NETWORK', 'mqnode.metrics.btc.network.nvt', 'calculate_nvt', '1h', true, 'v1', 'btc_nvt_1h', '["btc_primitive_30m"]'::jsonb)
ON CONFLICT (metric_name, chain, interval, version)
DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = now();
'''

if __name__ == '__main__':
    with DB(get_settings()).cursor() as cur:
        cur.execute(SQL)
    print('Metric registry seeded.')
