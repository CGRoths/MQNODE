# MQNODE (MamakQuantNode) - Phase 0 (Bitcoin)

Self-owned crypto data infrastructure with raw ingestion, primitive caching, derived metrics, and API delivery.

## Full Project Tree

```text
mqnode/
  config/
    settings.py
    logging_config.py
  core/
    app_context.py
    errors.py
    utils.py
  db/
    connection.py
    schema.sql
    migrations.py
    repositories.py
  chains/
    btc/
      rpc.py
      listener.py
      ingest.py
      primitive_builder.py
      block_parser.py
  queue/
    redis_conn.py
    jobs.py
    producer.py
  registry/
    metric_registry.py
    dynamic_loader.py
  checkpoints/
    checkpoint_service.py
  metrics/
    btc/
      network/
        nvt.py
      miner/
        miner_revenue.py
      market/
        market_cap.py
      fee/
        fee_metrics.py
  workers/
    worker_base.py
    btc_primitive_worker.py
    btc_network_worker.py
    btc_miner_worker.py
    btc_market_worker.py
    run_worker.py
  api/
    main.py
    routes/
      health.py
      btc_metrics.py
      registry.py
      checkpoints.py
  scripts/
    init_db.py
    seed_registry.py
    backfill_btc.py
  tests/
    test_nvt.py
    test_checkpoint.py
    test_registry.py
Dockerfile
docker-compose.yml
requirements.txt
.env.example
README.md
```

## Primitive vs Derived Separation

- Bitcoin RPC is only called at ingestion (`listener.py` -> `ingest.py`).
- Raw data is persisted in `btc_blocks_raw`.
- Reusable block primitives are persisted in `btc_primitive_block`.
- Reusable 30m primitives are persisted in `btc_primitive_30m`.
- Derived metrics such as NVT are calculated from primitive tables and persisted in separate metric tables (`btc_nvt_30m`, `btc_nvt_1h`).
- Final metric outputs are not stored in primitive tables.

## Worker Process (Complete Lifecycle)

1. Worker starts via `python -m mqnode.workers.run_worker --queue <queue_name>`.
2. RQ waits for queue jobs.
3. Job payload is received (`primitive_ready` for network factor).
4. Factor worker loads enabled metric rows from `metric_registry`.
5. Dynamic loader imports metric module/function.
6. Metric function executes with `(db, bucket_start_utc, interval)`.
7. Metric upserts into output table.
8. Checkpoint updates status in `sync_checkpoints`.
9. Exceptions are isolated per metric and do not crash whole worker.
10. Worker continues listening.

## Checkpoint Logic

- Checkpoints are keyed by `(chain, component, interval)`.
- Listener updates `btc_raw_block_ingestion:block` only after successful transaction commit.
- Primitive builder updates `btc_primitive_30m_builder:30m` after upsert success.
- Metric workers update per-metric checkpoints (e.g., `btc_metric_nvt_raw_30m`).
- On error, checkpoint status is set to `error` with `error_message`.

## Hot-Add New Metric (No Full Shutdown)

1. Create new metric file: `metrics/btc/network/new_metric.py`.
2. Implement `calculate_new_metric(db, bucket_start_utc, interval)`.
3. Add output table in `schema.sql`.
4. Insert row in `metric_registry` with enabled=false.
5. Build new Docker image.
6. Start target worker only:
   ```bash
   docker compose up -d --build btc-network-worker
   ```
7. Run health check.
8. Enable metric:
   ```sql
   UPDATE metric_registry SET enabled = true WHERE metric_name = 'new_metric';
   ```
9. Worker continues processing from checkpoint.
10. API endpoint becomes available (generic route or dedicated route).

## Manual Operation Commands

Start whole stack:
```bash
docker compose up -d --build
```

View logs:
```bash
docker compose logs -f btc-listener
docker compose logs -f btc-network-worker
docker compose logs -f mqnode-api
```

Initialize DB:
```bash
python scripts/init_db.py
```

Seed registry:
```bash
python scripts/seed_registry.py
```

Run listener locally:
```bash
python -m mqnode.chains.btc.listener
```

Run worker locally:
```bash
python -m mqnode.workers.run_worker --queue btc_network
```

Run backfill:
```bash
python scripts/backfill_btc.py --start-height 0 --end-height 1000
```

Check API:
```bash
curl http://localhost:8000/health
curl "http://localhost:8000/api/v1/btc/metrics/nvt?interval=30m&start=2024-01-01&end=2024-01-02"
```

## Example registry seed data for NVT

See `mqnode/scripts/seed_registry.py`:
- `nvt_raw` for `30m` -> `btc_nvt_30m`
- `nvt_raw` for `1h` -> `btc_nvt_1h`

## Example API Response for NVT

```json
{
  "interval": "30m",
  "count": 1,
  "items": [
    {
      "bucket_start_utc": "2025-01-01T00:00:00+00:00",
      "price_usd": "43000",
      "supply_total_sat": "1960000000000000",
      "market_cap_usd": "842800000000",
      "transferred_sat_30m": "125000000000",
      "transferred_value_usd": "53750000",
      "nvt_raw": "15680.9302",
      "source_start_height": 820000,
      "source_end_height": 820003,
      "version": "v1"
    }
  ]
}
```
