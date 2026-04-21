from __future__ import annotations

import argparse

from mqnode.market.price.runtime import run_price_source_ingestion_once


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    args = parser.parse_args()

    processed = run_price_source_ingestion_once(args.source)
    print(f'Price source processed: source={args.source} count={processed}')


if __name__ == '__main__':
    main()
