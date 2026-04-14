from __future__ import annotations

import argparse

from rq import Connection, Worker

from mqnode.config.settings import get_settings
from mqnode.queue.redis_conn import get_redis


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--queue', required=True)
    args = parser.parse_args()

    settings = get_settings()
    redis = get_redis(settings)
    with Connection(redis):
        worker = Worker([args.queue])
        worker.work()


if __name__ == '__main__':
    main()
