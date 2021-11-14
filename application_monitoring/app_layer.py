import argparse
import asyncio
from random import randint, uniform
from datetime import  datetime as dt
from datetime import timezone as tz
from contextlib import suppress
import zmq, zmq.asyncio, psutil

ctx = zmq.asyncio.Context()

async def stats_reporter(color: str):
    p = psutil.Process()
    sock = ctx.socket(zmq.PUB)
    sock.setsockopt(zmq.LINGER, 1)
    sock.connect('tcp://localhost:5555')
    with suppress(asyncio.CancelledError):
        while True:
            await sock.send_json(
                dict(
                    color=color,
                    timestamp=dt.now(tz=tz.utc).isoformat(),
                    cpu=p.cpu_percent(),
                    mem=p.memory_percent()*100
                )
            )
            await asyncio.sleep(1)
    sock.close()

async def main(args):
    asyncio.create_task(stats_reporter(args.color))
    leak = []
    with suppress(asyncio.CancelledError):
        while True:
            sum(range(randint(1_000, 10_000_000)))
            await asyncio.sleep(uniform(0, 1))
            leak += [0] * args.leak

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--color', type=str)
    parser.add_argument('--leak', type=int, default=0)
    args = parser.parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print('leaving...')
        ctx.term()
