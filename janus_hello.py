import time
import random
import asyncio

import janus

async def main():
    loop = asyncio.get_event_loop()
    queue = janus.Queue()
    future = loop.run_in_executor(None, data_source, queue)
    while (data:=await queue.async_q.get()) is not None: #async compatible queue api
        print(f'Got {data} off queue')
    print('Done')

def data_source(queue):
    for i in range(10):
        r = random.randint(0, 4)
        time.sleep(r)
        queue.sync_q.put(r) #blocking queue api
    queue.sync_q.put(None)

asyncio.run(main())
