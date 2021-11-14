import asyncio
from contextlib import suppress

import zmq
import zmq.asyncio
import aiohttp
from aiohttp import web
from aiohttp_sse import sse_response
from weakref import WeakSet
import json

ctx = zmq.asyncio.Context()
connections = WeakSet()

async def collector():
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt_string(zmq.SUBSCRIBE, '')
    sock.bind('tcp://*:5555')
    with suppress(asyncio.CancelledError):
        while data := await sock.recv_json():
            print(data)
            for q in connections:
                await q.put(data)
    sock.close()

async def feed(request):
    queue = asyncio.Queue()
    connections.add(queue)
    with suppress(asyncio.CancelledError):
        async with sse_response(request) as resp:
            while data := await queue.get():
                print(f'sending data {data}')
                await resp.send(json.dumps(data))
    return resp

async def index(request):
    return aiohttp.web.FileResponse('/home/leksus/Documents/experience/asynciobook/application_monitoring/charts.html')

async def start_collector(app):
    loop = asyncio.get_running_loop()
    app['collector'] = loop.create_task(collector())

async def stop_collector(app):
    print('stopping collector')
    app['collector'].cancel()
    await app['collector']
    ctx.term()

if __name__ == '__main__':
    app = web.Application()
    app.router.add_route('GET', '/', index)
    app.router.add_route('GET', '/feed', feed)
    app.on_startup.append(start_collector)
    app.on_cleanup.append(stop_collector)
    web.run_app(app, host='localhost', port=8888)
