import asyncio
from asyncio.exceptions import SendfileNotAvailableError
from collections import deque, defaultdict
from contextlib import suppress
from typing import Deque, DefaultDict, Dict
from msgproto import read_msg, send_msg

SUBSCRIBERS: DefaultDict[bytes, Deque] = defaultdict(deque)
SEND_QUEUES: DefaultDict[asyncio.StreamWriter, asyncio.Queue] = defaultdict(asyncio.Queue)
CHAN_QUEUES: Dict[bytes, asyncio.Queue] = {} #1

async def client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peername = writer.get_extra_info('peername')
    subscribe_chan = await read_msg(reader)
    SUBSCRIBERS[subscribe_chan].append(writer)#2
    send_task = asyncio.create_task(send_client(writer, SEND_QUEUES[writer]))#3
    print(f'Remote {peername} subscribed to {subscribe_chan}')
    try:
        while channel_name := await read_msg(reader):
            data = await read_msg(reader)
            if channel_name not in CHAN_QUEUES:#4
                CHAN_QUEUES[channel_name] = asyncio.Queue(maxsize=10)#5
                asyncio.create_task(chan_sender(channel_name))#6
            await CHAN_QUEUES[channel_name].put(data)#7
    except asyncio.CancelledError:
        print(f'Remote {peername} connection cancelled')
    except asyncio.IncompleteReadError:
        print(f'Remote {peername} disconnected')
    finally:
        print(f'Remote {peername} closed')
        await SEND_QUEUES[writer].put(None)#8
        await send_task#9
        del SEND_QUEUES[writer]#10
        SUBSCRIBERS[subscribe_chan].remove(writer)

async def send_client(writer: asyncio.StreamWriter, queue: asyncio.Queue):#11
    while True:
        try:
            data = await queue.get()
        except asyncio.CancelledError:
            continue

        if not data:
            break

        try:
            await send_msg(writer, data)
        except  asyncio.CancelledError:
            await send_msg(writer, data)

    writer.close()
    await writer.wait_closed()

async def chan_sender(name: bytes):
    with suppress(asyncio.CancelledError):
        while True:
            writers = SUBSCRIBERS[name]
            if not writers:
                await asyncio.sleep(1)
                continue
            if name.startswith(b'/queue'): #13
                writers.rotate()
                writers = [writers[0]]
            if not (msg := await CHAN_QUEUES[name].get()):#14
                break
            for writer in writers:
                if not SEND_QUEUES[writer].full():
                    print(f'Sending to {name}: {msg[:19]}')
                    await SEND_QUEUES[writer].put(msg)#15

async def main(*args, **kwargs):
    server = await asyncio.start_server(*args, **kwargs)
    async with server:
        await server.server_forever()

try:
    asyncio.run(main(client, host='127.0.0.1', port=25000))
except KeyboardInterrupt:
    print('Bye')
