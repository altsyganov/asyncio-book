import asyncio
from collections import deque, defaultdict
from typing import Deque, DefaultDict
from msgproto import read_msg, send_msg

SUBSCRIBERS: DefaultDict[bytes, Deque] = defaultdict(deque)

async def client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peername = writer.get_extra_info('peername')
    subscribe_chan = await read_msg(reader)
    SUBSCRIBERS[subscribe_chan].append(writer)
    print(f'Remote {peername} subscribed to {subscribe_chan}')
    try:
        while channel_name := await read_msg(reader):
            data = await read_msg(reader)
            print(f'Sending to {channel_name}: {data[:19]}...')
            conns = SUBSCRIBERS[channel_name] #8
            if conns and channel_name.startswith(b'/queue'): #9
                conns.rotate() #10
                conns = [conns[0]] #11
            await asyncio.gather(*[send_msg(c, data) for c in conns]) #12
    except asyncio.CancelledError:
        print(f'Remote {peername} closing connection')
        writer.close()
        await writer.wait_closed()
    except asyncio.IncompleteReadError:
        print(f'Remote {peername} disconnected')
    finally:
        print(f'Remote {peername} closed')
        SUBSCRIBERS[subscribe_chan].remove(writer) #13

async def main(*args, **kwargs):
    server = await asyncio.start_server(*args, **kwargs)
    async with server:
        await server.serve_forever()

try:
    asyncio.run(main(client, host='127.0.0.1', port=25000))
except KeyboardInterrupt:
    print('Bye!')
