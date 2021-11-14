import asyncio
import argparse, uuid
from itertools import count
from msgproto import send_msg

async def main(args):
    me = uuid.uuid4().hex[:8] #1
    print(f'Starting up {me}')
    reader, writer = await asyncio.open_connection(host=args.host, port=args.port) #2
    print(f'I am {writer.get_extra_info("sockname")}')

    channel = b'/null' #3
    print(writer, channel)
    await send_msg(writer, channel) #4

    chan = args.channel.encode() #5
    try:
        for i in count(): #6
            await asyncio.sleep(args.interval) #7
            data = b'X'*args.size or f'Msg {i} from {me}'.encode()
            try:
                await send_msg(writer, chan)
                await send_msg(writer, data) #8
            except OSError:
                print('Connection ended')
                break
    except asyncio.CancelledError:
        writer.close()
        await writer.wait_closed()

if __name__ == '__main__':
    parser = argparse.ArgumentParser() #9
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default=25000, type=int)
    parser.add_argument('--channel', default='topic/foo')
    parser.add_argument('--interval', default=1, type=float)
    parser.add_argument('--size', default=0, type=int)
try:
    asyncio.run(main(parser.parse_args()))
except KeyboardInterrupt:
    print('Bye')

