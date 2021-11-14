import asyncio
import argparse, uuid
from msgproto import read_msg, send_msg

async def main(args):
    me = uuid.uuid4().hex[:8] #1
    print(f'Starting up {me}')
    reader, writer = await asyncio.open_connection(args.host, args.port) #2
    print(f'I am {writer.get_extra_info("sockname")}')
    channel = args.listen.encode() #3
    await send_msg(writer, channel) #4
    try:
        while data := await read_msg(reader): #5
            print(f'Received by {me}: {data[:20]}')
        print('Connection ended')
    except asyncio.IncompleteReadError:
        print('Server closed')
    finally:
        writer.close()
        await writer.wait_closed()

if __name__ == '__main__':
    parser = argparse.ArgumentParser() #6
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default=25000)
    parser.add_argument('--listen', default='/topic/foo/')
    try:
        asyncio.run(main(parser.parse_args()))
    except KeyboardInterrupt:
        print('Bye')
