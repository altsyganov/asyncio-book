from time import ctime
from twisted.internet import asyncioreactor
asyncioreactor.install()
from twisted.internet import reactor, defer, task


async def main():
    for i in range(5):

        await task.deferLater(reactor, 1, print(f'{ctime()} Hello {i}'))

defer.ensureDeferred(main())
reactor.run()
