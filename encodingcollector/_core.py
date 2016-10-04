from collections import deque, namedtuple
import chardet

from twisted.internet import defer, endpoints, protocol, task, threads
from twisted.python import threadpool
from twisted.logger import Logger
from twisted.words.protocols import irc
import sys
import sqlite3
import random

Channel = namedtuple("Channel", "name visible topic")


class DeferredBuffer(object):
    stopped = False

    def __init__(self):
        self.pending = deque()
        self.ready = deque()

    def _errbackAllPending(self, exc):
        pending, self.pending = self.pending, deque()
        while pending:
            pending.popleft().errback(exc)

    def _callbackAllPending(self, value):
        pending, self.pending = self.pending, deque()
        while pending:
            pending.popleft().callback(value)

    def push(self, channel):
        if not self.pending:
            self.ready.append(channel)
        else:
            self._callbackAllPending(channel)

    def stop(self):
        self.stopped = True

    def get(self):
        if self.stopped and not self.ready:
            self._errbackAllPending(StopIteration())
            return defer.fail(StopIteration())
        try:
            return defer.succeed(self.ready.popleft())
        except IndexError:
            pass
        pending = defer.Deferred()
        self.pending.append(pending)
        return pending


class Persists(object):
    _log = Logger()

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS encodings (name TEXT, count INT);
    CREATE TABLE IF NOT EXISTS summary (total INT, utf8 INT);
    """

    def __init__(self, reactor, threadPool):
        self.reactor = reactor
        self.threadPool = threadPool

    def start(self, path):
        self.connection = sqlite3.connect(path)
        self.connection.executescript(self.SCHEMA)
        self.threadPool.start()

    def stop(self):
        self.threadPool.stop()
        self.connection.close()

    def _incrementEncoding(self, txn, encoding):
        q = '''
        INSERT OR REPLACE INTO encodings
          VALUES (?, COALESCE((SELECT count FROM encodings WHERE name = ?),
                               0) + 1)
        '''
        txn.execute(q, (encoding,))

    def _incrementSummary(self, txn, isUTF8):
        q = '''
        INSERT OR REPLACE INTO summary
          VALUES (COALESCE((SELECT total FROM summary), 0) + 1,
                  COALESCE((SELECT utf8 FROM summary), 0) + ?)
        '''
        txn.execute(q, (1 if isUTF8 else 0,))

    def _recordStatistics(self, encoding, isUTF8):
        with self.connection:
            self._incrementEncoding(self.connection, encoding)
            self._incrementSummary(self.connection, isUTF8)

    def recordStatistics(self, encoding, isUTF8):
        threads.deferToThread(self.reactor, self.threadPool,
                              self._recordStatistics, encoding, isUTF8)


class AnalyzesText(object):
    _log = Logger()

    UNKNOWN = "<unknown>"

    def __init__(self, persister):
        self.persister = persister

    def detectEncoding(self, byteString):
        try:
            encoding = chardet.detect(byteString)
        except Exception:
            self._log.failure("Could not detect encoding of {byteString}",
                              byteString=byteString)
            encoding = UNKNOWN
        return encoding

    def isUTF8(self, byteString):
        try:
            bytesString.decode('utf-8')
        except UnicodeDecodeError:
            return False
        else:
            return True

    def process(self, bytesString):
        self.persister.recordStatistics(
            encoding=self.detectEncoding(bytesString),
            isUTF8=isUTF8(bytesString),
        )


class EncodingCollectionBot(irc.IRCClient):
    nickname = "encodingcollecto"

    def __init__(self):
        self._channelBuffers = []

    @defer.inlineCallbacks
    def signedOn(self):
        iterator = yield self.listChannels()
        while True:
            try:
                channel = yield iterator.get()
            except StopIteration:
                break
            log.msg("Joining %r" % channel.name)
            yield task.deferLater(self.factory.reactor,
                                  self.factory.rnd.randint(1, 5),
                                  lambda: None)
            self.join(channel.name)

    def joined(self, channel):
        try:
            channel.name.decode('utf-8')
        except UnicodeDecodeError:


    def listChannels(self):
        iterator = DeferredBuffer()
        self._channelBuffers.append(iterator)
        self._listAllChannels()
        return iterator

    def _listAllChannels(self):
        self.sendLine("LIST")

    def irc_RPL_LIST(self, prefix, params):
        channel = Channel(*params[1:])
        for iterator in self._channelBuffers:
            iterator.push(channel)

    def irc_RPL_LISTEND(self, prefix, params):
        for iterator in self._channelBuffers:
            iterator.stop()

    def privmsg(self, user, channel, message):
        try:
            message.decode('utf-8')
        except UnicodeError:
            pass
        else:
            self.factory.utf_8 += 1
        self.factory.total += 1


class EncodingCollectionFactory(protocol.ReconnectingClientFactory):
    protocol = EncodingCollectionBot
    total = 0
    utf_8 = 0

    loopingPrint = None

    def __init__(self, reactor):
        self.reactor = reactor
        self.rnd = random.SystemRandom()

    def startLoopingPrint(self):
        self.loopingPrint = task.LoopingCall(self.loopingPrint)
        self.loopingPrint.reactor = self.reactor
        self.loopingPrint.start(30)

    def stopLoopingPrint(self):
        if self.loopingPrint:
            self.loopingPrint.stop()

    def loopingPrint(self):
        total = float(self.total)
        if total:
            utf_8_pct = self.utf_8 / total
        else:
            utf_8_pct = 0

        print "utf-8: {} ({})".format(utf_8_pct, self.total)


def main(reactor, description):
    from twisted.python import log
    log.startLogging(sys.stdout)
    endpoint = endpoints.clientFromString(reactor, description)
    factory = EncodingCollectionFactory(reactor)
    factory.startLoopingPrint()
    d = endpoint.connect(factory)
    return d.addCallback(lambda ignored: defer.Deferred())

task.react(main, ['tcp:irc.dal.net:6667'])
