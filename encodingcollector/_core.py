from contextlib import closing
from collections import deque, namedtuple
import chardet
import datetime

from twisted.internet import defer, endpoints, protocol, task, threads
from twisted.python import threadpool
from twisted.logger import Logger, textFileLogObserver, globalLogBeginner
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
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS runs (
                                 id INTEGER,
                                 start DATETIME,
                                 stop DATETIME,
                                 CONSTRAINT n_pk PRIMARY KEY(id)
                               );
    CREATE TABLE IF NOT EXISTS networks (
                                 id INTEGER,
                                 address TEXT,
                                 port INTEGER,
                                 CONSTRAINT n_pk PRIMARY KEY(id)
                               );
    CREATE TABLE IF NOT EXISTS runs_networks (
                                 run INTEGER,
                                 network INTEGER,
                                 CONSTRAINT nr_rn PRIMARY KEY(run, network),
                                 FOREIGN KEY(run) REFERENCES runs(id),
                                 FOREIGN KEY(network) REFERENCES networks(id)
    );
    CREATE TABLE IF NOT EXISTS encodings (
                                 id INTEGER,
                                 run INTEGER,
                                 network INTEGER,
                                 name TEXT,
                                 count INTEGER,
                                 FOREIGN KEY(run) REFERENCES runs(id),
                                 CONSTRAINT encodings_pk PRIMARY KEY(id)
                                 CONSTRAINT encodings_rn UNIQUE(run,
                                                                network,
                                                                name)
                               );
    CREATE TABLE IF NOT EXISTS summary (
                                 id INTEGER,
                                 run INTEGER,
                                 network INTEGER,
                                 total INTEGER,
                                 utf8 INTEGER,
                                 FOREIGN KEY(run) REFERENCES runs(id),
                                 CONSTRAINT summary_pk PRIMARY KEY(id),
                                 CONSTRAINT summary_rn UNIQUE(run, network)
                               );
    """

    now = datetime.datetime.now

    def __init__(self, reactor, threadPool):
        self.reactor = reactor
        self.threadPool = threadPool
        self.networkIDs = {}

    def start(self, path, addresses):
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.executescript(self.SCHEMA)

        self.networkIDs = self._beginRun(addresses)

        self.threadPool.start()

    def stop(self):
        self.threadPool.stop()

        self._endRun()

        self.connection.close()

    def _beginRun(self, addresses):
        runQ = '''
        INSERT INTO runs (start) VALUES (?)
        '''

        networkQ = '''
        INSERT OR REPLACE INTO networks (address, port) VALUES (?, ?)
        '''

        runsNetworksQ = '''
        INSERT INTO runs_networks VALUES (?, ?)
        '''
        with self.connection:
            with closing(self.connection.cursor()) as runCursor:
                runCursor.execute(runQ, (self.now(),))
                self.runID = runCursor.lastrowid

            runNetworks = []
            networkIDs = {}
            with closing(self.connection.cursor()) as addressCursor:
                for addr in addresses:
                    addressCursor.execute(networkQ, addr)
                    networkID = addressCursor.lastrowid
                    runNetworks.append((self.runID, networkID))
                    networkIDs[addr] = networkID

            with closing(self.connection.cursor()) as networksRunsCursor:
                networksRunsCursor.executemany(runsNetworksQ, runNetworks)
        return networkIDs

    def _endRun(self):
        if self.runID is None:
            raise ValueError("Must call start before stop!")

        q = '''
        UPDATE runs SET stop = ? WHERE id = ?
        '''
        with self.connection:
            self.connection.execute(q, (self.now(), self.runID))

    def _incrementEncoding(self, txn, networkID, encoding):
        q = '''
        INSERT OR REPLACE INTO encodings
          (id, run, network, name, count)
          VALUES ((SELECT id FROM encodings WHERE
                       run = :run
                   AND network = :network
                   AND name = :name),
                  :run,
                  :network,
                  :name,
                  COALESCE((SELECT count FROM encodings WHERE
                                   run = :run
                               AND network = :network
                               AND name = :name),
                           0) + 1)
        '''
        txn.execute(q, {"run": self.runID,
                        "network": networkID,
                        "name": encoding})

    def _incrementSummary(self, txn, networkID, isUTF8):
        q = '''
        INSERT OR REPLACE INTO summary
          (id, run, network, total, utf8)
          VALUES ((SELECT id FROM summary WHERE
                                  run = :run
                              AND network = :network),
                  :run,
                  :network,
                  COALESCE((SELECT total FROM summary WHERE
                                             run = :run
                                         AND network = :network),
                           0) + 1,
                  COALESCE((SELECT utf8 FROM summary WHERE
                                             run = :run
                                         AND network = :network),
                           0) + :utf8_increment)
        '''
        txn.execute(q, {"run": self.runID,
                        "network": networkID,
                        "utf8_increment": 1 if isUTF8 else 0})

    def _recordStatistics(self, networkID, encoding, isUTF8):
        with self.connection:
            self._incrementEncoding(self.connection, networkID, encoding)
            self._incrementSummary(self.connection, networkID, isUTF8)

    def recordStatistics(self, addr, encoding, isUTF8):
        networkID = self.networkIDs[addr]
        d = threads.deferToThreadPool(self.reactor, self.threadPool,
                                      self._recordStatistics,
                                      networkID, encoding, isUTF8)
        d.addErrback(lambda f: self._log.failure("Could not persist", f))


class AnalyzesText(object):
    _log = Logger()

    UNKNOWN = "<unknown>"

    def __init__(self, persister):
        self.persister = persister

    def detectEncoding(self, byteString):
        try:
            guess = chardet.detect(byteString)
        except Exception:
            self._log.failure("Could not detect encoding of {byteString}",
                              byteString=byteString)
            encoding = self.UNKNOWN
        else:
            encoding = guess['encoding']
        self._log.info("Detected encoding: {encoding}", encoding=encoding)
        return encoding

    def isUTF8(self, byteString):
        try:
            byteString.decode('utf-8')
        except UnicodeDecodeError:
            return False
        else:
            return True

    def process(self, addr, bytesString):
        self.persister.recordStatistics(
            addr,
            encoding=self.detectEncoding(bytesString),
            isUTF8=self.isUTF8(bytesString),
        )


class EncodingCollectionBot(irc.IRCClient):
    nickname = "encodingcollecto"
    _log = Logger()
    addr = None

    def __init__(self, reactor, rnd, analyzer):
        self._channelBuffers = []

        self._reactor = reactor
        self._random = rnd
        self._analyzer = analyzer

    @defer.inlineCallbacks
    def signedOn(self):
        iterator = yield self.listChannels()
        while True:
            try:
                channel = yield iterator.get()
            except StopIteration:
                break
            self._log.info("Joining {channel}", channel=channel)
            yield task.deferLater(self._reactor,
                                  self._random.randint(1, 5),
                                  lambda: None)
            self.join(channel.name)

    def joined(self, channel):
        self._log.info("Joined {channel}", channel=repr(channel))
        self._analyzer.process(self.addr, channel)

    def listChannels(self):
        buffer = DeferredBuffer()
        self._channelBuffers.append(buffer)
        self._listAllChannels()
        return buffer

    def _listAllChannels(self):
        self.sendLine("LIST")

    def irc_RPL_LIST(self, prefix, params):
        channel = Channel(*params[1:])
        for buffer in self._channelBuffers:
            buffer.push(channel)

    def irc_RPL_LISTEND(self, prefix, params):
        for buffer in self._channelBuffers:
            buffer.stop()

    def privmsg(self, user, channel, message):
        self._analyzer.process(self.addr, user)
        self._analyzer.process(self.addr, channel)
        self._analyzer.process(self.addr, message)


class EncodingCollectionFactory(protocol.ReconnectingClientFactory):
    protocol = EncodingCollectionBot

    def __init__(self, reactor, rnd, analyzer):
        self._reactor = reactor
        self._random = rnd
        self._analyzer = analyzer

    def buildProtocol(self, addr):
        p = self.protocol(self._reactor, self._random, self._analyzer)
        p.factory = self
        return p


@defer.inlineCallbacks
def main(reactor, *descriptions):
    log = Logger()
    globalLogBeginner.beginLoggingTo([textFileLogObserver(sys.stdout)])
    endpointObjects = [endpoints.clientFromString(reactor, description)
                       for description in descriptions]
    hostPorts = [(endpoint._host, endpoint._port)
                 for endpoint in endpointObjects]

    pool = threadpool.ThreadPool(minthreads=1, maxthreads=1, name="persiter")
    persister = Persists(reactor, pool)
    reactor.addSystemEventTrigger("before", "shutdown", persister.stop)
    persister.start("log.sqlite", hostPorts)

    analyzer = AnalyzesText(persister)

    factory = EncodingCollectionFactory(reactor,
                                        random.SystemRandom(),
                                        analyzer)

    for (host, port), endpoint in zip(hostPorts, endpointObjects):
        try:
            protocol = yield endpoint.connect(factory)
        except Exception:
            log.failure("Could not connect to {host}:{port}",
                        host=host, port=port)
            raise
        protocol.addr = (host, port)

    defer.returnValue(defer.Deferred())

task.react(main, ['tcp:irc.us.ircnet.net:6667',
                  'tcp:irc.quakenet.org:6667',
                  'tcp:irc.prison.net:6667',
                  'tcp:Chicago.IL.US.Undernet.org:6667',
                  'tcp:irc.rizon.sexy:6667',
                  'tcp:irc.chlame.net:6667',
                  'tcp:irc.irc-hispano.org:6667',
                  'tcp:irc.oftc.net:6667',
                  'tcp:irc.chatzona.org:6667',
                  'tcp:irc.freenode.net:6667'])
