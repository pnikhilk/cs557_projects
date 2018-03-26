#! /usr/bin/python
import glob
from hashlib import sha256
import logging
import os
import pdb
import socket
import sys
sys.path.append("lib/gen-py/chord")
sys.path.append("gen-py/chord")
try:
    sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])
except Exception as e:
    pass

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import FileStore
import ttypes


class ChordHandler:

    logging.basicConfig(level=logging.ERROR)

    def __init__(self):
        ip = socket.gethostbyname(socket.gethostname())
        ip_port = ip + ":" + sys.argv[1]
        self.logger = logging.getLogger(__name__)
        filehash = self._get_hash(ip_port)
        self.node = ttypes.NodeID(filehash, ip, sys.argv[1])
        self.finger_table = None
        self.file_table = {}

    @staticmethod
    def _get_hash(arg):
        sha = sha256()
        sha.update(arg)
        return sha.hexdigest()

    @staticmethod
    def _in_range(id1, id2, key, preced=False):
        """
        Find if key belongs to range (id1, id2)
        """
        if (id1 > id2):
            if (not preced):
                if (key <= id2 or key > id1):
                    return True
            elif (key < id2 or key > id1):
                return True
        else:
            if (not preced):
                if (key > id1) and (key <= id2):
                    return True
            elif (key > id1 and key < id2):
                return True
        return False

    @staticmethod
    def _client_succ(node):
        socket = TSocket.TSocket(node.ip, node.port)
        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = FileStore.Client(protocol)
        transport.open()
        return client.getNodeSucc()

    @staticmethod
    def _client_findpred(node, key):
        socket = TSocket.TSocket(node.ip, node.port)
        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = FileStore.Client(protocol)
        transport.open()
        return client.findPred(key)

    def writeFile(self, rFile):
        content = rFile.content
        meta = rFile.meta
        filehash = self._get_hash(meta.owner + ":" + meta.filename)
        # pdb.set_trace()
        content_hash = self._get_hash(content)
        successor = self.findSucc(filehash)
        if successor.id != self.node.id:
            raise(ttypes.SystemException("This node does not own this file."))
        if filehash not in self.file_table:
            meta.contentHash = content_hash
            meta.version = 0
            self.file_table[filehash] = meta
        else:
            self.file_table[filehash].version += 1
            self.file_table[filehash].contentHash = content_hash

        with open(meta.filename, "wb") as fout:
            fout.write(content)
        pass

    def readFile(self, filename, owner):
        """
        Retrun requested file
        :param filename: Name of file
        :param owner: Owner of file
        :return rFile: RFile object
        """
        self.logger.debug("Readfile fname:%s, owner:%s" % (filename, owner))
        filehash = self._get_hash(owner + ":" + filename)
        if (filehash in self.file_table) and os.path.exists(filename):
            with open(filename, "rb") as fin:
                content = fin.read()
            rFile = ttypes.RFile(self.file_table[filehash], content)
            return rFile
        else:
            if not os.path.exists(filename):
                raise ttypes.SystemException("Requested file does not exists.")
            else:
                raise ttypes.SystemException("File does not belong to thes node.")

    def setFingertable(self, node_list):
        """
        Set finger table for current server node.
        :param node_list: List containing NodeID objects
        """
        for i, node in enumerate(node_list, start=1):
            if not self.finger_table:
                self.finger_table = {}
            self.finger_table[i] = node

    def findSucc(self, key):
        """
        Find successor node of key
        :param key: SHA256 hash key of a file
        :return node: successor NodeID object
        """
        node = self.findPred(key)
        self.logger.debug(node)
        if node != self.node:
            return self._client_succ(node)
        else:
            self.logger.debug("returning self")
            return self.getNodeSucc()

    def findPred(self, key):
        """
        Find predecessor node of key
        :param key: SHA256 hash key of a file
        :return node: Predecessor NodeID object
        """
        node = self
        if self.finger_table:
            self.logger.debug("key %s" % key)
            self.logger.debug("id %s" % node.node.id)

            # pdb.set_trace()
            if not self._in_range(node.node.id, node.finger_table[1].id, key):
                for i in xrange(256, 0, -1):
                    fing_node = node.finger_table[i]
                    if self._in_range(node.node.id, key, fing_node.id, True):
                        succ_node = self._client_succ(node.finger_table[i])
                        if self._in_range(fing_node.id, succ_node.id, key):
                            return node.finger_table[i]
                        else:
                            self.logger.debug("calling findsucc")
                            return self._client_findpred(fing_node, key)
            return node.node
        else:
            raise ttypes.SystemException("No finger table exist for current node.")

    def getNodeSucc(self):
        """
        Get immediate successor of node
        :return node: immediate successor NodeID object of current node
        """
        self.logger.debug(sys.argv[1])
        if self.finger_table[1]:
            return self.finger_table[1]
        else:
            raise ttypes.SystemException("No such node in finger table.")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: ./server <port-number>")
        sys.exit()
    try:
        handler = ChordHandler()
        processor = FileStore.Processor(handler)
        transport = TSocket.TServerSocket(port=sys.argv[1])
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

        print("Starting server at %s:%s" % (handler.node.ip, handler.node.port))
        server.serve()
    except KeyboardInterrupt:
        sys.exit()
    except Exception as e:
        print(e)
    finally:
        for meta in handler.file_table.values():
            print(meta.filename)
            if os.path.exists(meta.filename):
                os.remove(meta.filename)
