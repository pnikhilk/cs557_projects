#! /usr/bin/python
import hashlib
import logging
import pdb
import socket
import sys
sys.path.append("lib/gen-py/chord")

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import FileStore
import ttypes


class ChordHandler:

    logging.basicConfig(level=logging.DEBUG)

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        sha256 = hashlib.sha256()
        ip = socket.gethostbyname(socket.gethostname())
        ip_port = ip + ":" + sys.argv[1]
        self.logger.debug("ip_port %s" % ip_port)
        sha256.update(ip_port)
        self.node = ttypes.NodeID(sha256.hexdigest(), ip, sys.argv[1])
        self.finger_table = {}

    @staticmethod
    def _in_range(id1, id2, key):
        """
        Find if key belongs to range (id1, id2)
        """
        init_node = "0" * 64
        if (key > id1 or key > init_node) and (key < id2):
            return True
        else:
            return False

    def writeFile(self, rFile):
        pass

    def readFile(self, rFile):
        pass

    def setFingertable(self, node_list):
        """
        Set finger table for current server node.
        :param node_list: List containing NodeID objects
        """
        for i, node in enumerate(node_list, start=1):
            self.finger_table[i] = node

        # for k, v in self.finger_table.items():
        #     self.logger.debug(k,v)

    def findSucc(self, key):
        """
        Find successor node of key
        :param key: SHA256 hash key of a file
        """
        node = self.findPred(key)
        self.logger.debug(node)
        if node != self.node:
            socket = TSocket.TSocket(node.ip, node.port)
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = FileStore.Client(protocol)
            transport.open()
            return client.getNodeSucc()
        else:
            return self.getNodeSucc()

    def findPred(self, key):
        """
        Find predecessor node of key
        :param key: SHA256 hash key of a file
        """
        node = self
        self.logger.debug("in pred")
        self.logger.debug("ke %s" % key)
        self.logger.debug("id %s" % node.node.id)
        self.logger.debug("fin %s" % node.finger_table[1].id)
        # pdb.set_trace()
        while not self._in_range(node.node.id, node.finger_table[1].id, key):
            self.logger.debug('in while')
            for i in xrange(256, 0, -1):
                self.logger.debug("id %s" % node.node.id)
                self.logger.debug("fin %s" % node.finger_table[i].id)
                if self._in_range(node.node.id, key, node.finger_table[i].id):
                    return node.finger_table[i]
        return node.node

    def getNodeSucc(self):
        """
        Get immediate successor of node
        """
        self.logger.debug(sys.argv[1])
        self.logger.debug(self.finger_table[1])
        return self.finger_table[1]


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: ./server <port-number>")
        sys.exit()
    handler = ChordHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=sys.argv[1])
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print("Starting server...")
    server.serve()
