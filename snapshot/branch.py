#! /usr/bin/env python

"""
Distributed bank branch
"""

import logging
# import pdb
import pickle
import random
import socket
import sys
import threading
import time

import bank_pb2


class Branch():
    logging.basicConfig(level=logging.INFO)
    threads = []

    def __init__(self, branch_name, ip, port):
        self.name = branch_name
        self.ip = ip
        self.port = int(port)
        self.branch = bank_pb2.BranchMessage()
        self.lock = threading.Lock()
        self.min_balance = 0
        self.max_balance = 0
        self.transfer_thread = None
        self.sockets = {}
        self.logger = logging.getLogger(__name__)

    def _init_connections(self, active_branches=None):
        if active_branches:
            for branch, ip, port in active_branches:
                self.logger.debug("connecting to %d" % port)
                branch_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                branch_socket.connect((ip, port))
                branch_port = branch_socket.getsockname()[1]
                branch_socket.send("branch_connection,%s,%s,%d" %
                                   (self.name, self.ip, branch_port))
                self.sockets[branch] = branch_socket
                thread_args = (branch_socket, self.ip)
                branch_thread = threading.Thread(target=self.listen,
                                                 args=thread_args)
                self.threads.append(branch_thread)
                branch_thread.start()

        self.transfer_thread = threading.Thread(target=self._transfer)
        self.transfer_thread.start()

    def _close_connections(self):
        for socket_conn in self.sockets.values():
            socket_conn.close()

    def init_branch(self, branch_message):
        self.branch = branch_message.init_branch
        self.min_balance = self.branch.balance * 0.01
        self.max_balance = self.branch.balance * 0.05

    def transfer(self, money, remote_branch):
        if self.branch.balance - money > 0:
            branch_socket = self.sockets[remote_branch.name]
            self.logger.info("transferring %d" % money)
            transfer_message = bank_pb2.BranchMessage()
            transfer = transfer_message.transfer
            transfer.money = money
            with self.lock:
                self.branch.balance -= money
                branch_socket.send(transfer_message.SerializeToString())

    def credit(self, money):
        with self.lock:
            self.branch.balance += money
        self.logger.info("credited %d" % money)

    def _transfer(self):
        while True:
            remote_branch = None
            while True:
                remote_branch = random.choice(self.branch.all_branches)
                if remote_branch.name != self.name:
                    break
            money = random.randint(self.min_balance, self.max_balance)
            self.logger.debug("random %d" % money)
            self.transfer(money, remote_branch)
            time.sleep(random.randint(0, 5))

    def listen(self, client_socket, client_ip):
        while True:
            msg = client_socket.recv(1024)
            # self.logger.debug("msg %s" % msg)
            if not msg:
                break
            self._parse_message(msg, client_socket, client_ip)

    def _parse_message(self, message, client_socket, client_ip):
        try:
            branch_message = bank_pb2.BranchMessage()
            branch_message.ParseFromString(message)
            message_type = branch_message.WhichOneof("branch_message")
            active_branches = []
            if message_type == "init_branch":
                self.init_branch(branch_message)
                client_socket.send("active_branches")
                message = client_socket.recv(1024)
                if message:
                    active_branches = pickle.loads(message)
                    self._init_connections(active_branches)
            elif message_type == "transfer":
                self.credit(branch_message.transfer.money)
            return message_type
        except Exception:
            if "branch_connection" in message:
                fields = message.split(',')
                if client_ip[1] == int(fields[3]):
                    self.sockets[fields[1]] = client_socket
                    if not self.transfer_thread:
                        self._init_connections()

    def start_branch(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind((self.ip, self.port))
            self.ip = socket.gethostbyaddr(socket.gethostname())[0]
            self.port = sock.getsockname()[1]
            sock.listen(5)
            while True:
                client_socket, client_ip = sock.accept()
                self.logger.debug("request from %s" % str(client_ip))
                thread_args = (client_socket, client_ip)
                client_thread = threading.Thread(target=self.listen,
                                                 args=thread_args)
                self.threads.append(client_thread)
                client_thread.start()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.debug("closing connections")
            self._close_connections()
            for thread in self.threads:
                thread._Thread__stop()
                thread.join()
            self.logger.debug("threads joined")
            if self.transfer_thread and self.transfer_thread.isAlive():
                self.logger.debug("stopping thread")
                self.transfer_thread._Thread__stop()
                self.transfer_thread.join()
            print("exit balance of branch %s = %d" % (self.name, self.branch.balance))
            sock.close()


if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Usage: python branch.py <branch_name> <port>")
        sys.exit(1)

    print("Branch %s started at port %s." % (sys.argv[1], sys.argv[2]))
    current_branch = Branch(sys.argv[1], "localhost", sys.argv[2])
    current_branch.start_branch()
