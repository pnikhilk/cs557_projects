#! /usr/bin/env python

"""
Distributed bank branch
"""

import collections
import logging
import pickle
import random
import socket
import sys
import threading
import time

sys.path.append('/home/phao3/protobuf/protobuf-3.4.0/python')

import bank_pb2
from google.protobuf.message import DecodeError


class Snapshot(object):
    """
    Object to store snapshot information
    """
    def __init__(self, snapshot_id, balance):
        self.snapshot_id = snapshot_id
        self.balance = balance
        self.channels = collections.OrderedDict()
        self.recording_channels = []

    def __str__(self):
        return "%d, balance = %d\n channels:%s" % (self.snapshot_id,
                                                   self.balance,
                                                   str(self. channels))


class Branch():
    logging.basicConfig(level=logging.ERROR)
    logger = logging.getLogger(__name__)
    threads = []

    def __init__(self, branch_name, ip, port):
        self.name = branch_name
        self.ip = ip
        self.port = int(port)
        self.branch = bank_pb2.BranchMessage()
        self.balance_lock = threading.Lock()
        self.message_lock = threading.Lock()
        self.min_balance = 0
        self.max_balance = 0
        self.transfer_thread = None
        self.sockets = collections.OrderedDict()
        self.snapshot_info = collections.OrderedDict()
        self.controller_socket = None

    def _init_connections(self, active_branches=None):
        """
        Initialize branch connections and start transfer thread
        :param active_branches:
            List of previously active branches to setup single TCP b2b connection
        """
        if type(active_branches) is list:
            for branch, ip, port in active_branches:
                self.logger.debug("connecting to %d" % port)
                branch_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                branch_socket.connect((ip, port))
                branch_port = branch_socket.getsockname()[1]
                branch_socket.send("branch_connection,%s,%s,%d\0" %
                                   (self.name, self.ip, branch_port))
                self.sockets[branch] = branch_socket
                thread_args = (branch_socket, self.ip)
                branch_thread = threading.Thread(target=self.listen,
                                                 args=thread_args)
                self.threads.append(branch_thread)
                branch_thread.start()
        if self.sockets:
            self.transfer_thread = threading.Thread(target=self._transfer)
            self.transfer_thread.start()

    def _close_connections(self):
        """
        Close all the created socket connections
        """
        for socket_conn in self.sockets.values():
            socket_conn.close()

    def _get_sender(self, branch_socket):
        """
        Get owner branch of socket
        :param branch_socket:
            Socket object of a branch
        :return
            Name of the branch
        """
        for key, val in self.sockets.items():
            if val == branch_socket:
                return key

    def init_branch(self, branch_message):
        """
        Set initial balance and min/max transfer limit
        :param branch_message
            Object of bank_pb2.BranchMessage
        """
        self.branch = branch_message.init_branch
        self.min_balance = int(self.branch.balance * 0.01)
        self.max_balance = int(self.branch.balance * 0.05)

    def _transfer(self):
        """
        Select random branch and random amount to transfer
        """
        while True:
            remote_branch = None
            time.sleep(random.randint(0, 5))
            while True:
                remote_branch = random.choice(self.branch.all_branches)
                if remote_branch.name != self.name and \
                        remote_branch.name in self.sockets:
                    break
            money = random.randint(self.min_balance, self.max_balance)
            self.transfer(money, remote_branch)

    def transfer(self, money, remote_branch):
        """
        If amount is valid, transfer to remote branch
        :param money:
            Amount to transfer
        :param remote_branch:
            Object of Branch
        """
        if self.branch.balance - money > 0:
            branch_socket = self.sockets[remote_branch.name]
            transfer_message = bank_pb2.BranchMessage()
            transfer = transfer_message.transfer
            transfer.money = money
            self.logger.info("transferring %d to %s" % (money, remote_branch.name))
            with self.balance_lock:
                self.branch.balance -= money
                branch_socket.send(transfer_message.SerializeToString() + '\0')

    def credit(self, sender, money):
        """
        On receiving amount, add money to branch balance
        :param sender:
            Sender branch name
        :param money:
            Amount transferred
        """
        with self.balance_lock:
            for snapshot in self.snapshot_info.values():
                if len(snapshot.recording_channels) != 0 and\
                        sender in snapshot.recording_channels:
                    snapshot.channels[sender] += money

            self.branch.balance += money
            self.logger.info("credited %d, new balance %d" % (money, self.branch.balance))

    def init_snapshot(self, branch_message):
        """
        Initialize snapshot on branch
        :param branch_message:
            Object of InitSnapshot
        """
        snapshot_id = branch_message.init_snapshot.snapshot_id
        self.logger.debug("init_snapshot id:%d" % snapshot_id)
        marker_message = bank_pb2.BranchMessage()
        marker_message.marker.snapshot_id = snapshot_id
        with self.balance_lock:
            snapshot = Snapshot(snapshot_id, self.branch.balance)
            for branch, branch_socket in self.sockets.items():
                snapshot.recording_channels.append(branch)
                snapshot.channels[branch] = 0
                branch_socket.send(marker_message.SerializeToString() + '\0')
        self.snapshot_info[snapshot_id] = snapshot

    def process_marker(self, sender, marker_message):
        """
        On receiving marker message, record state and send marker to other branches
        :param sender:
            Sender branch of marker message
        :param marker_message:
            Object of MarkerMessage
        """
        snapshot_id = marker_message.marker.snapshot_id
        self.logger.debug("marker from %s, sid:%d" % (sender, snapshot_id))
        if snapshot_id not in self.snapshot_info:
            self.logger.debug("first marker of snapshot id %d" % snapshot_id)
            snapshot = Snapshot(snapshot_id, self.branch.balance)
            with self.balance_lock:
                for key, branch_socket in self.sockets.items():
                    if key == sender:
                        snapshot.channels[key] = 0
                    elif key not in snapshot.channels:
                        self.logger.debug("id:%d, recording %s " % (snapshot_id, key))
                        snapshot.recording_channels.append(key)
                        snapshot.channels[key] = 0
                    branch_socket.send(marker_message.SerializeToString() + '\0')
                self.snapshot_info[snapshot_id] = snapshot
        else:
            snapshot = self.snapshot_info[snapshot_id]
            if sender in snapshot.recording_channels:
                snapshot.recording_channels.remove(sender)

    def return_snapshot(self, branch_message):
        """
        On receiving retrieve_snapshot message send local snapshot to controller
        :param branch_message:
            Object of RetriveMessage
        """
        snapshot_id = branch_message.retrieve_snapshot.snapshot_id
        try:
            snapshot = self.snapshot_info[snapshot_id]
        except KeyError:
            while snapshot_id not in self.snapshot_info:
                # wait  for processing snapshot
                time.sleep(2)

            snapshot = self.snapshot_info[snapshot_id]
        branch_message = bank_pb2.BranchMessage()
        return_snapshot_message = branch_message.return_snapshot
        local_snapshot = return_snapshot_message.local_snapshot
        local_snapshot.snapshot_id = snapshot_id
        local_snapshot.balance = snapshot.balance
        for branch_channel_state in snapshot.channels.values():
            local_snapshot.channel_state.append(branch_channel_state)
        self.controller_socket.send(branch_message.SerializeToString())

    def listen(self, client_socket, client_ip):
        """
        Receive messages on each socket infinitely
        :param client_socket:
            Socket object of a branch
        :param client_ip:
            Tuple indicating ip and port of client_socket
        """
        while True:
            msg = client_socket.recv(1024)
            if not msg:
                break
            for message in msg.split('\0'):
                self._parse_message(message, client_socket, client_ip)

    def setup_branch_connection(self, message, client_ip, client_socket):
        """
        Setup branch to branch connection
        :param message:
            Information about active branches
        :param client_ip:
            Tuple indicating ip and port of client_socket
        :param client_socket:
            Socket object of a branch
        """
        fields = message.split(',')
        with self.message_lock:
            if client_ip[1] == int(fields[3]):
                self.sockets[fields[1]] = client_socket
                if not self.transfer_thread:
                    self._init_connections([])

    def _parse_message(self, message, client_socket, client_ip):
        """
        Parse received messages from controller or other branches
        :param message:
            Message received by client_socket
        :param client_socket:
            Socket object of a branch
        :param client_ip:
            Tuple indicating ip and port of client_socket
        """
        try:
            branch_message = bank_pb2.BranchMessage()
            branch_message.ParseFromString(message)
            message_type = branch_message.WhichOneof("branch_message")
            active_branches = []
            sender = self._get_sender(client_socket)
            if message_type == "init_branch":
                self.controller_socket = client_socket
                self.init_branch(branch_message)
                client_socket.send("active_branches")
                message = client_socket.recv(1024)
                if message:
                    active_branches = pickle.loads(message)
                    self._init_connections(active_branches)
            elif message_type == "transfer":
                with self.message_lock:
                    self.credit(sender, branch_message.transfer.money)
            elif message_type == "init_snapshot":
                with self.message_lock:
                    self.init_snapshot(branch_message)
            elif message_type == "marker":
                with self.message_lock:
                    self.process_marker(sender, branch_message)
            elif message_type == "retrieve_snapshot":
                self.return_snapshot(branch_message)
            return message_type
        except DecodeError:
            if "branch_connection" in message:
                self.setup_branch_connection(message, client_ip, client_socket)
        except Exception as e:
            self.logger.error(e)

    def start_branch(self):
        """
        Start current branch as server and accept connections
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.ip, self.port))
            self.ip = socket.gethostbyname(socket.gethostname())
            self.port = sock.getsockname()[1]
            print("Branch %s started at %s:%s." % (self.name, self.ip,
                                                   self.port))
            sock.listen(5)
            while True:
                client_socket, client_ip = sock.accept()
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
            self.stop_execution()
            sock.close()

    def stop_execution(self):
        """
        On Interrupt or exception, close all connections and stop all threads
        """
        try:
            self.logger.debug("closing connections")
            self._close_connections()
            for thread in self.threads:
                thread._Thread__stop()
                thread.join()
            self.logger.debug("threads joined")
            if self.transfer_thread and self.transfer_thread.isAlive():
                self.transfer_thread._Thread__stop()
                self.transfer_thread.join()
        except:
            sys.exit(1)


if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Usage: python branch.py <branch_name> <port>")
        sys.exit(1)

    current_branch = Branch(sys.argv[1], "", sys.argv[2])
    current_branch.start_branch()
