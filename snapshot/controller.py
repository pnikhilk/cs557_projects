#! /usr/bin/env python

"""
Controller
"""

import collections
import pickle
import random
import socket
import sys
import time

sys.path.append('/home/phao3/protobuf/protobuf-3.4.0/python')

import bank_pb2

sockets = collections.OrderedDict()
branch_info = collections.OrderedDict()


def init_branch(message):
    """
    Send InitBranch message to all branches
    :param message:
        Object of BranchMessage.InitBranch
    """
    active_branches = []
    all_branches = message.init_branch.all_branches
    for branch in all_branches:
        soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sockets[branch.name] = soc
        soc.connect((branch.ip, branch.port))
        soc.send(message.SerializeToString() + '\0')
        req = soc.recv(1024)
        if len(active_branches) != 0 and req == "active_branches":
            soc.send(pickle.dumps(active_branches) + '\0')
        else:
            soc.send(pickle.dumps([]) + '\0')
        active_branches.append((branch.name, branch.ip, branch.port))


def init_snapshot(all_branches):
    """
    Send InitSnapshot to random branch
    :param all_branches:
        List of all branches
    """
    snapshot_id = 1
    while True:
        message = bank_pb2.BranchMessage()
        snapshot = message.init_snapshot
        snapshot.snapshot_id = snapshot_id
        remote_branch = random.choice(all_branches)
        remote_socket = sockets[remote_branch.name]
        remote_socket.send(message.SerializeToString() + '\0')
        retrieve_snapshot(all_branches, snapshot_id)
        # if snapshot_id == 20:
        #     break
        snapshot_id += 1
        time.sleep(2)
    # for i in range(1, 21):
    #     retrieve_snapshot(all_branches, i)


def retrieve_snapshot(all_branches, snapshot_id):
    """
    Send RetrieveSnapshot message to all branches
    :param all_branches:
        List of all branches
    :param snapshot_id:
        Snapshot id
    """
    message = bank_pb2.BranchMessage()
    snapshot = message.retrieve_snapshot
    snapshot.snapshot_id = snapshot_id
    total_balance = 0
    print("snapshot_id: %d" % snapshot_id)
    for branch in all_branches:
        soc = sockets[branch.name]
        soc.send(message.SerializeToString() + '\0')
        ret = soc.recv(1024)
        snapshot_message = bank_pb2.BranchMessage()
        try:
            snapshot_message.ParseFromString(ret)
            balance, channel_state = parse_snapshot(snapshot_message,
                                                    branch.name)
            print("%s: %4d, %s" % (branch.name, balance, channel_state))
            total_balance += balance
        except Exception as e:
            print("error %s" % e)
    # print("total_balance = %d" % total_balance)


def parse_snapshot(message, branch_name):
    """
    Parse ReturnSnapshot message
    :param message:
        Object of BranchMessage.ReturnSnapshot
    :param branch_name:
        Name of local branch
    """
    local_snapshot = message.return_snapshot.local_snapshot
    balance = local_snapshot.balance
    channel_state = ""
    for i, bal in enumerate(local_snapshot.channel_state):
        state = branch_info[branch_name][i] + "->" + branch_name
        channel_state = channel_state + state + ": " + "{0:2d}".format(bal) + ", "
    return balance, str(channel_state).strip(', ')


if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Usage: python branch.py <total_balance> <branches>")
        sys.exit(1)

    total_balance = int(sys.argv[1])
    # all_branches = {}
    message = bank_pb2.BranchMessage()
    branches = message.init_branch
    all_branches = []
    with open(sys.argv[2]) as fin:
        for line in fin.readlines():
            name, ip, port = line.strip('\n').split()
            branch = branches.all_branches.add()
            branch.name, branch.ip, branch.port = name, ip, int(port)
            all_branches.append(name)

    for branch_name in all_branches:
        branch_info[branch_name] = [x for x in all_branches if x != branch_name]
    branches.balance = total_balance/len(branches.all_branches)
    all_branches = branches.all_branches
    try:
        init_branch(message)
        time.sleep(2)
        init_snapshot(all_branches)
    except KeyboardInterrupt:
        sys.exit(0)
    except socket.error:
        pass
    except Exception as e:
        print("ERROR: %s" % e)
