#! /usr/bin/env python

"""
Controller
"""

import pickle
import socket
import sys

import bank_pb2


def init_branch(message):
    active_branches = []
    all_branches = message.init_branch.all_branches
    for branch in all_branches:
        soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("connecting to port %d" % branch.port)
        soc.connect((branch.ip, branch.port))
        soc.send(message.SerializeToString())
        req = soc.recv(1024)
        if len(active_branches) != 0 and req == "active_branches":
            soc.send(pickle.dumps(active_branches))
        active_branches.append((branch.name, branch.ip, branch.port))
        soc.close()


if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Usage: python branch.py <total_balance> <branches>")
        sys.exit(1)

    total_balance = int(sys.argv[1])
    # all_branches = {}
    message = bank_pb2.BranchMessage()
    branches = message.init_branch
    with open(sys.argv[2]) as fin:
        for line in fin.readlines():
            name, ip, port = line.strip('\n').split()
            branch = branches.all_branches.add()
            branch.name, branch.ip, branch.port = name, ip, int(port)
            # all_branches[name] = (ip, int(port))
    branches.balance = total_balance/len(branches.all_branches)

    init_branch(message)
    # msg = message.SerializeToString()
    # m = bank_pb2.BranchMessage()
    # m.ParseFromString(msg)
    # print(m)
    # print(message.init_branch.all_branches[0])
    # print(type(message.init_branch.all_branches[0]))
    # exit()
