"""
This is an attempt to implement Raft distributed systems Consensus Algorithm

This server should be able to handle RPC.
#1 vote
#2 Append LOG

This server should also be able to handle the state
#1 Leader
#2 Candidate
#3 Follower

Should be able to handle cluster membership
Should be able to handle writing to Replication log
Should be able to handle client Request and finally be able to perform commit

Leader Election -
   Select one of the servers from the cluster to act as leader
   Detect crashes and elect a new leader.

Log Replication
   Leader takes commands from clients and appends them to its log
   Leader replicates its log to others ( Overwriting inconsistencies )

Safety
   Only one server with an up to date log can become leader


Term - Phrase used to identify new leaders vs old leaders
[  A Term is the period of time for which a certain server is a leader ]

In Leader election, the server which times out first will increase its Term first

Every message thats sent in Raft, includes Sender's Term number

Normal Case Election:

Server first enters into candidate state and they are kind of greedy and hence they vote
for themselves first.

Once the other server Receieve the RPC messages with modified Term from wannabe Leader,
they update their Term and respond with RPC Vote
Servers allowed to vote at most once per term

Once the wannabe Leader server reveives the majority votes, it becomes Leader and
its first job is to send heart-beats

======================================================================================

In case of split votes when there is no majority when two or more wannabe
server nominate themselves for the election, the algorithm just waits for another timeout
and the next wannabe leader which time out first will increment its Term and announce its
candidature for the election.

-----------------------------------------------------------------------------------------

Log Replication:

Normal case log replication:
============================
Leader can mark entry in its log committed once it reaches majority of the cluster.
Till the majority is achieved, the entries will remain in the Leaders log but will only
be der in the memory

The term state machine is used to represent arbitrary service.
Afterall state machines are one of the foundational blocks of computer science and
everything can be represented by them.
example : Databases , file servers , lock servers.

"""

from multiprocessing.connection import Listener
from threading import Thread
from rpcmodule import RPCServer, RPCProxy
from consensus import Consensus
from collections import defaultdict
from interface import implements
from connectionlistener import ConnectionListner
from Models import RequestVote, RequestVoteResponse
from config import cluster_info_by_name, cluster_info_by_port

import sys
import time


class RaftServer(implements(ConnectionListner)):
    def __init__(self, name, rpcServer, consensusModule):
        self.id = None
        self.name = name
        self.consensusModule = consensusModule
        self.rpcServer = rpcServer
        self.peerinfo = []

    def start_rpc(self):
        print(f"{self.name} : listening on {self.rpcServer.address}")
        for item in [self.request_vote, self.append_entries]:
            self.rpcServer.register_function(item)
        self.rpcServer.register_listener(self)
        t = Thread(name="Rpcthread", target=self.rpcServer.serve_forever)
        t.daemon = True
        t.start()

    def connected(self, name, client):
        # print(f"{self._name} : received connection from {cluster_info_by_name[name]}")
        # self.peerinfo.append((name, client))
        pass

    def disconnected(self, name, client):
        # self.peerinfo.remove((name, client))
        pass

    def connect_rpc_clients(self):
        while True:
            for server, address in cluster_info_by_name.items():
                if server != self.name:
                    if self.consensusModule.peers.get(server, None) is None:
                        print(f"{self.name} : trying to connect to {server} at {address}")
                        try:
                            remote = RPCProxy(cluster_info_by_name[server], authkey=b"peekaboo", timeout=2)
                        except Exception as e:
                            print(f"{self.name} : [ {server} ] generated an exception: {e}")
                        else:
                            self.consensusModule.peers[server] = remote

            time.sleep(1)

    def connect_all_peers(self):
        Thread(name="PeerThread",target=self.connect_rpc_clients, daemon=True).start()

    def request_vote(self, RequestVote):
        print(f"{self.name} : Vote Request  : Received vote request from Candidate {RequestVote}")
        return self.consensusModule.handle_request_vote_response(RequestVote)

    def append_entries(self, AppendEntry):
        print(f"{self.name} : AppendEntry Request  : Received appendEntry request from Candidate {AppendEntry}")
        return self.consensusModule.handle_append_entry_response(AppendEntry)


def main():
    server_name = f"server{sys.argv[1]}"
    print(f"Starting as : {server_name}")
    rpcServer = RPCServer(address=cluster_info_by_name[server_name], authkey=b"peekaboo")
    consensusModule = Consensus()
    raft_server = RaftServer(name=server_name, rpcServer=rpcServer, consensusModule=consensusModule)
    raft_server.start_rpc()
    raft_server.connect_all_peers()
    print(f"{server_name} : Attempting to connect to peers")
    consensusModule.set_server(raft_server)
    consensusModule.run_election_timer()
    time.sleep(100)


if __name__ == '__main__':
    main()
