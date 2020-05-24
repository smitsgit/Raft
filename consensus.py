"""
This class implements the consensus module used in Raft Algorithm

A key building block of the Raft algorithm is the election timer.
This is the timer every follower runs continuously,
restarting it every time it hears from the current leader.

The leader is sending periodic heartbeats, so when these stop arriving a follower assumes that the
leader has crashed or got disconnected,and starts an election (switches to the Candidate state).

If enough time has passed since the last "election reset event", this peer starts an election and becomes a candidate.
What is this election reset event? It's any of the things that can terminate an election - for example,
a valid heartbeat was received, or a vote given to another candidate. We'll see this code shortly.

"""
from threading import Thread, current_thread

from enum import Enum
from collections import defaultdict
import time
import random
import concurrent.futures

from config import cluster_info_by_name
from rpcmodule import RPCProxy
from Models import RequestVote, RequestVoteResponse, AppendEntryResponse, AppendEntry


class ConsensusState(Enum):
    CANDIDATE = 0
    FOLLOWER = 1
    LEADER = 2


state_to_name = {
    0: "CANDIDATE",
    1: "FOLLOWER",
    2: "LEADER"
}


class Consensus:

    def __init__(self):
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.state = ConsensusState.FOLLOWER
        self.election_reset_event_time = time.time()
        self.elction_timeout = self.get_timeout()
        self.peers = {}
        self.election_counter = 0

        # Volatile state in all raft servers
        self.commit_index = 0  # index of highest log entry known to be committed (initialized to 0, increases monotonically )
        self.lastApplied = 0  # index of highest log entry applied to state machine (initialized to 0, increases monotonically)

        # Volatile state in all Leaders

        # for each server, index of the next log entry to send to that server (initialized to leader
        # last log index + 1)
        self.nextIndex = defaultdict(int)

        # for each server, index of highest log entry known to be replicated on server
        # (initialized to 0, increases monotonically)
        self.matchIndex = defaultdict(int)

    def set_server(self, raft_server):
        self.raft_server = raft_server

    def run_election_timer(self):
        t = Thread(name=f"Election Timer-{self.election_counter}", target=self.handle_election,
                   args=(self.current_term,))
        t.daemon = True
        t.start()
        self.election_counter = self.election_counter + 1

    def start_election(self):
        self.become_candidate()
        print(f"{self.raft_server.name} : Becomes candidate with term = {self.current_term}")
        self.request_vote_from_peers()

    def become_candidate(self):
        self.state = ConsensusState.CANDIDATE
        self.current_term = self.current_term + 1
        self.election_reset_event_time = time.time()
        self.voted_for = self.raft_server.name

    def become_leader(self):
        self.state = ConsensusState.LEADER
        print(f"{self.raft_server.name} : Becomes leader with term {self.current_term}")
        self.start_heart_beats()

    def become_follower(self, leader_term):
        self.state = ConsensusState.FOLLOWER
        self.current_term = leader_term
        self.election_reset_event_time = time.time()
        self.voted_for = None
        self.run_election_timer()

    def handle_request_vote_response(self, request_vote):
        print(f"{self.raft_server.name} : handling Response for the request {request_vote}")
        if request_vote.Term > self.current_term:
            print(f"{self.raft_server.name} : Someone has greater term than me : Let me better be follower")
            self.become_follower(request_vote.Term)

        if self.current_term == request_vote.Term and \
                (self.voted_for is None or self.voted_for == request_vote.CandidateId):
            reply = RequestVoteResponse(Term=self.current_term, VoteGranted=True)
            self.voted_for = request_vote.CandidateId
            self.election_reset_event_time = time.time()
        else:
            reply = RequestVoteResponse(Term=self.current_term, VoteGranted=False)

        print(f"{self.raft_server.name} : The response is {reply}")
        return reply

    def handle_append_entry_response(self, request_append_entry):
        print(f"{self.raft_server.name} : handling Response for the request {request_append_entry}")
        if request_append_entry.Term > self.current_term:
            print(f"{self.raft_server.name} : Term out of date in Heartbeat reply")
            self.become_follower(request_append_entry.Term)

        if request_append_entry.Term == self.current_term:
            if self.state != ConsensusState.FOLLOWER:
                self.become_follower(request_append_entry.Term)

            self.election_reset_event_time = time.time()
            data = AppendEntryResponse(Term=self.current_term, Success=True)
        else:
            data = AppendEntryResponse(Term=self.current_term, Success=False)

        return data

    def handle_election(self, term_started):
        print(f"{self.raft_server.name} : {current_thread().name} started with Term = {self.current_term} "
              f"and timeout = {self.elction_timeout}s")

        while True:
            time.sleep(self.get_check_interval())
            if self.state != ConsensusState.CANDIDATE and \
                    self.state != ConsensusState.FOLLOWER:
                print(f"{self.raft_server.name} : In LEADER state, So supposed to send heartbeats")
                return

            if term_started != self.current_term:
                print(
                    f"{self.raft_server.name} : {current_thread().name} : In election timer, term changed from {term_started} to {self.current_term}")
                print(f"{self.raft_server.name} : This is plain wrong, bailing out...")
                return

            elapsed = time.time()

            if elapsed - self.election_reset_event_time > self.elction_timeout:
                self.start_election()
                return

    @staticmethod
    def get_timeout():
        return 10 + random.randint(0, 5)
        # return (150 + random.randint(0, 150)) / 1000

    @staticmethod
    def get_check_interval():
        return 1
        # return 0.01

    def do_rpc_request(self, remote_server, address):
        if self.peers[remote_server] is None:
            print(f"{self.raft_server.name} : trying to connect to {remote_server} at {address}")
            remote = RPCProxy(cluster_info_by_name[remote_server], authkey=b"peekaboo", timeout=2)
            print(f"{self.raft_server.name} : successfully connected to {remote_server}")
        else:
            remote = self.peers[remote_server]
        data = RequestVote(Term=self.current_term, CandidateId=self.raft_server.name, LastLogIndex=0, LastLogTerm=0)
        result = remote.request_vote(data)
        return result

    def request_vote_from_peers(self):
        if self.state != ConsensusState.CANDIDATE:
            return
        votes_received = 1
        peers = {server: address for server, address in cluster_info_by_name.items() if self.raft_server.name != server}

        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_server = {executor.submit(self.do_rpc_request, server, address): server for
                                server, address in peers.items()}
            for future in concurrent.futures.as_completed(future_to_server):
                server = future_to_server[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print(f"{self.raft_server.name} : [ {server} ] generated an exception {exc}")
                else:
                    print(f"{self.raft_server.name} : Result from {server} of the Request Vote response {data}")
                    if self.state != ConsensusState.CANDIDATE:
                        print(
                            f"{self.raft_server.name} : while waiting for the reply state changed {state_to_name[self.state]}")
                        return

                    if data.VoteGranted:
                        votes_received = votes_received + 1

        print(f"{self.raft_server.name} : For a current term {self.current_term} the votes received {votes_received}")
        nodes_that_can_fail = (len(cluster_info_by_name) - 1) / 2
        if votes_received > nodes_that_can_fail:
            print(f"{self.raft_server.name} : Got majority of votes .. Lets roll and become leader")
            self.become_leader()

        # Run timer in case of split votes
        self.run_election_timer()

    def heart_beat_handler(self):
        while True:
            time.sleep(1)
            if self.state != ConsensusState.LEADER:
                return

            self.send_heart_beat()

    def start_heart_beats(self):
        Thread(name="heart_beat_handler", target=self.heart_beat_handler, daemon=True).start()

    def do_heartbeat_rpc_request(self, remote_server, address):
        if self.peers[remote_server] is None:
            print(f"{self.raft_server.name} : trying to connect to {remote_server} at {address}")
            remote = RPCProxy(cluster_info_by_name[remote_server], authkey=b"peekaboo", timeout=2)
            print(f"{self.raft_server.name} : successfully connected to {remote_server}")
        else:
            remote = self.peers[remote_server]
        data = AppendEntry(Term=self.current_term, LeaderId=self.raft_server.name,
                           PrevLogIndex=0, PrevLogTerm=0, Entries=None, LeaderCommit=0)
        result = remote.append_entries(data)
        return result

    def send_heart_beat(self):
        peers = {server: address for server, address in cluster_info_by_name.items() if self.raft_server.name != server}

        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_server = {executor.submit(self.do_heartbeat_rpc_request, server, address): server for
                                server, address in peers.items()}
            for future in concurrent.futures.as_completed(future_to_server):
                server = future_to_server[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print(f"{self.raft_server.name} : [ {server} ] generated an exception {exc}")
                else:
                    print(f"{self.raft_server.name} : Result from {server} of the Heart Beat response {data}")
                    if data.Term > self.current_term:
                        print(f"{self.raft_server.name} : Term out of date in Heartbeat reply")
                        self.become_follower(data.Term)
                        return
