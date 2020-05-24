from typing import NamedTuple
from typing import List, TypeVar

LogEntryType = TypeVar('LogEntry')


class LogEntry(NamedTuple):
    Command: str
    Term: int


class RequestVote(NamedTuple):
    Term: int
    CandidateId: str
    LastLogIndex: int
    LastLogTerm: int


class RequestVoteResponse(NamedTuple):
    Term: int
    VoteGranted: bool


class AppendEntry(NamedTuple):
    Term: int
    LeaderId: str
    PrevLogIndex: int
    PrevLogTerm: int
    Entries: List[LogEntryType]
    LeaderCommit: int


class AppendEntryResponse(NamedTuple):
    Term: int
    Success: bool
