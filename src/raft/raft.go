package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const HeartbeatInterval = 60     // ms
const EletionTimeoutBase = 180   // ms
const EletionTimeoutRandom = 120 // ms

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm   int
	votedFor      int
	logs          []LogEntry
	snapshotTerm  int
	snapshotIndex int
	snapshot      []byte

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// other volatile auxiliary state
	state             State
	votesCount        int
	applyMsgCh        chan ApplyMsg
	heartbeatCh       chan bool
	grantVoteCh       chan bool
	downToFollowerCh  chan bool
	promoteToLeaderCh chan bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)

	if err := enc.Encode(rf.currentTerm); err != nil {
		panic(fmt.Sprintf("Failed to encode currentTerm. Error: %s", err))
	}

	if err := enc.Encode(rf.votedFor); err != nil {
		panic(fmt.Sprintf("Failed to encode votedFor. Error: %s", err))
	}

	if err := enc.Encode(rf.logs); err != nil {
		panic(fmt.Sprintf("Failed to encode logs. Error: %s", err))
	}

	if err := enc.Encode(rf.snapshotTerm); err != nil {
		panic(fmt.Sprintf("Failed to encode snapshotTerm. Error: %s", err))
	}

	if err := enc.Encode(rf.snapshotIndex); err != nil {
		panic(fmt.Sprintf("Failed to encode snapshotIndex. Error: %s", err))
	}

	if err := enc.Encode(rf.snapshot); err != nil {
		panic(fmt.Sprintf("Failed to encode snapshot. Error: %s", err))
	}

	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// bootstrap without any state?
	if data == nil || len(data) < 1 {
		return
	}

	// Your code here (2C).
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)

	var term int
	if err := dec.Decode(&term); err != nil {
		panic(fmt.Sprintf("Failed to decode currentTerm. Error: %s", err))
	}

	var votedFor int
	if err := dec.Decode(&votedFor); err != nil {
		panic(fmt.Sprintf("Failed to decode votedFor. Error: %s", err))
	}

	var logs []LogEntry
	if err := dec.Decode(&logs); err != nil {
		panic(fmt.Sprintf("Failed to decode logs. Error: %s", err))
	}

	var snapshotTerm int
	if err := dec.Decode(&snapshotTerm); err != nil {
		panic(fmt.Sprintf("Failed to decode snapshotTerm. Error: %s", err))
	}

	var snapshotIndex int
	if err := dec.Decode(&snapshotIndex); err != nil {
		panic(fmt.Sprintf("Failed to decode snapshotIndex. Error: %s", err))
	}

	var snapshot []byte
	if err := dec.Decode(&snapshot); err != nil {
		panic(fmt.Sprintf("Failed to decode snapshot. Error: %s", err))
	}

	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.logs = logs
	rf.snapshotTerm = snapshotTerm
	rf.snapshotIndex = snapshotIndex
	rf.snapshot = snapshot

	rf.applyLogs()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.downToFollower(args.Term)
	}

	reply.Term = args.Term
	reply.VoteGranted = false
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		if rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.sendToChannel(rf.grantVoteCh, true)
		}
	}
}

func (rf *Raft) isLogUpToDate(logLastTerm int, logLastIdx int) bool {
	rfLastTerm, rfLastIdx := rf.getLastLogTerm(), rf.getLastLogIndex()
	return logLastTerm > rfLastTerm || logLastTerm == rfLastTerm && logLastIdx >= rfLastIdx
}

func (rf *Raft) getLogEntry(logIndex int) LogEntry {
	if logIndex <= rf.snapshotIndex {
		panic(fmt.Sprintf("Input logIndex (%d) is less than snapshotIndex (%d)", logIndex, rf.snapshotIndex))
	}
	return rf.logs[logIndex-rf.snapshotIndex-1]
}

func (rf *Raft) getLogTerm(logIndex int) int {
	if logIndex <= rf.snapshotIndex {
		return rf.snapshotTerm
	}
	return rf.getLogEntry(logIndex).Term
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) + rf.snapshotIndex
}

func (rf *Raft) getLastLogTerm() int {
	return rf.getLogTerm(rf.getLastLogIndex())
}

func (rf *Raft) getMajority() int {
	return len(rf.peers)/2 + 1
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.downToFollower(reply.Term)
		rf.persist()
		return
	}

	if reply.VoteGranted {
		rf.votesCount++
		majority := rf.getMajority()
		if rf.votesCount == majority {
			rf.sendToChannel(rf.promoteToLeaderCh, true)
		}
	}
}

func (rf *Raft) broadcastRequestVotes() {
	if rf.state != Candidate {
		return
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// For reducing retry times when rejected
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied < rf.snapshotIndex {
		rf.applyMsgCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  rf.snapshotTerm,
			SnapshotIndex: rf.snapshotIndex,
		}
		rf.lastApplied = rf.snapshotIndex
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyMsgCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.getLogEntry(i).Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	// handle heart-beat
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = -1
		return
	}

	if args.Term > rf.currentTerm {
		rf.downToFollower(args.Term)
	}

	rf.sendToChannel(rf.heartbeatCh, true)

	reply.Term = args.Term
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	// handle normal append-entries
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		return
	}

	if rfTerm := rf.getLogTerm(args.PrevLogIndex); rfTerm != args.PrevLogTerm {
		reply.ConflictTerm = rfTerm
		for i := args.PrevLogIndex; i >= 0 && rf.getLogTerm(i) == rfTerm; i-- {
			reply.ConflictIndex = i
		}
		return
	}

	reply.Success = true
	logStartIdx := args.PrevLogIndex - rf.snapshotIndex
	diffIdx := findFirstDiffIndex(rf.logs[logStartIdx:], args.Entries)
	if diffIdx < len(args.Entries) {
		rf.logs = append(rf.logs[:logStartIdx+diffIdx], args.Entries[diffIdx:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}

	go rf.applyLogs()
}

func findFirstDiffIndex(src []LogEntry, dst []LogEntry) int {
	minLen := min(len(src), len(dst))
	for i := 0; i < minLen; i++ {
		if src[i].Term != dst[i].Term {
			return i
		}
	}
	return minLen
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.downToFollower(reply.Term)
		rf.persist()
		return
	}

	if reply.Success {
		matchIdx := args.PrevLogIndex + len(args.Entries)
		if matchIdx > rf.matchIndex[server] {
			rf.matchIndex[server] = matchIdx
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		// follower's log is shorter than leader's
		if reply.ConflictTerm < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			// try to find the conflict term in leader's log
			newNextIdx := rf.getLastLogIndex()
			for ; newNextIdx >= 0; newNextIdx-- {
				if rf.getLogTerm(newNextIdx) == reply.ConflictTerm {
					break
				}
			}

			// if not found, set nextIndex to conflictIndex
			if newNextIdx < 0 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				rf.nextIndex[server] = newNextIdx
			}
		}
		rf.makeAppendEntriesRPC(server)
	}

	majority := rf.getMajority()
	for n := rf.getLastLogIndex(); n >= rf.commitIndex; n-- {
		count := 1
		if rf.getLogTerm(n) == rf.currentTerm {
			for svr, matchIdx := range rf.matchIndex {
				if svr != rf.me && matchIdx >= n {
					count++
				}
			}
		}
		if count >= majority {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}
}

func (rf *Raft) broadcastAppendEntries(useLock bool) {
	if useLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			rf.makeAppendEntriesRPC(server)
		}
	}
}

func (rf *Raft) makeAppendEntriesRPC(server int) {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
	args.Entries = append(args.Entries, rf.logs[rf.nextIndex[server]-rf.snapshotIndex-1:]...)
	args.LeaderCommit = rf.commitIndex
	go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
}

func (rf *Raft) downToFollower(term int) {
	fromState := rf.state
	rf.currentTerm = term
	rf.votedFor = -1
	rf.votesCount = 0
	rf.state = Follower
	if fromState != Follower {
		rf.sendToChannel(rf.downToFollowerCh, true)
	}
}

func (rf *Raft) convertToCandidate(fromState State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != fromState {
		return
	}

	rf.resetChannels()
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.votesCount = 1
	rf.persist()

	rf.broadcastRequestVotes()
}

func (rf *Raft) convertToLeader(fromState State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != fromState {
		return
	}

	rf.resetNextAndMatchIndexArray()
	rf.resetChannels()
	rf.state = Leader

	rf.broadcastAppendEntries(false)
}

func (rf *Raft) sendToChannel(ch chan bool, val bool) {
	select {
	case ch <- val:
	default:
	}
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(EletionTimeoutBase + rand.Intn(EletionTimeoutRandom))
}

func (rf *Raft) resetChannels() {
	rf.heartbeatCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.downToFollowerCh = make(chan bool)
	rf.promoteToLeaderCh = make(chan bool)
}

func (rf *Raft) resetNextAndMatchIndexArray() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.getLastLogIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
		rf.matchIndex[i] = 0
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
	rf.persist()

	go rf.broadcastAppendEntries(true)

	return rf.getLastLogIndex(), rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			select {
			case <-rf.heartbeatCh:
			case <-rf.grantVoteCh:
			case <-time.After(time.Millisecond * rf.getElectionTimeout()):
				rf.convertToCandidate(Follower)
			}
		case Candidate:
			select {
			case <-rf.downToFollowerCh:
			case <-rf.promoteToLeaderCh:
				rf.convertToLeader(Candidate)
			case <-time.After(time.Millisecond * rf.getElectionTimeout()):
				rf.convertToCandidate(Candidate)
			}
		case Leader:
			select {
			case <-rf.downToFollowerCh:
			case <-time.After(time.Millisecond * HeartbeatInterval):
				rf.broadcastAppendEntries(true)
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(
	peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{0, nil})
	rf.snapshotTerm = 0
	rf.snapshotIndex = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = Follower
	rf.votesCount = 0
	rf.applyMsgCh = applyCh
	rf.resetChannels()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
