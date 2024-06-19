package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type MachineState int

const (
	DEAD MachineState = iota
	FOLLOWER
	CANDIDATE
	LEADER
)

func (state MachineState) getState() string {
	switch state {
	case FOLLOWER:
		return "Follower"
	case CANDIDATE:
		return "Candidate"
	case LEADER:
		return "Leader"
	case DEAD:
		return "Dead"
	default:
		panic("Unreachable")
	}
}

type StateMachine struct {
	mutex              sync.Mutex
	id                 int
	peerIds            []int
	currentTerm        int
	votedFor           int
	state              MachineState
	log                []LogEntry
	electionResetEvent time.Time
	server             *Server
}

func NewStateMachine(id int, peerIds []int, server *Server, ready <-chan interface{}) *StateMachine {
	stateMachine := new(StateMachine)
	stateMachine.id = id
	stateMachine.peerIds = peerIds
	stateMachine.server = server
	stateMachine.state = FOLLOWER
	stateMachine.votedFor = -1

	go func() {
		<-ready
		stateMachine.mutex.Lock()
		stateMachine.electionResetEvent = time.Now()
		stateMachine.mutex.Unlock()
		stateMachine.runElectionTimer()
	}()
	return stateMachine
}

func (stateMachine *StateMachine) runElectionTimer() {
	timeOutDuration := time.Duration(150+rand.Intn(150)) * time.Millisecond
	stateMachine.mutex.Lock()
	currTerm := stateMachine.currentTerm
	stateMachine.mutex.Unlock()
	log.Printf("State Machine %d: Election timer started(%v), Term = %d", stateMachine.id, timeOutDuration, currTerm)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		stateMachine.mutex.Lock()
		if stateMachine.state == LEADER {
			log.Printf("State Machine %d: in election timer State = %s bailing out", stateMachine.state)
			stateMachine.mutex.Unlock()
			return
		}
		if currTerm != stateMachine.currentTerm {
			log.Printf("State Machine %d: Term changed from %d to %d, bailing out", stateMachine.id, currTerm, stateMachine.currentTerm)
			stateMachine.mutex.Unlock()
			return
		}

		if timeElapsed := time.Since(stateMachine.electionResetEvent); timeElapsed >= timeOutDuration {
			stateMachine.startElection()
			stateMachine.mutex.Unlock()
			return
		}
		stateMachine.mutex.Unlock()
	}
}

func (stateMachine *StateMachine) Stop() {
	stateMachine.mutex.Lock()
	defer stateMachine.mutex.Unlock()
	stateMachine.state = DEAD
	log.Printf("State machine %d is dead", stateMachine.id)
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
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	voteGranted bool
}

func (stateMachine *StateMachine) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	stateMachine.mutex.Lock()
	defer stateMachine.mutex.Unlock()
	if stateMachine.state == DEAD {
		return nil
	}
	log.Printf("State Machine %d: AppendEntries %+v", args)
	if args.Term > stateMachine.currentTerm {
		log.Printf("State Machine %d: Term out of date in AppendEntries", stateMachine.id)
		stateMachine.becomeFollower(args.Term)
	}
	reply.Success = false
	if args.Term == stateMachine.currentTerm {
		if stateMachine.state != FOLLOWER {
			stateMachine.becomeFollower(args.Term)
		}
		stateMachine.electionResetEvent = time.Now()
		reply.Success = true
	}
	reply.Term = stateMachine.currentTerm
	log.Printf("State Machine %d: AppendEntries Reply: %+v", *reply)
	return nil
}

func (stateMachine *StateMachine) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	stateMachine.mutex.Lock()
	defer stateMachine.mutex.Unlock()
	if stateMachine.state == DEAD {
		return nil
	}
	log.Printf("State Machine %d: RequestVote: %+v [currentTerm = %d, votedFor = %d]", stateMachine.id, args, stateMachine.currentTerm, stateMachine.votedFor)
	if args.Term > stateMachine.currentTerm {
		log.Printf("State Machine %d: Term out of date in RequestVote", stateMachine.id)
		stateMachine.becomeFollower(args.Term)
	}
	if stateMachine.currentTerm == args.Term && (stateMachine.votedFor == -1 || stateMachine.votedFor == args.CandidateId) {
		reply.voteGranted = true
		stateMachine.votedFor = args.CandidateId
		stateMachine.electionResetEvent = time.Now()
	} else {
		reply.voteGranted = false
	}
	reply.Term = stateMachine.currentTerm
	log.Printf("State Machine %d: RequestVoteReply reply: %+v", stateMachine.id, reply)
	return nil
}

func (stateMachine *StateMachine) startElection() {
	stateMachine.state = CANDIDATE
	stateMachine.currentTerm += 1
	currTerm := stateMachine.currentTerm
	stateMachine.electionResetEvent = time.Now()
	stateMachine.votedFor = stateMachine.id
	log.Printf("State machine %d becomes candidate (Current Term = %d); Log: %v", currTerm, stateMachine.log)
	votesReceived := 1

	for _, peerId := range stateMachine.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        currTerm,
				CandidateId: stateMachine.id,
			}
			var reply RequestVoteReply
			log.Printf("State Machine %d: Sending RequestVote to %d: %+v", peerId, args)
			if err := stateMachine.server.Call(peerId, "StateMachine.RequestVote", args, &reply); err == nil {
				stateMachine.mutex.Lock()
				defer stateMachine.mutex.Unlock()
				log.Printf("State Machine %d: received RequestVoteReply %+v", stateMachine.id, reply)
				if stateMachine.state != CANDIDATE {
					log.Printf("State Machine %d while waiting for reply, state = %v", stateMachine.id, stateMachine.state)
					return
				}
				if reply.Term > currTerm {
					log.Printf("State Machine %d's term out of date in RequestVoteReply", stateMachine.id)
					stateMachine.becomeFollower(reply.Term)
					return
				} else if reply.Term == currTerm {
					if reply.voteGranted {
						votesReceived += 1
						if votesReceived > (len(stateMachine.peerIds)+1)/2 {
							log.Printf("State Machine %d wins the election with %d votes", stateMachine.id, votesReceived)
							stateMachine.becomeLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}
	go stateMachine.runElectionTimer()
}

func (stateMachine *StateMachine) becomeFollower(term int) {
	log.Printf("State Machine %d becomes follower with term = %d, Log: %v", stateMachine.id, term, stateMachine.log)
	stateMachine.state = FOLLOWER
	stateMachine.currentTerm = term
	stateMachine.votedFor = -1
	stateMachine.electionResetEvent = time.Now()
	go stateMachine.runElectionTimer()
}

func (stateMachine *StateMachine) becomeLeader() {
	stateMachine.state = LEADER
	log.Printf("State Machine %d becomes leader with term = %d, Log: %v", stateMachine.id, stateMachine.currentTerm, stateMachine.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			stateMachine.sendHeartBeats()
			<-ticker.C
			stateMachine.mutex.Lock()
			if stateMachine.state != LEADER {
				stateMachine.mutex.Unlock()
				return
			}
			stateMachine.mutex.Unlock()
		}
	}()
}

func (stateMachine *StateMachine) sendHeartBeats() {
	stateMachine.mutex.Lock()
	if stateMachine.state != LEADER {
		stateMachine.mutex.Unlock()
		return
	}
	currTerm := stateMachine.currentTerm
	stateMachine.mutex.Unlock()

	for _, peerId := range stateMachine.peerIds {
		args := AppendEntriesArgs{
			Term:     currTerm,
			LeaderId: stateMachine.id,
		}
		go func(peerId int) {
			log.Printf("State Machine %d: Sending AppendEntries to %v, Args: %+v", stateMachine.id, peerId, args)
			var reply AppendEntriesReply
			if err := stateMachine.server.Call(peerId, "StateMachine.AppendEntries", args, &reply); err == nil {
				stateMachine.mutex.Lock()
				defer stateMachine.mutex.Unlock()
				if reply.Term > currTerm {
					log.Printf("State Machine %d: Term out of date in heartbeat reply", stateMachine.id)
					stateMachine.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}
