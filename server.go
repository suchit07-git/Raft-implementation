package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Server struct {
	mutex        sync.Mutex
	serverId     int
	peerIds      []int
	stateMachine *StateMachine
	rpcProxy     *RPCProxy
	rpcServer    *rpc.Server
	listener     net.Listener
	peerClients  map[int]*rpc.Client
	ready        <-chan interface{}
	quit         chan interface{}
	wg           sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, ready <-chan interface{}) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan interface{})
	return s
}

func (s *Server) Serve() {
	s.mutex.Lock()
	s.stateMachine = NewStateMachine(s.serverId, s.peerIds, s, s.ready)
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{stateMachine: s.stateMachine}
	s.rpcServer.RegisterName("StateMachine", s.rpcProxy)
	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("State Machine %d listening at %s", s.serverId, s.listener.Addr())
	s.mutex.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("Error in accepting connection:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) DisconnectAll() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.stateMachine.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddress() net.Addr {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.listener.Addr()
}

func (s *Server) connectToPeer(peerId int, addr net.Addr) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *Server) DisconnectPeer(peerId int) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mutex.Lock()
	peer := s.peerClients[id]
	s.mutex.Unlock()
	if peer == nil {
		return fmt.Errorf("Called client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

type RPCProxy struct {
	stateMachine *StateMachine
}

func (rpcProxy *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	return rpcProxy.stateMachine.RequestVote(args, reply)
}

func (rpcProxy *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	return rpcProxy.stateMachine.AppendEntries(args, reply)
}
