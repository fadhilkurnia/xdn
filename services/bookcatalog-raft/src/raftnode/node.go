package raftnode

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	rdb "github.com/hashicorp/raft-boltdb/v2"
	"gorm.io/gorm"
)

type Peer struct {
	ID      string
	Address string
}

type RaftNode struct {
	Raft *raft.Raft
	fsm  *BookFSM
}

func NewRaftNode(nodeID, raftAddr, raftDir string, peers []Peer, db *gorm.DB, dbPath string) (*RaftNode, error) {
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("raft: failed to create data dir: %w", err)
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Info,
		Output: os.Stderr,
	})

	// BoltDB store for log entries and stable storage — FSYNC #1 happens here.
	store, err := rdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("raft: failed to create bolt store: %w", err)
	}

	snapStore, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to create snapshot store: %w", err)
	}

	// Find this node's advertise address from the peer list.
	// We can't use raftAddr directly because it may be 0.0.0.0 (not advertisable).
	advertiseAddr := raftAddr
	for _, p := range peers {
		if p.ID == nodeID {
			advertiseAddr = p.Address
			break
		}
	}

	advAddr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to resolve advertise addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(raftAddr, advAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to create transport: %w", err)
	}

	// Bootstrap cluster if this is a fresh node
	lastIndex, err := store.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("raft: failed to get last index: %w", err)
	}
	if lastIndex == 0 {
		servers := make([]raft.Server, 0, len(peers))
		for _, p := range peers {
			servers = append(servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(p.ID),
				Address:  raft.ServerAddress(p.Address),
			})
		}
		configuration := raft.Configuration{Servers: servers}
		if err := raft.BootstrapCluster(config, store, store, snapStore, transport, configuration); err != nil {
			return nil, fmt.Errorf("raft: bootstrap failed: %w", err)
		}
	}

	fsm := NewBookFSM(db, dbPath)

	r, err := raft.NewRaft(config, fsm, store, store, snapStore, transport)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to create raft instance: %w", err)
	}

	return &RaftNode{Raft: r, fsm: fsm}, nil
}

func (rn *RaftNode) IsLeader() bool {
	return rn.Raft.State() == raft.Leader
}

func (rn *RaftNode) LeaderAddr() string {
	_, id := rn.Raft.LeaderWithID()
	return string(id)
}

func (rn *RaftNode) ApplyCommand(cmd Command, timeout time.Duration) (interface{}, error) {
	data, err := Encode(cmd)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to encode command: %w", err)
	}

	future := rn.Raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return future.Response(), nil
}
