package goha

import (
	"encoding/json"
	"errors"
	"fmt"
)

type NodeState uint8
type Nid uint32
type quietCycles uint8

const (
	NodeStateInactive NodeState = 0
	NodeStateActive   NodeState = 1
	NodeStateFailed   NodeState = 2
)

type Node struct {
	Name  string
	Nid   Nid
	Addr  string
	State NodeState
}

// Contains compact data regarding one node's knowledge of another node in the cluster
type GossipNode struct {
  Nid   Nid
  Quiet quietCycles // number of gossip cycles since one node as heard from another
  State NodeState   // the known state of the node
}

func (n Node) String() string {
	b, _ := json.Marshal(n)
	return string(b)
}

func (n *Node) validate() error {
	if len(n.Name) == 0 {
		return errors.New(fmt.Sprintf("nid must not be empty: %s", n.Nid))
	}
	if n.Addr == "" {
		return errors.New("internal addr must not be nil")
	}
	if &(n.State) == nil {
		return errors.New("node state must not be nil")
	}
	return nil
}

func (n *Node) EqualsExcludingState(n2 *Node) bool {
  return n2 != nil &&
    n.Nid == n2.Nid &&
    n.Name == n2.Name &&
    n.Addr == n2.Addr
}
