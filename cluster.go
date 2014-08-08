package goha

import (
  "fmt"
  "math/rand"
  "sync"
)

const (
  gossipQuietThreshold quietCycles = 250
)

type cluster struct {
  self   *Node
	nodes  map[Nid]*Node
	gossip map[Nid]*GossipNode
	lock   sync.RWMutex
}

// Create a new cluster object from scratch
func initCluster() *cluster {
  return &cluster{
    nil,
    make(map[Nid]*Node),
    make(map[Nid]*GossipNode),
    sync.RWMutex{},
  }
}

func (c *cluster) AddSelf(self Node) error {
  c.lock.Lock()
  if c.self != nil {
    c.lock.Unlock()
    return fmt.Errorf("AddSelf: self already initialized")
  }
  c.self = &Node{self.Name, self.Nid, self.Addr, self.State}
  c.lock.Unlock()

  if err := c.AddNode(self); err != nil {
    c.lock.Lock()
    c.self = nil
    c.lock.Unlock()
    return err
  }

  return nil
}

func (c *cluster) GetSelf() (self *Node) {
  c.lock.RLock()
  defer c.lock.RUnlock()

  self = &Node{c.self.Name, c.self.Nid, c.self.Addr, c.self.State}
  return self
}

// Synchronously add a new node to the cluster, failing if Nid or Name is a duplicate
func (c *cluster) AddNode(newNode Node) error {
  c.lock.Lock()
  defer c.lock.Unlock()

  if c.self == nil {
    return fmt.Errorf("AddNode: must init self node first")
  }

  for nid, node := range c.nodes {
    // ignore calls to add identical node, and do not modify state in cluster
    if (&node).EqualsExcludingState(&newNode) {
      return nil
    }

    // detect duplicate information and fail
    if newNode.Nid == nid {
      return fmt.Errorf("AddNode: duplicate NID: %d", nid)
    } else if newNode.Name == node.Name {
      return fmt.Errorf("AddNode: duplicate node name: %s", node.Name)
    } else if newNode.Addr == node.Addr {
      return fmt.Errorf("AddNode: duplicate internal address: %s", node.Addr)
    }
  }
  c.nodes[newNode.Nid] = &newNode
  c.gossip[newNode.Nid] = &GossipNode{newNode.Nid, 0, newNode.State}

  return nil
}

// Update the cluster's gossip about a node
func (c *cluster) ReceiveGossip(newGossip *GossipNode) error {
  c.lock.Lock()
  defer c.lock.Unlock()

  gossip := c.gossip[newGossip.Nid]
  if gossip == nil {
    return fmt.Errorf("ReceiveGossip: node not found: %d", newGossip.Nid)
  }

  // update cluster to reflect state changes and/or quiet cycle max
  if gossip.State != newGossip.State {
    gossip.State = newGossip.State
  }
  if gossip.Quiet > newGossip.Quiet {
    gossip.Quiet = newGossip.Quiet
  }

  return nil
}

// After receiving gossip, increment quiet cycles for all nodes and fail any
// active nodes that exceed threshold
func (c *cluster) IncrementQuietCycles() {
  c.lock.Lock()
  defer c.lock.Unlock()

  for nid, gossip := range c.gossip {
    // Failed and Inactive nodes are not incremented
    if gossip.State != NodeStateActive {
      continue;
    }
    gossip.Quiet++
    if gossip.Quiet >= gossipQuietThreshold && gossip.State == NodeStateActive {
      gossip.State = NodeStateFailed
      c.nodes[nid].State = NodeStateFailed
    }
  }
}

// Generate a new Nid that doesn't exist in the current cluster
func (c *cluster) GenerateNid() Nid {
  c.lock.RLock()
  defer c.lock.RUnlock()

  var result Nid = Nid(rand.Uint32())
  for ; c.nodes[result] != nil; result = Nid(rand.Uint32()) {}
  return result
}

func (c *cluster) GetActiveNode(nid Nid) (node *Node, err error) {
  node, err = c.GetNode(nid) // locking occurs in inner method
  if err != nil {
    return nil, err
  } else if node.State != NodeStateActive {
    return nil, fmt.Errorf("GetActiveNode: node not in active state: %d", nid)
  }

  return node, nil
}

func (c *cluster) GetNode(nid Nid) (node *Node, err error) {
  c.lock.RLock()
  defer c.lock.RUnlock()

  clusterNode := c.nodes[nid]
  if clusterNode == nil {
    return nil, fmt.Errorf("GetActiveNode: node not found: %d", nid)
  }

  node = &Node{clusterNode.Name, clusterNode.Nid, clusterNode.Addr, clusterNode.State}
  return node, nil
}

func (c *cluster) GetRandomActiveNode() (node *Node) {
  c.lock.RLock()
  defer c.lock.RUnlock()

  if c.self == nil {
    node = nil
    return node
  }

  activeNids := make([]Nid, 0, len(c.nodes))
  for nid, node := range c.nodes {
    if nid != c.self.Nid && node.State == NodeStateActive {
      activeNids = append(activeNids, nid)
    }
  }

  if len(activeNids) == 0 {
    node = nil
    return node
  }

  index := rand.Intn(len(activeNids))
  chosen := c.nodes[activeNids[index]]
  node = &Node{chosen.Name, chosen.Nid, chosen.Addr, chosen.State}
  return node
}

// Get all NIDs currently in cluster
func (c *cluster) GetAllNids() (result []Nid) {
  c.lock.RLock()
  defer c.lock.RUnlock()

  result = make([]Nid, 0, len(c.nodes))
  for nid, _ := range c.nodes {
    result = append(result, nid)
  }

  return result
}

// Gets the latest gossip. Caller must not modify the gossip.
func (c *cluster) GetGossip() (gossip []GossipNode) {
  c.lock.RLock()
  defer c.lock.RUnlock()

  gossip = make([]GossipNode, 0, len(c.gossip))
  for _, gossipNode := range c.gossip {
    gossip = append(gossip, *gossipNode)
  }

  return gossip
}
