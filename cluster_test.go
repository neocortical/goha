package goha

import (
  "strings"
  "testing"
)

func TestAddSelf(t *testing.T) {
  c, _ := initCluster()
  _, err := addNodeToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)
  if err == nil {
    t.Errorf("able to add node without first initializing self node")
  }

  n1, err := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)
  if err != nil {
    t.Errorf("cluster.AddNode failed: %v", err)
  }
  if len(c.nodes) != 1 || len(c.gossip) != 1 {
    t.Error("incorrect cluster size after successful add")
  }
  if c.self == nil || c.self.Nid != n1.Nid {
    t.Error("failed to add first node as self node")
  }

  n1.Name = "derp"
  if c.self.Name != "foo" {
    t.Error("data access violation: reference to cluster self node outside cluster")
  }

  _, err = addSelfToCluster(c, "bar", "127.0.0.1:1338", "127.0.0.1:1438", NodeStateActive)
  if err == nil {
    t.Errorf("able to add self node after initializing self node")
  }
}

func TestGetSelf(t *testing.T) {
  c, _ := initCluster()
  n1, _ := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)

  n2 := c.GetSelf()
  if n2 == nil || n1.Nid != n2.Nid {
    t.Errorf("Unable to get self node from cluster")
  }
  n2.Name = "herp"
  if c.self.Name != "foo" {
    t.Error("data access violation: reference to cluster self node outside cluster")
  }
}

func TestAddNode(t *testing.T) {
  c, _ := initCluster()
  _, err := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)
  if err != nil {
    t.Errorf("cluster.AddNode failed: %v", err)
  }
  if len(c.nodes) != 1 || len(c.gossip) != 1 {
    t.Error("incorrect cluster size after successful add")
  }

  _, err = addNodeToCluster(c, "bar", "127.0.0.1:1338", "127.0.0.1:1438", NodeStateActive)

  if err != nil {
    t.Errorf("cluster.AddNode failed: %v", err)
  }
  if len(c.nodes) != 2 || len(c.gossip) != 2 {
    t.Error("incorrect cluster size after successful add")
  }
}

func TestAddingDuplicateNodesFail(t *testing.T) {
  c, _ := initCluster()
  n1, err := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)

  if err != nil {
    t.Errorf("cluster.AddNode failed: %v", err)
  }

  n2, err := addNodeToCluster(c, "foo", "127.0.0.1:1338", "127.0.0.1:1438", NodeStateActive)

  if err == nil || !strings.HasPrefix(err.Error(), "AddNode: duplicate node name") {
    t.Error("cluster.AddNode succeeded when it should have failed on name")
  }
  if len(c.nodes) != 1 || len(c.gossip) != 1 {
    t.Error("incorrect cluster size after failed add")
  }

  n2.Name = "bar"
  n2.Nid = n1.Nid
  err = c.AddNode(n2)
  if err == nil || !strings.HasPrefix(err.Error(), "AddNode: duplicate NID") {
    t.Error("cluster.AddNode succeeded when it should have failed on NID")
  }
  if len(c.nodes) != 1 || len(c.gossip) != 1 {
    t.Error("incorrect cluster size after failed add")
  }

  n2.Nid = c.GenerateNid()
  n2.GossipAddr = "127.0.0.1:1337"
  err = c.AddNode(n2)
  if err == nil || !strings.HasPrefix(err.Error(), "AddNode: duplicate internal address") {
    t.Error("cluster.AddNode succeeded when it should have failed on address")
  }
  if len(c.nodes) != 1 || len(c.gossip) != 1 {
    t.Error("incorrect cluster size after failed add")
  }
}

func TestAddIdenticalNodeSucceedsSilently(t *testing.T) {
  c, _ := initCluster()
  n1, err := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)

  if err != nil {
    t.Errorf("cluster.AddNode failed: %v", err)
  }

  n1.State = NodeStateInactive

  if err := c.AddNode(n1); err != nil {
    t.Errorf("cluster.AddNode failed on identical node: %v", err)
  }
  if len(c.nodes) != 1 || len(c.gossip) != 1 {
    t.Error("identical node added as separate entry in cluster")
  }
  if c.nodes[n1.Nid].State != NodeStateActive {
    t.Error("adding identical node changed node's state in cluster")
  }
}

func TestRecieveGossipNoChange(t *testing.T) {
  c, _ := initCluster()
  n1, _ := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)

  gossip := GossipNode{n1.Nid, 5, NodeStateActive, 1}
  err := c.ReceiveGossip(&gossip)
  if err != nil {
    t.Errorf("Receiving gossip for node in cluster produced error: %v", err)
  }
  if c.gossip[n1.Nid].Quiet > 0 {
    t.Error("Quiet for node in cluster increased by gossip")
  }
  if c.gossip[n1.Nid].State != NodeStateActive {
    t.Error("Node state changed by gossip")
  }
}

func TestRecieveGossipChangeStateAndQuiet(t *testing.T) {
  c, _ := initCluster()
  n1, _ := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)

  c.gossip[n1.Nid].Quiet = 10

  gossip := GossipNode{n1.Nid, 9, NodeStateInactive, 1}
  err := c.ReceiveGossip(&gossip)
  if err != nil {
    t.Errorf("Receiving gossip for node in cluster produced error: %v", err)
  }
  if c.gossip[n1.Nid].State != NodeStateActive {
    t.Error("state changed even though state counter wasn't higher")
  }

  gossip = GossipNode{n1.Nid, 8, NodeStateInactive, 2}
  err = c.ReceiveGossip(&gossip)
  if c.gossip[n1.Nid].Quiet != 8 {
    t.Error("Quiet for node in cluster not lowered by gossip")
  }
  if c.gossip[n1.Nid].State != NodeStateInactive {
    t.Error("Node state not changed by gossip")
  }
}

func TestReceiveGossipFailsOnUnknownNode(t *testing.T) {
  c, _ := initCluster()
  addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)

  gossip := GossipNode{12345, 1, NodeStateActive, 1}
  err := c.ReceiveGossip(&gossip)
  if err == nil || !strings.HasPrefix(err.Error(), "ReceiveGossip: node not found") {
    t.Error("Gossip for unknown node didn't produce expected error")
  }
}

func TestIncrementQuietCycles(t *testing.T) {
  c, _ := initCluster()
  n1, _ := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)
  n2, _ := addNodeToCluster(c, "bar", "127.0.0.1:1338", "127.0.0.1:1438", NodeStateActive)
  n3, _ := addNodeToCluster(c, "baz", "127.0.0.1:1339", "127.0.0.1:1439", NodeStateInactive)
  n4, _ := addNodeToCluster(c, "qux", "127.0.0.1:1340", "127.0.0.1:1440", NodeStateInactive)

  c.gossip[n2.Nid].Quiet = gossipQuietThreshold - 1
  c.gossip[n3.Nid].Quiet = gossipQuietThreshold - 1

  c.IncrementQuietCycles()

  if c.nodes[n1.Nid].State != NodeStateActive || c.gossip[n1.Nid].State != NodeStateActive {
    t.Error("node #1 mistakenly marked as failed")
  }
  if c.gossip[n1.Nid].Quiet != 1 {
    t.Errorf("Node #1's quiet value not incremented correctly: %d", c.gossip[n1.Nid].Quiet)
  }
  if c.nodes[n2.Nid].State != NodeStateFailed || c.gossip[n2.Nid].State != NodeStateFailed {
    t.Error("node #2 should have been marked as failed but wasn't")
  }
  if c.nodes[n2.Nid].StateCtr != 2 {
    t.Error("node #2 should have state counter incremented on state change but wasn't")
  }
  if c.nodes[n3.Nid].State != NodeStateInactive || c.gossip[n3.Nid].State != NodeStateInactive {
    t.Error("node #3 was inactive but was marked as failed")
  }
  if c.nodes[n4.Nid].State != NodeStateInactive || c.gossip[n4.Nid].State != NodeStateInactive {
    t.Error("node #4 was inactive but was marked as failed")
  }
  if c.gossip[n4.Nid].Quiet != 0 {
    t.Errorf("Node #4's quiet value incremented even though it is inactive: %d", c.gossip[n4.Nid].Quiet)
  }
}

func TestSelfStateChange(t *testing.T) {
  c, _ := initCluster()
  n1, _ := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)

  c.gossip[n1.Nid].Quiet = gossipQuietThreshold - 1
  c.IncrementQuietCycles()
  if c.nodes[n1.Nid].State != NodeStateFailed || c.gossip[n1.Nid].State != NodeStateFailed {
    t.Error("node #1 not failed when quiet threshold exceeded")
  }
  if c.self.State != NodeStateFailed {
    t.Error("Self node not failed appropriately")
  }

}

func TestGetActiveNode(t *testing.T) {
  c, _ := initCluster()
  n1, _ := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)
  n2, _ := addNodeToCluster(c, "bar", "127.0.0.1:1338", "127.0.0.1:1438", NodeStateInactive)
  n3, _ := addNodeToCluster(c, "baz", "127.0.0.1:1339", "127.0.0.1:1439", NodeStateFailed)

  n1b, err := c.GetActiveNode(n1.Nid)
  if err != nil || n1b == nil {
    t.Error("failed to get active node in cluster")
  }
  n1b.Name = "foo2"
  if c.nodes[n1.Nid].Name != "foo" {
    t.Error("illegal data access of node in cluster")
  }

  n2b, err := c.GetActiveNode(n2.Nid)
  if err == nil || !strings.HasPrefix(err.Error(), "GetActiveNode: node not in active state") || n2b != nil {
    t.Error("GetActiveNode returned inactive node")
  }

  n3b, err := c.GetActiveNode(n3.Nid)
  if err == nil || !strings.HasPrefix(err.Error(), "GetActiveNode: node not in active state") || n3b != nil {
    t.Error("GetActiveNode returned failed node")
  }

  n4b, err := c.GetActiveNode(c.GenerateNid())
  if err == nil || !strings.HasPrefix(err.Error(), "GetActiveNode: node not found") || n4b != nil {
    t.Error("GetActiveNode returned node not actually in cluster")
  }
}

func TestGetNode(t *testing.T) {
  c, _ := initCluster()
  n1, _ := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)
  n2, _ := addNodeToCluster(c, "bar", "127.0.0.1:1338", "127.0.0.1:1438", NodeStateInactive)
  n3, _ := addNodeToCluster(c, "baz", "127.0.0.1:1339", "127.0.0.1:1439", NodeStateFailed)

  n1b, err := c.GetNode(n1.Nid)
  if err != nil || n1b == nil {
    t.Error("failed to get node in cluster")
  }
  n1b.Name = "foo2"
  if c.nodes[n1.Nid].Name != "foo" {
    t.Error("illegal data access of node in cluster")
  }

  n2b, err := c.GetNode(n2.Nid)
  if err != nil || n2b == nil {
    t.Error("failed to get node in cluster")
  }

  n3b, err := c.GetNode(n3.Nid)
  if err != nil || n3b == nil {
    t.Error("failed to get node in cluster")
  }

  n4b, err := c.GetNode(c.GenerateNid())
  if err == nil || !strings.HasPrefix(err.Error(), "GetActiveNode: node not found") || n4b != nil {
    t.Error("GetNode returned node not actually in cluster")
  }
}

func TestGetAllNids(t *testing.T) {
  c, _ := initCluster()
  n1, _ := addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)
  n2, _ := addNodeToCluster(c, "bar", "127.0.0.1:1338", "127.0.0.1:1438", NodeStateInactive)
  n3, _ := addNodeToCluster(c, "baz", "127.0.0.1:1339", "127.0.0.1:1439", NodeStateFailed)

  allNids := c.GetAllNids()
  if len(allNids) != 3 {
    t.Errorf("should be 3 NIDs in cluster but found %d", len(allNids))
  }
  for _, nid := range allNids {
    if nid != n1.Nid && nid != n2.Nid && nid != n3.Nid {
      t.Errorf("unknown NID found in GetAllNids results: %d", nid)
    }
  }
}

func TestGetGossip(t *testing.T) {
  c, _ := initCluster()
  addSelfToCluster(c, "foo", "127.0.0.1:1337", "127.0.0.1:1437", NodeStateActive)
  addNodeToCluster(c, "bar", "127.0.0.1:1338", "127.0.0.1:1438", NodeStateInactive)
  addNodeToCluster(c, "baz", "127.0.0.1:1339", "127.0.0.1:1439", NodeStateFailed)

  gossip := c.GetGossip()
  if len(gossip) != 3 {
    t.Errorf("expected gossip of length 3 but was %d", len(gossip))
  }
}

// convenience method for adding a node to a cluster as the self node
func addSelfToCluster(c *cluster, name string, gossipAddr string, restAddr string, state NodeState) (n Node, err error) {
  n = Node{
    name,
    c.GenerateNid(),
    gossipAddr,
    restAddr,
    state,
    1,
  }
  err = c.AddSelf(n)
  return n, err
}

// convenience method for adding a node to a cluster
func addNodeToCluster(c *cluster, name string, gossipAddr string, restAddr string, state NodeState) (n Node, err error) {
  n = Node{
    name,
    c.GenerateNid(),
    gossipAddr,
    restAddr,
    state,
    1,
  }
  err = c.AddNode(n)
  return n, err
}
