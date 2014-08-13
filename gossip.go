package goha

import (
	"fmt"
	l5g "github.com/neocortical/log5go"
  "net"
  "net/rpc"
  "time"
)

const (
  maxNodeDiscoveryBatchSize int = 100
)

// Encapsulates an entire gossip message (gossip list and control data)
type GossipMessage struct {
  Sender Nid
	Gossip []GossipNode
}

// Encapsulates the gossip server
type GossipService struct {
	cluster *cluster
	log     l5g.Log5Go
}

// Encapsulates a slice of nodes as a response
type NodesResponse struct {
  Nodes []Node
}

// Encapsulates a call to change a node's state
type StateChangeMessage struct {
  Sender Nid
  Target Nid
  State NodeState
}

// RPC-exposed method to receive gossip data from another node
func (svc *GossipService) Gossip(msg *GossipMessage, response *string) error {

	svc.log.Trace("Got gossip message: %v", msg)

  // loop over gossip, processing each node
  unknownNodes, err := svc.cluster.HandleGossip(msg.Sender, msg.Gossip)
  if err != nil {
    return err
  }

  // process any unknown nodes encountered
  unknownCount := len(*unknownNodes)
  if unknownCount > 0 {
    svc.log.Debug("Encountered %d unknown nodes during gossip", unknownCount)
    go svc.discoverNodes(msg.Sender, *unknownNodes)
  }

  *response = "ok"

	return nil
}

// RPC-exposed method to serve node discovery information to another node
func (svc *GossipService) GetNodes(nids *[]Nid, response *NodesResponse) error {
  if nids == nil || len(*nids) == 0 {
    svc.log.Error("GetNodes: received nil Nid slice")
    return fmt.Errorf("GetNodes: nids must not be nil")
  }

  response.Nodes = make([]Node, 0 , len(*nids))
  for _, nid := range *nids {
    if node, err := svc.cluster.GetNode(nid); err == nil {
      response.Nodes = append(response.Nodes, *node)
    } else {
      svc.log.Debug("node not found during discovery: %d", nid)
    }
  }

  return nil
}

// RPC-exposed method for a new node to join cluster
func (svc *GossipService) JoinCluster(newNode *Node, response *NodesResponse) error {
  if newNode == nil {
    svc.log.Error("JoinCluster: received nil join request")
    return fmt.Errorf("JoinCluster: node cannot be nil")
  } else if err := newNode.validate(); err != nil {
    svc.log.Error("JoinCluster: received invalid join request: %v", newNode)
    return fmt.Errorf("JoinCluster: invalid node")
  }

  err := svc.cluster.AddNode(*newNode)
  if err != nil {
    return err
  }

  clusterNids := svc.cluster.GetAllNids()
  if len(clusterNids) > maxNodeDiscoveryBatchSize {
    clusterNids = clusterNids[:maxNodeDiscoveryBatchSize]
  }

  response.Nodes = make([]Node, 0, len(clusterNids))
  for _, nid := range clusterNids {
    if node, err := svc.cluster.GetNode(nid); err == nil {
      response.Nodes = append(response.Nodes, *node)
    } else {
      svc.log.Debug("node not found during discovery: %d", nid)
    }
  }

  // broadcast the join event to all nodes in the cluster
  go svc.broadcastNodeJoinEvent(newNode)

  svc.log.Info("Returning %d cluster nodes to join requestor", len(response.Nodes))
  return nil
}

// RPC-exposed method to add a node to the local cluster
func (svc *GossipService) AddNode(node *Node, response *string) error {
  svc.log.Trace("Got AddNode: %v", *node)
  if node == nil {
    svc.log.Error("AddNode: received nil node")
    return fmt.Errorf("AddNode: node cannot be nil")
  } else if err := node.validate(); err != nil {
    svc.log.Error("AddNode: received invalid node signature: %v", node)
    return fmt.Errorf("AddNode: invalid node")
  }

  err := svc.cluster.AddNode(*node)
  if err != nil {
    svc.log.Error("AddNode: node could not be added")
    return fmt.Errorf("AddNode: node could not be added")
  }

  *response = "ok"

  return nil
}

func (svc *GossipService) ChangeNodeState(msg StateChangeMessage, response *string) error {
  err := svc.cluster.ChangeNodeState(msg.Target, msg.State)
  if err != nil {
    return err
  } else {
    *response = "ok"
    return nil
  }
}

// internal func to discover nodes that we received gossip for but don't have in our cluster
func (svc *GossipService) discoverNodes(senderNid Nid, nids []Nid) {
  if len(nids) > maxNodeDiscoveryBatchSize {
    nids = nids[:maxNodeDiscoveryBatchSize]
  }

  sender, err := svc.cluster.GetActiveNode(senderNid)
  if err != nil {
    svc.log.Error("Error discovering %d nodes: Couldn't get sender %d: %v", len(nids), senderNid, err)
    return
  }

  // dial RPC
  remoteGossipSvc, err := rpc.Dial("tcp", sender.GossipAddr)
  if err != nil {
    svc.log.Error("Error trying to gossip to \"%s\": %v", sender.GossipAddr, err)
    return
  }
  defer remoteGossipSvc.Close()

  nodes := make([]Node, 0, len(nids))
  err = remoteGossipSvc.Call("GossipService.GetNodes", &nids, &nodes)
  if err != nil {
    svc.log.Error("Error discoving nodes from \"%s\": %v", sender.GossipAddr, err)
    return
  }

  added := 0
  for _, node := range nodes {
    if err = svc.cluster.AddNode(node); err != nil {
      svc.log.Warn("Failure to add newly discovered node: %v", node)
    } else {
      added++
    }
  }
  svc.log.Info("Added %d nodes to cluster via discovery", added)
}

// Broadcast a newly joined node's details across the cluster
func (svc *GossipService) broadcastNodeJoinEvent(newNode *Node) {
  clusterNids := svc.cluster.GetAllNids()
  svc.log.Trace("Broadcasting new node %d to cluster: %v", newNode.Nid, clusterNids)

  for _, nid := range clusterNids {
    if node, err := svc.cluster.GetActiveNode(nid); err == nil {
      // dial RPC
      remoteGossipSvc, err := rpc.Dial("tcp", node.GossipAddr)
      if err != nil {
        svc.log.Error("Error trying to broadcast node join to \"%s\": %v", node.GossipAddr, err)
      }

      response := ""
      err = remoteGossipSvc.Call("GossipService.AddNode", newNode, &response)
      remoteGossipSvc.Close()
      if err != nil {
        svc.log.Error("Error broadcasting add node to \"%s\": %v", node.GossipAddr, err)
      }
    } else {
      svc.log.Trace("Ingoring broadcast to node %d: %v", nid, err)
    }
  }
}

// Start the gossip server. Method is unexposed so that RPC doesn't complain
func (svc *GossipService) startGossip(joinAddr string) error {
	svc.log.Info("Starting gossip service...")

  self := *svc.cluster.GetSelf()
  serviceAddr := self.GossipAddr

  // start listening for RPC communication
  rpcServer := rpc.NewServer()
	rpcServer.Register(svc)
	listener, err := net.Listen("tcp", serviceAddr)
	if err != nil {
		svc.log.Fatal("RPC listen error: %v", err)
		return err
	} else {
    svc.log.Info("Listening on %s", serviceAddr)
		go func() {
      for {
        cxn, err := listener.Accept()
  			if err != nil {
  				svc.log.Error("RPC error: %v", err)
  				continue
  			}
  			svc.log.Trace("Server %s accepted RPC connection from %s", serviceAddr, cxn.RemoteAddr())
  			go rpcServer.ServeConn(cxn)
      }
    }()
	}

	// If we were given a broker address, try to join a cluster or die
	if joinAddr != "" {
		svc.log.Info("Joining cluster via broker: %s", joinAddr)
		remoteGossipSvc, err := rpc.Dial("tcp", joinAddr)
		if err != nil {
			svc.log.Fatal("Error trying to dial cluster join broker: %v", err)
			return err
		}

    joinResponse := NodesResponse{make([]Node, 0, 1)}
		err = remoteGossipSvc.Call("GossipService.JoinCluster", &self, &joinResponse)
    remoteGossipSvc.Close()
		if err != nil {
			svc.log.Fatal("Unable to join cluster: %v", err)
			return err
		}

    svc.log.Info("Got %d nodes back from join request", len(joinResponse.Nodes))
    for _, clusterNode := range joinResponse.Nodes {
      if err = svc.cluster.AddNode(clusterNode); err != nil {
        svc.log.Error("failed to add node to cluster: %v: %v", clusterNode, err)
      }
    }

		svc.log.Info("Joined cluster via broker %s. We are node: %d", joinAddr, self.Nid)
    svc.cluster.DoJoinedClusterCallback()
	}

  // start gossip loop
	duration, _ := time.ParseDuration("1s") // TODO: allow gossip period to be configured
	ticker := time.NewTicker(duration)
	lastTick := time.Now()
	for {
		tick := <-ticker.C
		if tick.Sub(lastTick).Seconds() > 1.5 { // TODO: allow gossip period to be configured
			svc.log.Warn("Lost one or more ticks due to server load: %v", tick.Sub(lastTick))
		}

    self := *svc.cluster.GetSelf()
    if self.State != NodeStateActive {
      svc.log.Trace("Not gossiping because we not in active state: %s", self.State)
      lastTick = tick
      continue
    }

    svc.cluster.IncrementQuietCycles()

    randomNode := svc.cluster.GetRandomActivePeer()
    if randomNode != nil {

      // choose a random neighbor to gossip with
      svc.log.Trace("Gossiping to %s", randomNode.GossipAddr)

      // dial up that neighbor and gossip
      remoteGossipSvc, err := rpc.Dial("tcp", randomNode.GossipAddr)
      if err != nil {
        svc.log.Error("Error trying to gossip to \"%s\": %v", randomNode.GossipAddr, err)
        continue
      }

      msg := GossipMessage{self.Nid, svc.cluster.GetGossip()}
      response := ""
      err = remoteGossipSvc.Call("GossipService.Gossip", &msg, &response)
      remoteGossipSvc.Close()
      if err != nil {
        svc.log.Error("Error getting gossip response from \"%s\": %v", randomNode.GossipAddr, err)
      }
    } else {
      svc.log.Trace("Not gossipping because we are the only node in the cluster")
    }

		lastTick = tick
	}

	svc.log.Info("Gossip service shutting down.")
	return nil
}
