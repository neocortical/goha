package goha

import (
	"fmt"
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
	log     *Log
}

// Encapsulates a slice of nodes as a response
type NodesResponse struct {
  Nodes []Node
}

// RPC-exposed method to receive gossip data from another node
func (svc *GossipService) Gossip(msg *GossipMessage, response *string) error {

	svc.log.logTrace(fmt.Sprintf("Got gossip message: %v", msg))

  // loop over gossip, processing each node
  unknownNodes := make([]Nid, 0, 10)
  for _, gossip := range msg.Gossip {
    if err := svc.cluster.ReceiveGossip(&gossip); err != nil {
      unknownNodes = append(unknownNodes, gossip.Nid)
    }
  }

  // process any unknown nodes encountered
  unknownCount := len(unknownNodes)
  if unknownCount > 0 {
    svc.log.logDebug(fmt.Sprintf("Encountered %d unknown nodes during gossip", unknownCount))
    go svc.discoverNodes(msg.Sender, unknownNodes)
  }

  // finally, increment quiet values and fail any nodes over threshold
  svc.cluster.IncrementQuietCycles()

  *response = "ok"

	return nil
}

// RPC-exposed method to serve node discovery information to another node
func (svc *GossipService) GetNodes(nids *[]Nid, response *NodesResponse) error {
  if nids == nil || len(*nids) == 0 {
    svc.log.logError("GetNodes: received nil Nid slice")
    return fmt.Errorf("GetNodes: nids must not be nil")
  }

  response.Nodes = make([]Node, 0 , len(*nids))
  for _, nid := range *nids {
    if node, err := svc.cluster.GetNode(nid); err == nil {
      response.Nodes = append(response.Nodes, *node)
    } else {
      svc.log.logDebug(fmt.Sprintf("node not found during discovery: %d", nid))
    }
  }

  return nil
}

// RPC-exposed method for a new node to join cluster
func (svc *GossipService) JoinCluster(newNode *Node, response *NodesResponse) error {
  if newNode == nil {
    svc.log.logError("JoinCluster: received nil join request")
    return fmt.Errorf("JoinCluster: node cannot be nil")
  } else if err := newNode.validate(); err != nil {
    svc.log.logError(fmt.Sprintf("JoinCluster: received invalid join request: %v", newNode))
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
      svc.log.logDebug(fmt.Sprintf("node not found during discovery: %d", nid))
    }
  }

  // broadcast the join event to all nodes in the cluster
  go svc.broadcastNodeJoinEvent(newNode)

  svc.log.logInfo(fmt.Sprintf("Returning %d cluster nodes to join requestor", len(response.Nodes)))
  return nil
}

// RPC-exposed method to add a node to the local cluster
func (svc *GossipService) AddNode(node *Node, response *string) error {
  svc.log.logTrace(fmt.Sprintf("Got AddNode: %v", *node))
  if node == nil {
    svc.log.logError("AddNode: received nil node")
    return fmt.Errorf("AddNode: node cannot be nil")
  } else if err := node.validate(); err != nil {
    svc.log.logError(fmt.Sprintf("AddNode: received invalid node signature: %v", node))
    return fmt.Errorf("AddNode: invalid node")
  }

  err := svc.cluster.AddNode(*node)
  if err != nil {
    svc.log.logError("AddNode: node could not be added")
    return fmt.Errorf("AddNode: node could not be added")
  }

  *response = "ok"

  return nil
}

// internal func to discover nodes that we received gossip for but don't have in our cluster
func (svc *GossipService) discoverNodes(senderNid Nid, nids []Nid) {
  if len(nids) > maxNodeDiscoveryBatchSize {
    nids = nids[:maxNodeDiscoveryBatchSize]
  }

  sender, err := svc.cluster.GetActiveNode(senderNid)
  if err != nil {
    svc.log.logError(fmt.Sprintf("Error discovering %d nodes: Couldn't get sender %d: %v", len(nids), senderNid, err))
    return
  }

  // dial RPC
  remoteGossipSvc, err := rpc.Dial("tcp", sender.GossipAddr)
  if err != nil {
    svc.log.logError(fmt.Sprintf("Error trying to gossip to \"%s\": %v", sender.GossipAddr, err))
    return
  }
  defer remoteGossipSvc.Close()

  nodes := make([]Node, 0, len(nids))
  err = remoteGossipSvc.Call("GossipService.GetNodes", &nids, &nodes)
  if err != nil {
    svc.log.logError(fmt.Sprintf("Error discoving nodes from \"%s\": %v", sender.GossipAddr, err))
    return
  }

  added := 0
  for _, node := range nodes {
    if err = svc.cluster.AddNode(node); err != nil {
      svc.log.logWarn(fmt.Sprintf("Failure to add newly discovered node: %v", node))
    } else {
      added++
    }
  }
  svc.log.logInfo(fmt.Sprintf("Added %d nodes to cluster via discovery", added))
}

// Broadcast a newly joined node's details across the cluster
func (svc *GossipService) broadcastNodeJoinEvent(newNode *Node) {
  clusterNids := svc.cluster.GetAllNids()
  svc.log.logTrace(fmt.Sprintf("Broadcasting new node %d to cluster: %v", newNode.Nid, clusterNids))

  for _, nid := range clusterNids {
    if node, err := svc.cluster.GetActiveNode(nid); err == nil {
      // dial RPC
      remoteGossipSvc, err := rpc.Dial("tcp", node.GossipAddr)
      if err != nil {
        svc.log.logError(fmt.Sprintf("Error trying to broadcast node join to \"%s\": %v", node.GossipAddr, err))
      }

      response := ""
      err = remoteGossipSvc.Call("GossipService.AddNode", newNode, &response)
      remoteGossipSvc.Close()
      if err != nil {
        svc.log.logError(fmt.Sprintf("Error broadcasting add node to \"%s\": %v", node.GossipAddr, err))
      }
    } else {
      svc.log.logTrace(fmt.Sprintf("Ingoring broadcast to node %d: %v", nid, err))
    }
  }
}

// Start the gossip server. Method is unexposed so that RPC doesn't complain
func (svc *GossipService) startGossip(joinAddr string) error {
	svc.log.logInfo("Starting gossip service...")

  self := *svc.cluster.GetSelf()

  // start listening for RPC communication
  rpcServer := rpc.NewServer()
	rpcServer.Register(svc)
	listener, err := net.Listen("tcp", self.GossipAddr)
	if err != nil {
		svc.log.logFatal(fmt.Sprintf("RPC listen error: %v", err))
		return err
	} else {
    svc.log.logInfo(fmt.Sprintf("Listening on %s", self.GossipAddr))
		go func() {
      for {
        cxn, err := listener.Accept()
  			if err != nil {
  				svc.log.logError(fmt.Sprintf("RPC error: %v", err))
  				continue
  			}
  			svc.log.logTrace(fmt.Sprintf("Server %s accepted RPC connection from %s", self.GossipAddr, cxn.RemoteAddr()))
  			go rpcServer.ServeConn(cxn)
      }
    }()
	}

	// If we were given a broker address, try to join a cluster or die
	if joinAddr != "" {
		svc.log.logInfo(fmt.Sprintf("Joining cluster via broker: %s", joinAddr))
		remoteGossipSvc, err := rpc.Dial("tcp", joinAddr)
		if err != nil {
			svc.log.logFatal(fmt.Sprintf("Error trying to dial cluster join broker: %v", err))
			return err
		}

    joinResponse := NodesResponse{make([]Node, 0, 1)}
		err = remoteGossipSvc.Call("GossipService.JoinCluster", &self, &joinResponse)
    remoteGossipSvc.Close()
		if err != nil {
			svc.log.logFatal(fmt.Sprintf("Unable to join cluster: %v", err))
			return err
		}

    svc.log.logInfo(fmt.Sprintf("Got %d nodes back from join request", len(joinResponse.Nodes)))
    for _, clusterNode := range joinResponse.Nodes {
      if err = svc.cluster.AddNode(clusterNode); err != nil {
        svc.log.logError(fmt.Sprintf("failed to add node to cluster: %v: %v", clusterNode, err))
      }
    }

		svc.log.logInfo(fmt.Sprintf("Joined cluster via broker %s. We are node: %d", joinAddr, self.Nid))
    svc.cluster.DoJoinedClusterCallback()
	}

  // start gossip loop
	duration, _ := time.ParseDuration("1s") // TODO: allow gossip period to be configured
	ticker := time.NewTicker(duration)
	lastTick := time.Now()
	for {
		tick := <-ticker.C
		if tick.Sub(lastTick).Seconds() > 1.5 { // TODO: allow gossip period to be configured
			svc.log.logWarn(fmt.Sprintf("Lost one or more ticks due to server load: %v", tick.Sub(lastTick)))
		}

    randomNode := svc.cluster.GetRandomActiveNode()
    if randomNode != nil {

      // choose a random neighbor to gossip with
      svc.log.logTrace(fmt.Sprintf("Gossiping to %s", randomNode.GossipAddr))

      // dial up that neighbor and gossip
      remoteGossipSvc, err := rpc.Dial("tcp", randomNode.GossipAddr)
      if err != nil {
        svc.log.logError(fmt.Sprintf("Error trying to gossip to \"%s\": %v", randomNode.GossipAddr, err))
        continue
      }

      msg := GossipMessage{self.Nid, svc.cluster.GetGossip()}
      response := ""
      err = remoteGossipSvc.Call("GossipService.Gossip", &msg, &response)
      remoteGossipSvc.Close()
      if err != nil {
        svc.log.logError(fmt.Sprintf("Error getting gossip response from \"%s\": %v", randomNode.GossipAddr, err))
      }
    } else {
      svc.log.logTrace("Not gossipping because we are the only node in the cluster")
    }

		lastTick = tick
	}

	svc.log.logInfo("Gossip service shutting down.")
	return nil
}
