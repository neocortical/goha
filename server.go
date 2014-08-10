package goha

import (
  "encoding/json"
  "fmt"
  "net/http"
  "net/rpc"
)

type RestService struct {
  cluster *cluster
  log *Log
}

type Cluster struct {
  Active []ClusterNode
  Inactive []ClusterNode
  Failed []ClusterNode
}

type ClusterNode struct {
  Name string
  Addr string
}

type StateChangeResponse struct {
  Success bool
}

func (svc *RestService) startRestService() {
  self := svc.cluster.GetSelf()
  mux := http.NewServeMux()
  mux.HandleFunc("/self/cluster", func(w http.ResponseWriter, req *http.Request) {
    svc.handleCluster(w, req)
  })
  mux.HandleFunc("/self/deactivate", func(w http.ResponseWriter, req *http.Request) {
    svc.handleDeactivate(w, req)
  })

  http.ListenAndServe(self.RestAddr, mux)
}

func (svc *RestService) handleCluster(w http.ResponseWriter, req *http.Request) {
  nodes := svc.cluster.GetAllNodes()

  output := Cluster{
    make([]ClusterNode, 0, len(*nodes)),
    make([]ClusterNode, 0, 0),
    make([]ClusterNode, 0, 0),
  }
  for _, node := range *nodes {
    switch node.State {
    case NodeStateActive:
      output.Active = append(output.Active, ClusterNode{node.Name, node.RestAddr})
    case NodeStateInactive:
      output.Inactive = append(output.Inactive, ClusterNode{node.Name, node.RestAddr})
    case NodeStateFailed:
      output.Failed = append(output.Failed, ClusterNode{node.Name, node.RestAddr})
    }
  }

  j, _ := json.Marshal(output)

  w.Header().Set("Content-Type", "application/json")
  w.Write(j)
}

func (svc *RestService) handleDeactivate(w http.ResponseWriter, req *http.Request) {

  output := StateChangeResponse{true}

  self := svc.cluster.GetSelf()
  if self != nil {
    if err := svc.cluster.ChangeNodeState(self.Nid, NodeStateInactive); err != nil {
      output.Success = false
    } else {
      svc.log.logInfo("Deactivated self. Broadcasting to cluster.")
      go svc.broadcastSelfDeactivation()
    }
  } else {
    output.Success = false
  }

  j, _ := json.Marshal(output)

  w.Header().Set("Content-Type", "application/json")
  w.Write(j)
}

func (svc *RestService) broadcastSelfDeactivation() {
  self := svc.cluster.GetSelf()
  nids := svc.cluster.GetAllNids()

  for _, nid := range nids {
    node, err := svc.cluster.GetActiveNode(nid)
    if err != nil {
      continue
    }

    svc.log.logTrace(fmt.Sprintf("Sending deactivate message to %s", node.GossipAddr))

    // dial up that neighbor and gossip
    remoteGossipSvc, err := rpc.Dial("tcp", node.GossipAddr)
    if err != nil {
      svc.log.logError(fmt.Sprintf("Error trying to send deactivate message to \"%s\": %v", node.GossipAddr, err))
      continue
    }

    msg := StateChangeMessage{self.Nid, self.Nid, NodeStateInactive}
    response := ""
    err = remoteGossipSvc.Call("GossipService.ChangeNodeState", msg, &response)
    remoteGossipSvc.Close()
    if err != nil {
      svc.log.logError(fmt.Sprintf("Error getting state change response from \"%s\": %v", node.GossipAddr, err))
    }
  }

  svc.log.logInfo("Self deactivation broadcasted to cluster")
}
