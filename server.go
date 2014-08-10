package goha

import (
  "encoding/json"
  "net/http"
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
  mux.HandleFunc("/nodes", func(w http.ResponseWriter, req *http.Request) {
    svc.handleNodes(w, req)
  })
  mux.HandleFunc("/deactivate", func(w http.ResponseWriter, req *http.Request) {
    svc.handleDeactivate(w, req)
  })

  http.ListenAndServe(self.RestAddr, mux)
}

func (svc *RestService) handleNodes(w http.ResponseWriter, req *http.Request) {
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
  svc.cluster.DeactivateSelf()

  output := StateChangeResponse{true}
  j, _ := json.Marshal(output)

  w.Header().Set("Content-Type", "application/json")
  w.Write(j)
}
