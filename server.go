package goha

import (
  "net/http"
)

type RestService struct {
  cluster *cluster
  log *Log
}

func (svc *RestService) startRestService() {
  self := svc.cluster.GetSelf()
  mux := http.NewServeMux()
  mux.HandleFunc("/nodes", func(w http.ResponseWriter, req *http.Request) {
    svc.handleNodes(w, req)
  })
  http.ListenAndServe(self.RestAddr, mux)
}

func (svc *RestService) handleNodes(w http.ResponseWriter, req *http.Request) {
  w.Write([]byte("Hello, world."))
}
