package goha

import (
  l5g "github.com/neocortical/log5go"
  "strings"
)

func Start(config Config) (_ <-chan Callback, err error) {
  config.Name = strings.TrimSpace(config.Name)
  log, err := loggingInit(config.Logdir, config.LogLevel, config.Name)
  if err != nil {
    return nil, err
  }

  log.Info("Node startup from config: %s", config)

  cluster, cbChan := initCluster()

  self := Node{
    config.Name,
    cluster.GenerateNid(),
    config.InternalAddr,
    config.ExternalAddr,
    NodeStateActive,
    1,
  }
  err = cluster.AddSelf(self)
  if err != nil {
    log.Fatal("Error adding self to cluster: %v", err)
    return cbChan, err
  }

  doService(cluster, config.JoinAddr)

  log.Info("Cluster started")

  return cbChan, nil
}

func doService(cluster *cluster, joinAddr string) {

  log, _ := l5g.GetLog(cluster.GetSelf().Name)
  log.Info("Cluster initialized. Starting service. We are NID: %d", cluster.GetSelf().Nid)

  gossipService := &GossipService{cluster, log}
  go gossipService.startGossip(joinAddr)

  restService := &RestService{cluster, log}
  go restService.startRestService()
}
