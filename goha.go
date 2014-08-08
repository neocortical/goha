package goha

import (
  "fmt"
  "strings"
)

func Start(config Config) (_ <-chan Callback, err error) {
  config.Name = strings.TrimSpace(config.Name)
  log, err := loggingInit(config.Logdir, config.LogLevel, config.Name)
  if err != nil {
    return nil, err
  }

  log.logInfo(fmt.Sprintf("Node startup from config: %s", config))

  cluster, cbChan := initCluster()

  self := Node{
    config.Name,
    cluster.GenerateNid(),
    config.InternalAddr,
    NodeStateActive,
    1,
  }
  err = cluster.AddSelf(self)
  if err != nil {
    log.logFatal(fmt.Sprintf("Error adding self to cluster: %v", err))
    return cbChan, err
  }

  go doService(cluster, log, config.JoinAddr)

  log.logInfo("Cluster started")

  // TODO: spawn external API service

  return cbChan, nil
}

func doService(cluster *cluster, log *Log, joinAddr string) {

  log.logInfo(fmt.Sprintf("Cluster initialized. We are NID: %d", cluster.GetSelf().Nid))

  gossipService := &GossipService{cluster, log}
  go gossipService.startGossip(joinAddr)
}
