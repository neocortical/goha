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

  ch := make(chan Callback)

  go doService(config.InternalAddr, config.ExternalAddr, log, config, ch)

  log.logInfo("Node started")

  return ch, nil
}

func doService(intAddr string, extAddr string, log *Log, config Config, cbChan chan<- Callback) {
  cluster := initCluster()

  self := Node{
    config.Name,
    cluster.GenerateNid(),
    intAddr,
    NodeStateActive,
  }

  err := cluster.AddSelf(self)
  if err != nil {
    log.logFatal(fmt.Sprintf("Error adding self to cluster: %v", err))
    cb := Callback{CBInitError, self}
    cbChan <- cb
    return
  }
  log.logInfo(fmt.Sprintf("Cluster initialized. We are NID: %d", cluster.GetSelf().Nid))

  gossipService := &GossipService{cluster, log}
  go gossipService.startGossip(&cbChan, config.JoinAddr)

  // TODO: spawn external API service
}
