package goha

import (
	"encoding/json"
)

type CallbackType string

const (
  CBInitError     CallbackType = "InitializationError"
	CBJoinedCluster CallbackType = "JoinedCluster"
  CBNodeJoined    CallbackType = "NodeJoined"
  CBNodeActive    CallbackType = "NodeActive"
  CBNodeInactive  CallbackType = "NodeInactive"
  CBNodeFailed    CallbackType = "NodeFailed"
  CBNodeDead      CallbackType = "NodeDead"
)

type Callback struct {
	Name CallbackType
	Node Node
}

func (c Callback) String() string {
	b, _ := json.Marshal(c)
	return string(b)
}
