package goha

import (
	"fmt"
	l5g "github.com/neocortical/log5go"
)

type LogLevel uint16

const (
	LogLevelTrace LogLevel = LogLevel(l5g.LogTrace)
	LogLevelDebug LogLevel = LogLevel(l5g.LogDebug)
	LogLevelInfo  LogLevel = LogLevel(l5g.LogInfo)
	LogLevelWarn  LogLevel = LogLevel(l5g.LogWarn)
	LogLevelError LogLevel = LogLevel(l5g.LogError)
	LogLevelFatal LogLevel = LogLevel(l5g.LogFatal)
)

func loggingInit(logdir string, logLevel LogLevel, nodeName string) (_ l5g.Log5Go, err error) {
	logfname := fmt.Sprintf("%s.log", nodeName)
	log, err := l5g.Log(l5g.LogLevel(logLevel)).ToFile(logdir, logfname).WithRotation(l5g.RollDaily, 7).Register(nodeName)
	if err != nil {
		return nil, err
	}
	return log, nil
}
