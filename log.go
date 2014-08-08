package goha

import (
	"fmt"
	"log"
	"os"
)

type LogLevel uint8

const (
	LogLevelTrace LogLevel = 0
	LogLevelDebug LogLevel = 1
	LogLevelInfo  LogLevel = 2
	LogLevelWarn  LogLevel = 3
	LogLevelError LogLevel = 4
	LogLevelFatal LogLevel = 5
)

type Log struct {
	logger   *log.Logger
	logLevel LogLevel
}

func loggingInit(logdir string, logLevel LogLevel, nid string) (_ *Log, err error) {
	logfname := fmt.Sprintf("%s/%s.log", logdir, nid)
	logfile, err := os.Create(logfname)
	if err != nil {
		return nil, err
	}
	logger := log.New(logfile, "", log.LstdFlags)
	return &Log{logger, logLevel}, nil
}

func (l *Log) logTrace(msg string) {
	if l.logLevel == LogLevelTrace {
		l.logger.Println(fmt.Sprintf("TRACE: %s", msg))
	}
}

func (l *Log) logDebug(msg string) {
	if l.logLevel <= LogLevelDebug {
		l.logger.Println(fmt.Sprintf("DEBUG: %s", msg))
	}
}

func (l *Log) logInfo(msg string) {
	if l.logLevel <= LogLevelInfo {
		l.logger.Println(fmt.Sprintf("INFO: %s", msg))
	}
}

func (l *Log) logWarn(msg string) {
	if l.logLevel <= LogLevelWarn {
		l.logger.Println(fmt.Sprintf("WARN: %s", msg))
	}
}

func (l *Log) logError(msg string) {
	if l.logLevel <= LogLevelError {
		l.logger.Println(fmt.Sprintf("ERROR: %s", msg))
	}
}

func (l *Log) logFatal(msg string) {
	l.logger.Println(fmt.Sprintf("FATAL: %s", msg))
}
