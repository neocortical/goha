package goha

import (
  "bufio"
  "fmt"
  "os"
  "strings"
  "testing"
)

var logLevels []string = []string{ "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL" }
var messages []string = []string{ "trace", "debug", "info", "warn", "error", "fatal" }

func TestLogfile(t *testing.T) {
  log, err := loggingInit("/tmp", LogLevelTrace, "node1")
  if err != nil {
    t.Errorf("tried to init logging, but got an error: %v", err)
  }

  log.logTrace(messages[0])
  log.logDebug(messages[1])
  log.logInfo(messages[2])
  log.logWarn(messages[3])
  log.logError(messages[4])
  log.logFatal(messages[5])

  file, err := os.Open("/tmp/node1.log")
  if err != nil {
    t.Errorf("couldn't open log file: %v", err)
  }
  defer file.Close()

  reader := bufio.NewReader(file)
  for i := 0; i < 6; i++ {
    line, err := reader.ReadString('\n')
    if err != nil {
      t.Errorf("unexpected error reading from log file")
      return
    }
    suffix := fmt.Sprintf("%s: %s\n", logLevels[i], messages[i])
    if !strings.HasSuffix(line, suffix) {
      t.Errorf("expected log message to end in %s but line is %s", suffix, line)
    }
  }
}

func TestLogLevels(t *testing.T) {
  log, err := loggingInit("/tmp", LogLevelInfo, "node1")
  log.logTrace(messages[0])
  log.logDebug(messages[1])
  log.logInfo(messages[2])
  log.logWarn(messages[3])
  log.logError(messages[4])
  log.logFatal(messages[5])

  file, err := os.Open("/tmp/node1.log")
  if err != nil {
    t.Errorf("couldn't open log file: %v", err)
  }
  defer file.Close()

  reader := bufio.NewReader(file)
  for i := 2; i < 6; i++ {
    line, err := reader.ReadString('\n')
    if err != nil {
      t.Errorf("unexpected error reading from log file")
      return
    }
    suffix := fmt.Sprintf("%s: %s\n", logLevels[i], messages[i])
    if !strings.HasSuffix(line, suffix) {
      t.Errorf("expected log message to end in %s but line is %s", suffix, line)
    }
  }
  line, err := reader.ReadString('\n')
  if err == nil {
    t.Errorf("should have gotten EOF, but got line %s", line)
  }
}
