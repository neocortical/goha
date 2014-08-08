package goha

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	DefaultIface        = "127.0.0.1"
	DefaultInternalPort = 6090
	DefaultExternalPort = 6091
)

type Config struct {
	Name         string
	InternalAddr string
	ExternalAddr string
	JoinAddr     string
	Datadir      string
	Logdir       string
	LogLevel     LogLevel
}

func DefaultConfig() Config {
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))

	config := *new(Config)
	config.Name = fmt.Sprintf("node_%d", os.Getpid())
	config.InternalAddr = fmt.Sprintf("%s:%d", DefaultIface, DefaultInternalPort)
	config.ExternalAddr = fmt.Sprintf("%s:%d", DefaultIface, DefaultExternalPort)
	config.JoinAddr = ""
	config.Datadir = dir
	config.Logdir = dir
	config.LogLevel = LogLevelInfo

	return config
}

func (c Config) String() string {
	j, _ := json.Marshal(c)
	return string(j)
}
