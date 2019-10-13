package main

import (
	"log"
	"os"
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
	"gopkg.in/natefinch/lumberjack.v2"
)

var DEFAULT_CIRCUIT_BREAK = 60 * time.Second


func main() {
	//setup the output channel
	outChan := make(chan string)
	logger := Logger{verbosity: 20}

	createLogFile(logger)
	fileLogger := log.New(&lumberjack.Logger{
		Filename:   os.Getenv("LOG_FILE"),
		MaxSize:    50,
		MaxBackups: 1,
		MaxAge:     1,
	}, "", 0)

	config := nomadApi.DefaultConfig()
	config.WaitTime = 5 * time.Minute

	var nomadConfig NomadConfig
	nomadTokenBackend := os.Getenv("NOMAD_TOKEN_BACKEND")
	if nomadTokenBackend == "" {
		nomadConfig = NewNomadEnvAuth(config, logger)
	} else {
		nomadConfig = NewNomadRenewableAuth(
			config,
			nil,
			nomadTokenBackend,
			DEFAULT_CIRCUIT_BREAK,
			logger,
		)
	}

	af, err := NewAllocationFollower(nomadConfig, outChan, logger)
	if err != nil {
		logger.Errorf("main", "Error creating Allocation Follower: %s", err)
		return
	}

	af.Start(time.Second * 30)

	if af != nil {
		for {
			select {
			case message := <-af.OutChan:
				fileLogger.Println(message)
			}
		}
	}
}

func createLogFile(logger Logger) {
	path := os.Getenv("LOG_FILE")
	// detect if file exists
	var _, err = os.Stat(path)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(path)
		if err != nil {
			logger.Infof("createLogFile", "Error creating log file Error: %v", err)
			return
		}
		defer file.Close()
	}
	logger.Infof("createLogFile", "Created log file: %s", path)
}
