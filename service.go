package main

import (
	"log"
	"os"
	"strconv"
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
	"gopkg.in/natefinch/lumberjack.v2"
)

var DEFAULT_CIRCUIT_BREAK = 60 * time.Second

var DEFAULT_LOG_FILE = "nomad.log"
var DEFAULT_SAVE_FILE = "nomad-follower.json"

var MAX_LOG_SIZE = 50
var MAX_LOG_BACKUPS = 1
var MAX_LOG_AGE = 1

var NOMAD_MAX_WAIT = 5 * time.Minute
var ALLOC_REFRESH_TICK = time.Second * 30

var DEFAULT_VERBOSITY = INFO

func main() {
	var verbosity LogLevel
	verbose := os.Getenv("VERBOSE")
	i, err := strconv.Atoi(verbose)
	if err != nil {
		verbosity = DEFAULT_VERBOSITY
	} else {
		verbosity = LogLevel(i)
	}
	logger := Logger{verbosity: verbosity}

	logFile := os.Getenv("LOG_FILE")
	if logFile == "" {
		logFile = DEFAULT_LOG_FILE
	}

	saveFile := os.Getenv("SAVE_FILE")
	if saveFile == "" {
		saveFile = DEFAULT_SAVE_FILE
	}

	createLogFile(logFile, logger)
	fileLogger := log.New(&lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    MAX_LOG_SIZE,
		MaxBackups: MAX_LOG_BACKUPS,
		MaxAge:     MAX_LOG_AGE,
	}, "", 0)

	config := nomadApi.DefaultConfig()
	config.WaitTime = NOMAD_MAX_WAIT

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

	af, err := NewAllocationFollower(nomadConfig, logger)
	if err != nil {
		logger.Errorf("main", "Error creating Allocation Follower: %s", err)
		return
	}

	outChan := af.Start(ALLOC_REFRESH_TICK, saveFile)

	if af != nil {
		for {
			select {
			case message, ok := <-outChan:
				if ok {
					fileLogger.Println(message)
				} else {
					logger.Info("main", "Allocation Follower fatal error, exiting.")
					return
				}
			}
		}
	}
}

func createLogFile(logFile string, logger Logger) {
	// detect if file exists
	var _, err = os.Stat(logFile)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(logFile)
		if err != nil {
			logger.Infof(
				"createLogFile",
				"Error creating log file Error: %v",
				err,
			)
			return
		}
		defer file.Close()
	}
	logger.Infof("createLogFile", "Created log file: %s", logFile)
}
