package main

import (
	"fmt"
	"log"
	"os"
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {

	createLogFile()

	//setup the out channel and error channel
	outChan := make(chan string)
	errChan := make(chan string)

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
		nomadConfig = NewNomadEnvAuth(errChan, config)
	} else {
		nomadConfig = NewNomadRenewableAuth(
			nomadTokenBackend,
			errChan,
			config,
			nil,
		)
	}

	af, err := NewAllocationFollower(nomadConfig, outChan, errChan)
	if err != nil {
		fmt.Println(fmt.Sprintf("{ \"message\":\"%s\"}", err))
		return
	}

	af.Start(time.Second * 30)

	if af != nil {
		for {
			select {
			case message := <-af.OutChan:
				fileLogger.Println(message)

			case err := <-af.ErrorChan:
				fmt.Println(fmt.Sprintf("{ \"message\":\"%s\"}", err))
			}
		}
	}
}

func createLogFile() {
	path := os.Getenv("LOG_FILE")
	// detect if file exists
	var _, err = os.Stat(path)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(path)
		if err != nil {
			fmt.Errorf("Error creating log file Error:%v", err)
		}
		defer file.Close()
	}

	fmt.Printf("{ \"message\":\"created log file: %s\"}\n", path)
}
