package main

import (
	"fmt"
	"log"
	"os"

	nomadApi "github.com/hashicorp/nomad/api"
	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	//setup the out channel and error channel
	outChan := make(chan string, 1)
	errChan := make(chan string, 1)

	fileLogger := log.New(&lumberjack.Logger{
		Filename:   os.Getenv("LOG_FILE"),
		MaxSize:    50, // megabytes
		MaxBackups: 1,
		MaxAge:     1, //days
	}, "", 0)

	config := nomadApi.DefaultConfig()

	client, err := nomadApi.NewClient(config)

	if err != nil {
		errChan <- fmt.Sprintf("Error occoured configuring nomad api Error:%v", err)
	}

	af, err := NewAllocationFollower(client, &outChan, &errChan)

	if err != nil {
		errChan <- fmt.Sprintf("Error occoured starting AllocationFollower Error:%v", err)
	}

	for {
		select {
		case message := <-*af.OutChan:
			fileLogger.Println(message)

		case err := <-errChan:
			fmt.Printf("{ \"message\":\"%s\"}", err)
		}
	}
}
