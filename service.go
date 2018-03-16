package main

import (
	"fmt"
	"log"
	"os"
	"time"

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

	af, err := NewAllocationFollower(&outChan, &errChan)
	if err != nil {
		fmt.Println(fmt.Sprintf("{ \"message\":\"%s\"}", err))
	}

	af.Start(time.Second * 5)

	if af != nil {
		for {
			select {
			case message := <-*af.OutChan:
				fileLogger.Println(message)

			case err := <-*af.ErrorChan:
				fmt.Printf("{ \"message\":\"%s\"}", err)
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
		if isError(err) { return }
		defer file.Close()
	}

	fmt.Println("==> done creating log file", path)
}
