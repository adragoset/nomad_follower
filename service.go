package main

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
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
