package main

import (
	"time"

	"github.com/adragoset/nomad_follower/allocationFollower"
	nomadApi "github.com/hashicorp/nomad/api"
)

func main() {
	outChan := make(chan string)
	errChan := make(chan string)
	config := nomadApi.DefaultConfig()
	config.WaitTime = 5 * time.Minute

	client, err := nomadApi.NewClient(config)

	if err != nil {

	}

	allocFollower, err := allocationFollower.NewAllocationFollower(client, outChan, errChan)
}
