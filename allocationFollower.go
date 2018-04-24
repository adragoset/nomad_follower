package main

import (
	"fmt"
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
)

//AllocationFollower a container for the list of followed allocations
type AllocationFollower struct {
	Allocations map[string]*FollowedAllocation
	Config      *nomadApi.Config
	Client      *nomadApi.Client
	ErrorChan   chan string
	NodeID      string
	OutChan     chan string
	Quit        chan bool
	Ticker      *time.Ticker
}

//NewAllocationFollower Creates a new allocation follower
func NewAllocationFollower(outChan chan string, errorChan chan string) (a *AllocationFollower, e error) {

	config := nomadApi.DefaultConfig()
	config.WaitTime = 5 * time.Minute

	client, err := nomadApi.NewClient(config)

	if err != nil {
		return nil, err
	}

	self, err := client.Agent().Self()

	if err != nil {
		return nil, err
	}

	id := self.Stats["client"]["node_id"]
	return &AllocationFollower{Allocations: make(map[string]*FollowedAllocation), Config: config, Client: client, ErrorChan: errorChan, NodeID: id, OutChan: outChan, Quit: make(chan bool)}, nil
}

//Start registers and de registers allocation followers
func (a *AllocationFollower) Start(duration time.Duration) {
	a.Ticker = time.NewTicker(duration)
	tickChan := a.Ticker.C

	go func() {
		for {
			select {
			case <-tickChan:
				_, _ = fmt.Printf("{ \"message\":\"%s\"}", "Collecting allocations")
				err := a.collectAllocations()
				if err != nil {
					a.ErrorChan <- fmt.Sprintf("Error Collecting Allocations:%v", err)
				}
			case <-a.Quit:
				a.Ticker.Stop()
				return
			}
		}
	}()
}

//Stop stops all followed allocations and exits
func (a *AllocationFollower) Stop() {
	a.Quit <- true

	for _, v := range a.Allocations {
		v.Stop()
	}
}

func (a *AllocationFollower) collectAllocations() error {
	allocs, _, err := a.Client.Nodes().Allocations(a.NodeID, &nomadApi.QueryOptions{})

	if err != nil {
		return err
	}

	for _, alloc := range allocs {
		record := a.Allocations[alloc.ID]
		if record == nil && (alloc.DesiredStatus == "run" || alloc.ClientStatus == "running") {
			message := fmt.Sprintf("Following Allocation: %s", alloc.Name)
			_, _ = fmt.Printf("{ \"message\":\"%s\"}", message)
			falloc := NewFollowedAllocation(alloc, a.Client, a.ErrorChan, a.OutChan)
			falloc.Start()
			a.Allocations[alloc.ID] = falloc
		}
	}

	for k, fa := range a.Allocations {
		if !containsValidAlloc(k, allocs) {
			fa.Stop()
			delete(a.Allocations, k)
		}
	}

	return nil
}

func containsValidAlloc(id string, allocs []*nomadApi.Allocation) bool {
	for _, alloc := range allocs {
		if alloc.ID == id && (alloc.DesiredStatus == "run" || alloc.ClientStatus == "running") {
			return true
		}
	}
	return false
}
