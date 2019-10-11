package main

import (
	"fmt"
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
)

//AllocationFollower a container for the list of followed allocations
type AllocationFollower struct {
	Allocations map[string]*FollowedAllocation
	Nomad       NomadConfig
	ErrorChan   chan string
	NodeID      string
	OutChan     chan string
	Quit        chan bool
	Ticker      *time.Ticker
}

//NewAllocationFollower Creates a new allocation follower
func NewAllocationFollower(nomad NomadConfig, outChan chan string, errorChan chan string) (a *AllocationFollower, e error) {
	return &AllocationFollower{
		Allocations: make(map[string]*FollowedAllocation),
		Nomad: nomad,
		ErrorChan: errorChan,
		NodeID: "",
		OutChan: outChan,
		Quit: make(chan bool),
	}, nil
}

func (a *AllocationFollower) SetNodeID() error {
	self, err := a.Nomad.Client().Agent().Self()
	if err != nil {
		return err
	}
	a.NodeID = self.Stats["client"]["node_id"]
	return nil
}

//Start registers and de registers allocation followers
func (a *AllocationFollower) Start(duration time.Duration) {
	a.Ticker = time.NewTicker(duration)

	go func() {
		defer a.Ticker.Stop()

		// run here while errChan is being read to not deadlock
		a.Nomad.RenewToken()
		err := a.SetNodeID()
		if err != nil {
			// cannot lookup allocations w/o node-id, hard fail
			a.ErrorChan <- fmt.Sprintf("Could not fetch NodeID: %s", err)
			return
		}
		for {
			select {
			case <-a.Ticker.C:
				err := a.collectAllocations()
				if err != nil {
					a.ErrorChan <- fmt.Sprintf("Error Collecting Allocations:%v", err)
					// TODO determine if any good way to differentiate 403 from other?
					a.Nomad.RenewToken()
				}
			case <-a.Quit:
				message := fmt.Sprintf("{ \"message\":\"%s\"}", "Stopping Allocation Follower")
				_, _ = fmt.Println(message)
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
	message := fmt.Sprintf("Collecting Allocations")
	message = fmt.Sprintf("{ \"message\":\"%s\"}", message)
	_, _ = fmt.Println(message)

	nodeReader := a.Nomad.Client().Nodes()
	allocs, _, err := nodeReader.Allocations(a.NodeID, &nomadApi.QueryOptions{})

	if err != nil {
		return err
	}

	for _, alloc := range allocs {
		record := a.Allocations[alloc.ID]
		if record == nil && (alloc.DesiredStatus == "run" || alloc.ClientStatus == "running") {
			falloc := NewFollowedAllocation(alloc, a.Nomad, a.ErrorChan, a.OutChan)
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
