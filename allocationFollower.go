package main

import (
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
)

//AllocationFollower a container for the list of followed allocations
type AllocationFollower struct {
	Allocations map[string]*FollowedAllocation
	Nomad       NomadConfig
	NodeID      string
	OutChan     chan string
	Quit        chan bool
	Ticker      *time.Ticker
	log         Logger
}

//NewAllocationFollower Creates a new allocation follower
func NewAllocationFollower(nomad NomadConfig, outChan chan string, logger Logger) (a *AllocationFollower, e error) {
	return &AllocationFollower{
		Allocations: make(map[string]*FollowedAllocation),
		Nomad: nomad,
		NodeID: "",
		OutChan: outChan,
		Quit: make(chan bool),
		log: logger,
	}, nil
}

// SetNodeID set the current Node's ID from the local Nomad agent.
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
	logContext := "AllocationFollower.Start"
	a.Ticker = time.NewTicker(duration)

	go func() {
		defer a.Ticker.Stop()
		// TODO add defer a.Stop() ?

		a.Nomad.RenewToken()
		err := a.SetNodeID()
		if err != nil {
			// cannot lookup allocations w/o node-id, hard fail
			a.log.Errorf(logContext, "Could not fetch NodeID: %s", err)
			return
		}
		for {
			select {
			case <-a.Ticker.C:
				err := a.collectAllocations()
				if err != nil {
					a.log.Debugf(logContext, "Error Collecting Allocations: %v", err)
					// TODO determine if any good way to differentiate 403 from other?
					a.Nomad.RenewToken()
				}
			case <-a.Quit:
				a.log.Info(logContext, "Stopping Allocation Follower")
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
	a.log.Debug("AllocationFollower.collectAllocations", "Collecting Allocations")
	nodeReader := a.Nomad.Client().Nodes()
	allocs, _, err := nodeReader.Allocations(a.NodeID, &nomadApi.QueryOptions{})

	if err != nil {
		return err
	}

	for _, alloc := range allocs {
		record := a.Allocations[alloc.ID]
		if record == nil && (alloc.DesiredStatus == "run" || alloc.ClientStatus == "running") {
			falloc := NewFollowedAllocation(alloc, a.Nomad, a.OutChan, a.log)
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
