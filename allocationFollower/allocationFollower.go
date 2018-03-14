package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
)

//AllocationFollower a container for the list of followed allocations
type AllocationFollower struct {
	Allocations map[string]FollowedAllocation
	Config      *nomadApi.Config
	Client      *nomadApi.Client
	ErrorChan   *chan string
	NodeID      string
	OutChan     *chan string
	Quit        chan bool
	Ticker      *time.Ticker
}

//NewAllocationFollower Creates a new allocation follower
func NewAllocationFollower(outChan *chan string, errorChan *chan string) (a *AllocationFollower, e error) {

	config := nomadApi.DefaultConfig()
	config.Address = os.Getenv("NOMAD_ADDR")
	config.Region = os.Getenv("NOMAD_REGION")
	config.Namespace = os.Getenv("NOMAD_NAMESPACE")
	config.TLSConfig.CACert = os.Getenv("NOMAD_CACERT")
	config.TLSConfig.ClientCert = os.Getenv("NOMAD_CLIENT_CERT")
	config.TLSConfig.ClientKey = os.Getenv("NOMAD_CLIENT_KEY")
	config.TLSConfig.Insecure, _ = strconv.ParseBool(os.Getenv("NOMAD_SKIP_VERIFY"))
	config.WaitTime = 5 * time.Minute

	client, err := nomadApi.NewClient(config)

	agent := client.Agent()
	self, err := agent.Self()

	if err != nil {
		return nil, err
	}

	id := self.Stats["client"]["node_id"]
	return &AllocationFollower{Allocations: make(map[string]FollowedAllocation), Config: config, Client: client, ErrorChan: errorChan, NodeID: id, OutChan: outChan, Quit: make(chan bool)}, nil
}

//Start registers and de registers allocation followers
func (a AllocationFollower) Start(duration time.Duration) {
	a.Ticker = time.NewTicker(duration)
	tickChan := a.Ticker.C

	go func() {
		for {
			select {
			case <-tickChan:
				err := a.collectAllocations()
				if err != nil {
					*a.ErrorChan <- fmt.Sprintf("Error Collecting Allocations:%v", err)
				}
			case <-a.Quit:
				a.Ticker.Stop()
				return
			}
		}
	}()
}

//Stop stops all followed allocations and exits
func (a AllocationFollower) Stop() {
	a.Quit <- true

	for _, v := range a.Allocations {
		v.Stop()
	}
}

func (a AllocationFollower) collectAllocations() error {
	allocs, _, err := a.Client.Nodes().Allocations(a.NodeID, &nomadApi.QueryOptions{})

	if err != nil {
		return err
	}

	for _, alloc := range allocs {
		if _, ok := a.Allocations[alloc.ID]; !ok && alloc.DesiredStatus == "run" && alloc.ClientStatus == "running" {
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
		if alloc.ID == id && alloc.DesiredStatus == "run" && alloc.ClientStatus == "running" {
			return true
		}
	}
	return false
}
