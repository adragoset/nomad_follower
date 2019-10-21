package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
)

var SaveFormatVersion = 1

type SavePoint struct {
	NodeID string `json:"node_id"`
	SaveFormatVersion int `json:"save_format_version"`
	SavedAllocs map[string]SavedAlloc `json:"saved_allocs"`
}

type SavedAlloc struct {
	ID string `json:"alloc_id"`
	SavedTasks map[string]SavedTask `json:"saved_tasks"`
}

type SavedTask struct {
	Key string `json:"key"`
	StdOutOffsets map[string]int64 `json:"stdout_offsets"`
	StdErrOffsets map[string]int64 `json:"stderr_offsets"`
}

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
func NewAllocationFollower(nomad NomadConfig, logger Logger) (a *AllocationFollower, e error) {
	return &AllocationFollower{
		Allocations: make(map[string]*FollowedAllocation),
		Nomad: nomad,
		NodeID: "",
		Quit: make(chan bool),
		log: logger,
	}, nil
}

// SetNodeID set the current Node's ID from the local Nomad agent.
func (a *AllocationFollower) SetNodeID() error {
	logContext := "AllocationFollower.SetNodeID"
	var err error
	var delay time.Duration
	var maxRetries = 3
	for retryCount := 1; retryCount <= maxRetries; retryCount++ {
		// reset err after each retry -- but leave final error set for return
		err = nil
		self, err := a.Nomad.Client().Agent().Self()
		if err != nil {
			a.log.Debugf(logContext, "Unable to query Nomad self endpoint, retry %d of %d", retryCount, maxRetries)
		} else {
			a.NodeID = self.Stats["client"]["node_id"]
			return nil
		}
	}
	a.log.Error(logContext, "Failed to query Nomad self endpoint, exiting")
	return err
}

//Start registers and de registers allocation followers
func (a *AllocationFollower) Start(duration time.Duration, savePath string) (<-chan string) {
	logContext := "AllocationFollower.Start"
	a.Ticker = time.NewTicker(duration)
	a.OutChan = make(chan string)

	go func() {
		defer a.Ticker.Stop()

		// handle first run special case items
		a.Nomad.RenewToken()
		err := a.SetNodeID()
		if err != nil {
			// cannot lookup allocations w/o node-id, hard fail
			a.log.Errorf(logContext, "Could not fetch NodeID: %s", err)
			close(a.OutChan)
			return
		}
		savePoint := a.restoreSavePoint(savePath)
		err = a.collectAllocations(savePoint)
		if err != nil {
			a.log.Debugf(
				logContext,
				"Error collecting allocations, first run: %s",
				err,
			)
		}
		// start routine loop
		for {
			select {
			case <-a.Ticker.C:
				err := a.collectAllocations(nil)
				if err != nil {
					a.log.Debugf(
						logContext,
						"Error collecting Allocations: %v",
						err,
					)
					// TODO differentiate 403 error from others
					a.Nomad.RenewToken()
				}
				a.createSavePoint(savePath)
			case <-a.Quit:
				a.log.Info(
					logContext,
					"Stopping Allocation Follower",
				)
				// TODO add a.Stop() here?
				//close(a.OutChan) // cannot do until we know there are no more senders on it
				return
			}
		}
	}()
	return a.OutChan
}

//Stop stops all followed allocations and exits
func (a *AllocationFollower) Stop() {
	a.Quit <- true

	for _, v := range a.Allocations {
		v.Stop()
	}
}

func (a *AllocationFollower) createSavePoint(path string) {
	logContext := "AllocationFollower.buildSavePoint"
	a.log.Debug(logContext, "Building save file from allocations")
	savePoint := SavePoint{}
	savePoint.NodeID = a.NodeID
	savePoint.SaveFormatVersion = SaveFormatVersion
	savePoint.SavedAllocs = make(map[string]SavedAlloc, 0)
	for allocId, alloc := range a.Allocations {
		allocSave := SavedAlloc{}
		allocSave.ID = allocId
		allocSave.SavedTasks = make(map[string]SavedTask, 0)

		for _, task := range alloc.Tasks {
			taskSave := SavedTask{}
			taskSave.StdErrOffsets = make(map[string]int64)
			taskSave.StdOutOffsets = make(map[string]int64)
			taskSave.Key = fmt.Sprintf(
				"%s:%s",
				task.TaskGroup,
				task.Task.Name,
			)
			taskSave.StdErrOffsets = task.errState.FileOffsets
			taskSave.StdOutOffsets = task.outState.FileOffsets
			allocSave.SavedTasks[taskSave.Key] = taskSave
		}
		savePoint.SavedAllocs[allocId] = allocSave
	}
	data, err := json.Marshal(savePoint)
	if err != nil {
		a.log.Errorf(
			logContext,
			"Cannot create save file, JSON marshal err: %s",
			err,
		)
		return
	}
	err = ioutil.WriteFile(path, data, 0644)
	if err != nil {
		a.log.Errorf(
			logContext,
			"Cannot create save file, IO error: %s",
			err,
		)
		return
	}
}

func (a *AllocationFollower) restoreSavePoint(path string) *SavePoint {
	logContext := "AllocationFollower.restoreSavePoint"
	a.log.Debugf(logContext, "Attempting to restore save file from: %s", path)
	savePoint := SavePoint{}
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	jsonDecoder := json.NewDecoder(file)
	err = jsonDecoder.Decode(&savePoint)
	if err != nil {
		return nil
	}
	if savePoint.NodeID != a.NodeID {
		a.log.Errorf(
			logContext,
			"Cannot restore save from '%s' NodeID differs.",
			path,
		)
		return nil
	}
	if savePoint.SaveFormatVersion != SaveFormatVersion {
		a.log.Errorf(
			logContext,
			"Cannot restore save from '%s' version %d != %d",
			path,
			savePoint.SaveFormatVersion,
			SaveFormatVersion,
		)
		return nil
	}
	a.log.Infof(
		logContext,
		"Restored save from file '%s'",
		path,
	)
	return &savePoint
}

func (a *AllocationFollower) collectAllocations(save *SavePoint) error {
	a.log.Debug("AllocationFollower.collectAllocations", "Collecting allocations")
	nodeReader := a.Nomad.Client().Nodes()
	allocs, _, err := nodeReader.Allocations(a.NodeID, &nomadApi.QueryOptions{})

	if err != nil {
		return err
	}

	for _, alloc := range allocs {
		record := a.Allocations[alloc.ID]
		runState := alloc.DesiredStatus == "run" || alloc.ClientStatus == "running"
		if record == nil && runState {
			// handle new alloc records w/ potentially saved state
			falloc := NewFollowedAllocation(alloc, a.Nomad, a.OutChan, a.log)
			if save != nil {
				a.log.Debug("AllocationFollower.collectAllocations", "Restoring saved allocations")
				savedAlloc := save.SavedAllocs[alloc.ID]
				falloc.Start(&savedAlloc)
			} else {
				falloc.Start(nil)
			}
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
		runState := alloc.DesiredStatus == "run" || alloc.ClientStatus == "running"
		if alloc.ID == id && runState {
			return true
		}
	}
	return false
}
