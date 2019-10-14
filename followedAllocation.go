package main

import (
	"fmt"
	nomadApi "github.com/hashicorp/nomad/api"
)

//FollowedAllocation a container for a followed allocations log process
type FollowedAllocation struct {
	Alloc      *nomadApi.Allocation
	Nomad      NomadConfig
	OutputChan chan string
	Quit       chan struct{}
	Tasks      []*FollowedTask
	log        Logger
}

//NewFollowedAllocation creates a new followed allocation
func NewFollowedAllocation(alloc *nomadApi.Allocation, nomad NomadConfig, outChan chan string, logger Logger) *FollowedAllocation {
	return &FollowedAllocation{
		Alloc: alloc,
		Nomad: nomad,
		OutputChan: outChan,
		Quit: make(chan struct{}),
		Tasks: make([]*FollowedTask, 0),
		log: logger,
	}
}

//Start starts following an allocation
func (f *FollowedAllocation) Start(save *SavedAlloc) {
	f.log.Debugf(
		"FollowedAllocation.Start",
		"Following Allocation: %s ID: %s",
		f.Alloc.Name,
		f.Alloc.ID,
	)
	for _, tg := range f.Alloc.Job.TaskGroups {
		for _, task := range tg.Tasks {
			ft := NewFollowedTask(f.Alloc, *tg.Name, task, f.Nomad, f.Quit, f.OutputChan, f.log)
			if save != nil {
				f.log.Debug("FollowedAllocation.Start", "Restoring saved allocation data")
				key := fmt.Sprintf("%s:%s", *tg.Name, task.Name)
				savedTask := save.SavedTasks[key]
				ft.Start(&savedTask)
			} else {
				ft.Start(nil)
			}
			f.Tasks = append(f.Tasks, ft)
		}
	}
}

//Stop stops tailing all allocation tasks
func (f *FollowedAllocation) Stop() {
	f.log.Debugf(
		"FollowedAllocation.Stop",
		"Stopping Allocation: %s ID: %s",
		f.Alloc.Name,
		f.Alloc.ID,
	)
	close(f.Quit)
}
