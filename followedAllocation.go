package main

import (
	nomadApi "github.com/hashicorp/nomad/api"
)

//FollowedAllocation a container for a followed allocations log process
type FollowedAllocation struct {
	Alloc      *nomadApi.Allocation
	Client     *nomadApi.Client
	ErrorChan  *chan string
	OutputChan *chan string
	Quit       chan struct{}
	Tasks      []*FollowedTask
}

//NewFollowedAllocation creates a new followed allocation
func NewFollowedAllocation(alloc *nomadApi.Allocation, client *nomadApi.Client, errorChan *chan string, outChan *chan string) *FollowedAllocation {
	return &FollowedAllocation{Alloc: alloc, Client: client, ErrorChan: errorChan, OutputChan: outChan, Quit: make(chan struct{}), Tasks: make([]*FollowedTask, 0)}
}

//Start starts following an allocation
func (f *FollowedAllocation) Start() {
	for _, tg := range f.Alloc.Job.TaskGroups {
		for _, task := range tg.Tasks {
			ft := NewFollowedTask(f.Alloc, f.Client, f.ErrorChan, f.OutputChan, f.Quit, task)
			ft.Start()
			f.Tasks = append(f.Tasks, ft)
		}
	}
}

//Stop stops tailing all allocation tasks
func (f *FollowedAllocation) Stop() {
	close(f.Quit)
}
