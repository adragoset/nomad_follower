package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	nomadApi "github.com/hashicorp/nomad/api"
)

//FollowedTask a container for a followed task log process
type FollowedTask struct {
	Alloc       *nomadApi.Allocation
	Client      *nomadApi.Client
	ErrorChan   *chan string
	OutputChan  *chan string
	Quit        chan struct{}
	ServiceTags []string
	Task        *nomadApi.Task
}

//NewFollowedTask creats a new followed task
func NewFollowedTask(alloc *nomadApi.Allocation, client *nomadApi.Client, errorChan *chan string, output *chan string, quit chan struct{}, task *nomadApi.Task) *FollowedTask {
	serviceTags := collectServiceTags(task.Services)
	return &FollowedTask{Alloc: alloc, Task: task, Quit: quit, ServiceTags: serviceTags, OutputChan: output}
}

//Start starts following a task for an allocation
func (ft FollowedTask) Start() {
	stdErrStream, stdErrErr := ft.Client.AllocFS().Logs(ft.Alloc, true, ft.Task.Name, "stderr", "start", 0, ft.Quit, &nomadApi.QueryOptions{})
	stdOutStream, stdOutErr := ft.Client.AllocFS().Logs(ft.Alloc, true, ft.Task.Name, "stderr", "start", 0, ft.Quit, &nomadApi.QueryOptions{})

	go func() {
		for {
			select {
			case stdErrMsg := <-stdErrStream:
				message, err := processMessage(stdErrMsg, ft)
				if err != nil {
					*ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
				} else {
					*ft.OutputChan <- message
				}

			case stdOutMsg := <-stdOutStream:
				message, err := processMessage(stdOutMsg, ft)
				if err != nil {
					*ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
				} else {
					*ft.OutputChan <- message
				}

			case errErr := <-stdErrErr:
				*ft.ErrorChan <- fmt.Sprintf("Error following stderr for Allocation:%s Task:%s Error:%s", ft.Alloc.ID, ft.Task.Name, errErr)

			case outErr := <-stdOutErr:
				*ft.ErrorChan <- fmt.Sprintf("Error following stdout for Allocation:%s Task:%s Error:%s", ft.Alloc.ID, ft.Task.Name, outErr)

			}
		}
	}()

}

func collectServiceTags(services []*nomadApi.Service) []string {
	result := make([]string, 0)

	for _, service := range services {
		result = append(result, service.Name)
	}
	return result
}

func processMessage(frame *nomadApi.StreamFrame, ft FollowedTask) (string, error) {
	n := bytes.IndexByte(frame.Data, 0)
	message := string(frame.Data[:n])

	if isJSON(message) {
		return addTagsJSON(ft.Alloc.ID, message, ft.ServiceTags)
	}

	return addTagsString(ft.Alloc.ID, message, ft.ServiceTags)
}

func isJSON(s string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

func getJSONMessage(s string) map[string]interface{} {
	var js map[string]interface{}
	json.Unmarshal([]byte(s), &js)

	return js
}

func addTagsJSON(allocid string, message string, serviceTags []string) (string, error) {
	js := getJSONMessage(message)

	js["syslog.appname"] = strings.Join(serviceTags[:], ",")
	js["allocid"] = allocid

	result, err := json.Marshal(js)

	if err != nil {
		return "", err
	}

	n := bytes.IndexByte(result, 0)
	return string(result[:n]), nil
}

func addTagsString(allocid string, message string, serviceTags []string) (string, error) {
	js := make(map[string]interface{})
	js["message"] = message
	js["syslog.appname"] = strings.Join(serviceTags[:], ",")
	js["allocid"] = allocid

	result, err := json.Marshal(js)

	if err != nil {
		return "", err
	}

	n := bytes.IndexByte(result, 0)
	return string(result[:n]), nil
}
