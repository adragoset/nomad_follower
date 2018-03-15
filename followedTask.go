package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
func (ft *FollowedTask) Start() {
	config := nomadApi.DefaultConfig()
	config.WaitTime = 5 * time.Minute
	client, err := nomadApi.NewClient(config)
	if err != nil {
		fmt.Println(fmt.Sprintf("{ \"message\":\"%s\"}", err))
	}

	fs := client.AllocFS()
	stdErrStream, stdErrErr := fs.Logs(ft.Alloc, true, ft.Task.Name, "stderr", "start", 0, ft.Quit, &nomadApi.QueryOptions{})
	stdOutStream, stdOutErr := fs.Logs(ft.Alloc, true, ft.Task.Name, "stderr", "start", 0, ft.Quit, &nomadApi.QueryOptions{})

	go func() {
		for {
			select {
			case stdErrMsg := <-stdErrStream:
				messages, err := processMessage(stdErrMsg, ft)
				if err != nil {
					*ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
				} else {
					for _, message := range messages {
						*ft.OutputChan <- message
					}
				}

			case stdOutMsg := <-stdOutStream:
				messages, err := processMessage(stdOutMsg, ft)
				if err != nil {
					*ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
				} else {
					for _, message := range messages {
						*ft.OutputChan <- message
					}
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

func processMessage(frame *nomadApi.StreamFrame, ft *FollowedTask) ([]string, error) {
	messages := strings.Split(string(frame.Data[:]), "/n")
	jsons := make([]string, 0)
	for _, message := range messages {

		if isJSON(message) {
			json, err := addTagsJSON(ft.Alloc.ID, message, ft.ServiceTags)
			if err != nil {
				*ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
			} else {
				jsons = append(jsons, json)
			}
		}

		s, err := addTagsString(ft.Alloc.ID, message, ft.ServiceTags)
		if err != nil {
			*ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
		} else {
			jsons = append(jsons, s)
		}
	}

	return jsons, nil
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

	return string(result[:]), nil
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

	return string(result[:]), nil
}
