package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bcampbell/fuzzytime"
	nomadApi "github.com/hashicorp/nomad/api"
)

//FollowedTask a container for a followed task log process
type FollowedTask struct {
	Alloc       *nomadApi.Allocation
	Client      *nomadApi.Client
	ErrorChan   chan string
	OutputChan  chan string
	Quit        chan struct{}
	ServiceTags []string
	Task        *nomadApi.Task
}

//NewFollowedTask creats a new followed task
func NewFollowedTask(alloc *nomadApi.Allocation, client *nomadApi.Client, errorChan chan string, output chan string, quit chan struct{}, task *nomadApi.Task) *FollowedTask {
	serviceTags := collectServiceTags(task.Services)
	return &FollowedTask{Alloc: alloc, Task: task, Quit: quit, ServiceTags: serviceTags, OutputChan: output}
}

//Start starts following a task for an allocation
func (ft *FollowedTask) Start() {
	config := nomadApi.DefaultConfig()
	config.WaitTime = 5 * time.Minute
	client, err := nomadApi.NewClient(config)
	if err != nil {
		// TODO review -- this should be json in json given output wrapping + needs a date
		ft.ErrorChan <- fmt.Sprintf("{ \"message\":\"%s\"}", err)
	}

	fs := client.AllocFS()
	stdErrStream, stdErrErr := fs.Logs(ft.Alloc, true, ft.Task.Name, "stderr", "start", 0, ft.Quit, &nomadApi.QueryOptions{})
	stdOutStream, stdOutErr := fs.Logs(ft.Alloc, true, ft.Task.Name, "stdout", "start", 0, ft.Quit, &nomadApi.QueryOptions{})

	go func() {
		var err error
		var messages []string
		// buffers to hold partial entries while waiting for more data
		// TODO set capacity as top level constant, maybe 100? depends heavily on log volume
		var multilineOutBuf = make([]string, 0, 10)
		var multilineErrBuf = make([]string, 0, 10)

		for {
			select {
			case _, ok := <-ft.Quit:
				if !ok {
					return
				}
			case stdErrMsg, stdErrOk := <-stdErrStream:
				if stdErrOk {
					messages, multilineErrBuf, err = processMessage(stdErrMsg, ft, multilineErrBuf)
					if err != nil {
						ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
					} else {
						for _, message := range messages {
							ft.OutputChan <- message
						}
					}
				} else {
					// TODO before re-initializing flush multiline buf
					// TODO keep offset to minimize writing duplicate logs
					stdErrStream, stdErrErr = fs.Logs(ft.Alloc, true, ft.Task.Name, "stderr", "start", 0, ft.Quit, &nomadApi.QueryOptions{})
				}

			case stdOutMsg, stdOutOk := <-stdOutStream:
				if stdOutOk {
					messages, multilineOutBuf, err = processMessage(stdOutMsg, ft, multilineOutBuf)
					if err != nil {
						ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
					} else {
						for _, message := range messages {
							ft.OutputChan <- message
						}
					}
				} else {
					// TODO before re-initializing flush multiline buf
					// TODO keep offset to minimize writing duplicate logs
					stdOutStream, stdOutErr = fs.Logs(ft.Alloc, true, ft.Task.Name, "stdout", "start", 0, ft.Quit, &nomadApi.QueryOptions{})
				}

			case errErr := <-stdErrErr:
				ft.ErrorChan <- fmt.Sprintf("Error following stderr for Allocation:%s Task:%s Error:%s", ft.Alloc.ID, ft.Task.Name, errErr)

			case outErr := <-stdOutErr:
				ft.ErrorChan <- fmt.Sprintf("Error following stdout for Allocation:%s Task:%s Error:%s", ft.Alloc.ID, ft.Task.Name, outErr)
			default:
				//message := fmt.Sprintf("Processing Allocation:%s ID:%s Task:%s", ft.Alloc.Name, ft.Alloc.ID, ft.Task.Name)
				//message = fmt.Sprintf("{ \"message\":\"%s\"}", message)
				//_, _ = fmt.Println(message)
				// TODO maybe able to get rid of this default clause entirely
				time.Sleep(10 * time.Second)
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

// json logs must be on a single line to be properly parsed aka newlines escaped if included
// needs to return multiline buf to be passed to next run
// pseudo code for non-single-line json logs
// if has a timestamp
//   if multiline buf is empty
//     add to multiline buf + continue
//   else
//     flush multiline buf as json
//     add to multiline buf + continue
// else
//   if multiline buf is empty
//     add frag header + line to multiline buf + continue
//   else
//     add line to multiline buf + continue
func processMessage(frame *nomadApi.StreamFrame, ft *FollowedTask, multilineBuf []string) ([]string, []string, error) {
	initialTimestamp := fuzzytime.DateTime{}
	lastTimestamp := initialTimestamp.ISOFormat()
	messages := strings.Split(string(frame.Data[:]), "\n")
	// TODO set capacity as top level constant, maybe 100? depends heavily on log volume
	jsons := make([]string, 0, 10)
	for _, message := range messages {
		if message != "" && message != "\n" {
			if isJSON(message) {
				fmt.Printf("found single-line json log: %s", message)
				// no multi-line buffering for valid single-line json
				// TODO add wrap json function + struct for consistent fields, etc
				json, err := addTagsJSON(ft.Alloc.ID, message, ft.ServiceTags)
				if err != nil {
					ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
				} else {
					jsons = append(jsons, json)
				}
			} else {
				timestamp := findTimestamp(message)
				if timestamp != "" {
					fmt.Printf("Found message with timestamp: %s, msg: %s\n", timestamp, message)
					lastTimestamp = timestamp
					if len(multilineBuf) == 0 {
						fmt.Printf("beginning multiline, msg: %s\n", message)
						multilineBuf = append(multilineBuf, message)
					} else {
						// flush multiline buf + start over
						// TODO add timestamp to json log
						fmt.Printf("flushing multiline:\n%s\n, new multiline: %s\n", multilineBuf, message)
						s, err := createJsonLog(ft.Alloc.ID, multilineBuf, ft.ServiceTags)
						if err != nil {
							// dropping log data here, add DLQ?
							ft.ErrorChan <- fmt.Sprintf("Error building json log message: %v", err)
						} else {
							jsons = append(jsons, s)
						}
						multilineBuf = make([]string, 0, 10)
						multilineBuf = append(multilineBuf, message)
					}
				} else {
					if len(multilineBuf) == 0 {
						// log fragment case, something wrong with parsing?
						fmt.Printf("log fragment case, msg: %s\n", message)
						header := createFragmentHeader(lastTimestamp)
						multilineBuf = append(multilineBuf, header)
						multilineBuf = append(multilineBuf, message)
					} else {
						fmt.Printf("appending to existing multiline, msg: %s\n", message)
						multilineBuf = append(multilineBuf, message)
					}
				}
				//s, err := addTagsString(ft.Alloc.ID, message, ft.ServiceTags)
				//if err != nil {
				//	ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
				//} else {
				//	jsons = append(jsons, s)
				//}
			}
		}
	}
	return jsons, multilineBuf, nil
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

	js["service_name"] = strings.Join(serviceTags[:], ",")
	js["allocid"] = allocid

	result, err := json.Marshal(js)

	if err != nil {
		return "", err
	}

	return string(result[:]), nil
}

func createFragmentHeader(timestamp string) string {
	return fmt.Sprintf("%s Log Fragment Header")
}

func createJsonLog(allocid string, messages, serviceTags []string) (string, error) {
	js := make(map[string]interface{})
	js["service_name"] = strings.Join(serviceTags[:], ",")
	js["allocid"] = allocid
	js["message"] = strings.Join(messages, "\n")

	result, err := json.Marshal(js)
	if err != nil {
		return "", err
	}
	return string(result[:]), nil
}

func addTagsString(allocid string, message string, serviceTags []string) (string, error) {
	js := make(map[string]interface{})
	js["message"] = message
	js["service_name"] = strings.Join(serviceTags[:], ",")
	js["allocid"] = allocid

	result, err := json.Marshal(js)

	if err != nil {
		return "", err
	}

	return string(result[:]), nil
}

func findTimestamp(line string) string {
	var maxStartPos = 4
	t, spans, err := fuzzytime.Extract(line)
	if err != nil {
		return ""
	}
	// Add guard to ensure date + time are within X distance
	// e.g. if spans == 2 distance between span[0].end and span[1]
	// begin cannot be more than X
	if len(spans) > 0 {
		// Line does not start with timestamp, doesn't count
		if spans[0].Begin > maxStartPos {
			return ""
		}
	}
	return t.ISOFormat()
}
