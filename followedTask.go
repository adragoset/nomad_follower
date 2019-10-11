package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bcampbell/fuzzytime"
	nomadApi "github.com/hashicorp/nomad/api"
)

// INITIAL_OUTPUT_CAP is the slice capacity of the output logs before sent.
var INITIAL_OUTPUT_CAP = 10

// JSON_DATETIME_FIELDS lists fields searched in a JSON log for a timestamp.
var JSON_DATETIME_FIELDS = []string{"datetime", "timestamp", "time", "date"}

// MAX_TIMESTAMP_START_POS determines if a log timestamp signals a new multi-line log.
var MAX_TIMESTAMP_START_POS = 4

// MULTILINE_BUF_CAP is the slice capacity of new buffers to aggregate multi-line logs.
var MULTILINE_BUF_CAP = 10

// NomadLog annotates task log data with metadata from Nomad about the task
// and allocation.  A timestamp is parsed out of the message body and string log
// ouput is preserved under the 'message' field and JSON log output is nested
// under the 'data' field.
type NomadLog struct {
	AllocId string `json:"alloc_id"`
	JobName string `json:"job_name"`
	NodeName string `json:"node_name"`
	ServiceName string `json:"service_name"`
	ServiceTags []string `json:"service_tags"`
	ServiceTagMap map[string]string `json:"service_tag_map"`
	TaskName string `json:"task_name"`
	// these all set at log time
	Timestamp string `json:"timestamp"`
	Message string `json:"message"`
	Data map[string]interface{} `json:"data"`
}

//FollowedTask a container for a followed task log process
type FollowedTask struct {
	Alloc       *nomadApi.Allocation
	Nomad       NomadConfig
	ErrorChan   chan string
	OutputChan  chan string
	Quit        chan struct{}
	Task        *nomadApi.Task
	LogTemplate NomadLog
}

//NewFollowedTask creates a new followed task
func NewFollowedTask(alloc *nomadApi.Allocation, nomad NomadConfig, errorChan chan string, output chan string, quit chan struct{}, task *nomadApi.Task) *FollowedTask {
	logTemplate := createLogTemplate(alloc, task)
	return &FollowedTask{
		Alloc: alloc,
		Nomad: nomad,
		ErrorChan: errorChan,
		OutputChan: output,
		Quit: quit,
		Task: task,
		LogTemplate: logTemplate,
	}
}

type StreamState struct {
	MultiLineBuf []string
	LastTimestamp string
	ConsecErrors uint
	FileOffset int64
	// internal use only
	initialBufferCap int
	quit chan struct{}
	allocFS *nomadApi.AllocFS
}

func (s *StreamState) BufAdd(msg string) {
	s.MultiLineBuf = append(s.MultiLineBuf, msg)
}

func (s *StreamState) BufReset() {
	s.MultiLineBuf = make([]string, 0, s.initialBufferCap)
}

func (s *StreamState) Start(alloc *nomadApi.Allocation, task, logType string) (<-chan *nomadApi.StreamFrame, <-chan error) {
	return s.allocFS.Logs(alloc, true, task, logType, "start", s.FileOffset, s.quit, &nomadApi.QueryOptions{})
}

func NewStreamState(allocFS *nomadApi.AllocFS, quit chan struct{}, initialBufferCap int) StreamState {
	s := StreamState{}
	s.initialBufferCap = initialBufferCap
	s.quit = quit
	s.allocFS = allocFS
	s.MultiLineBuf = make([]string, 0, initialBufferCap)
	return s
}

//Start starts following a task for an allocation
func (ft *FollowedTask) Start() {
	// TODO re-add as separate client once testing finished
	//config := nomadApi.DefaultConfig()
	//config.WaitTime = 5 * time.Minute
	//client, err := nomadApi.NewClient(config)
	//if err != nil {
		// TODO review -- this should be json in json given output wrapping + needs a date
		//ft.ErrorChan <- fmt.Sprintf("{ \"message\":\"%s\"}", err)
	//}
	//fs := client.AllocFS()

	fs := ft.Nomad.Client().AllocFS()
	var stdOutState = NewStreamState(fs, ft.Quit, MULTILINE_BUF_CAP)
	var stdErrState = NewStreamState(fs, ft.Quit, MULTILINE_BUF_CAP)
	stdOutStream, stdOutErr := stdOutState.Start(ft.Alloc, ft.Task.Name, "stdout")
	stdErrStream, stdErrErr := stdErrState.Start(ft.Alloc, ft.Task.Name, "stderr")

	go func() {
		var messages []string
		for {
			// TODO need some max possible sleep duration
			// Keeps dead allocs from crash looping (until 403/404 error differentiated)
			if stdOutState.ConsecErrors > 0 || stdErrState.ConsecErrors > 0 {
				delay := 1 << stdOutState.ConsecErrors + 1 << stdErrState.ConsecErrors
				ft.ErrorChan <- fmt.Sprintf("Inserting delay of %ds for allocation: %s task: %s", delay, ft.Alloc.ID, ft.Task.Name)
				time.Sleep(time.Duration(delay) * time.Second)
			}

			select {
			case _, ok := <-ft.Quit:
				// TODO handle quit case gracefully, flush buffer to out chan
				if !ok {
					return
				}
			case stdErrMsg, stdErrOk := <-stdErrStream:
				if stdErrOk {
					messages, stdErrState = ft.processMessage(stdErrMsg, stdErrState)
					for _, message := range messages {
						ft.OutputChan <- message
					}
					stdErrState.FileOffset = stdErrMsg.Offset
					stdErrState.ConsecErrors = 0
				} else {
					stdErrStream, stdErrErr = stdErrState.Start(ft.Alloc, ft.Task.Name, "stderr")
					stdErrState.ConsecErrors += 1
				}

			case stdOutMsg, stdOutOk := <-stdOutStream:
				if stdOutOk {
					messages, stdOutState = ft.processMessage(stdOutMsg, stdOutState)
					for _, message := range messages {
						ft.OutputChan <- message
					}
					stdOutState.FileOffset = stdOutMsg.Offset
					stdOutState.ConsecErrors = 0
				} else {
					stdOutStream, stdOutErr = stdOutState.Start(ft.Alloc, ft.Task.Name, "stdout")
					stdOutState.ConsecErrors += 1
				}

			case errErr := <-stdErrErr:
				ft.ErrorChan <- fmt.Sprintf("Error following stderr for Allocation:%s Task:%s Error:%s", ft.Alloc.ID, ft.Task.Name, errErr)
				// TODO handle 403 case separately from 404 case
				// Handle task starting while client has invalid token
				ft.Nomad.RenewToken()
				stdErrStream, stdErrErr = stdErrState.Start(ft.Alloc, ft.Task.Name, "stderr")
				stdErrState.ConsecErrors += 1

			case outErr := <-stdOutErr:
				ft.ErrorChan <- fmt.Sprintf("Error following stdout for Allocation:%s Task:%s Error:%s", ft.Alloc.ID, ft.Task.Name, outErr)
				// TODO handle 403 case separately from 404 case
				// Handle task starting while client has invalid token
				ft.Nomad.RenewToken()
				stdOutStream, stdOutErr = stdOutState.Start(ft.Alloc, ft.Task.Name, "stdout")
				stdOutState.ConsecErrors += 1
			}
		}
	}()
}

func createLogTemplate(alloc *nomadApi.Allocation, task *nomadApi.Task) NomadLog {
	tmpl := NomadLog{}
	service := nomadApi.Service{}

	tmpl.AllocId = alloc.ID
	tmpl.JobName = *alloc.Job.Name
	tmpl.NodeName = alloc.NodeName
	tmpl.ServiceTags = make([]string, 0)
	tmpl.ServiceTagMap = make(map[string]string)
	if len(task.Services) > 0 {
		// shouldn't have more than one service per task
		service = *task.Services[0]
		tmpl.ServiceName = service.Name
		tmpl.ServiceTags = service.Tags
		tmpl.ServiceTagMap = getServiceTagMap(service)
	} else {
		for _, tg := range alloc.Job.TaskGroups {
			if len(tg.Services) == 0 {
				continue
			}
			for _, t := range tg.Tasks {
				if t.Name == task.Name {
					service = *tg.Services[0]
					tmpl.ServiceName = service.Name
					tmpl.ServiceTags = service.Tags
					tmpl.ServiceTagMap = getServiceTagMap(service)
				}
			}
		}
	}
	tmpl.TaskName = task.Name
	tmpl.Data = make(map[string]interface{})
	return tmpl
}

func getServiceTagMap(service nomadApi.Service) (map[string]string) {
	var serviceTagMap = make(map[string]string)

	for _, t := range service.Tags {
		parts := strings.SplitN(t, "=", 2)
		if len(parts) == 2 {
			key := parts[0]
			val := parts[1]
			serviceTagMap[key] = val
		}
	}
	return serviceTagMap
}

// processMessage takes a single log line and determines if it is JSON, or a single or multi-line log.
//
// Requirements of operation:
// - JSON logs must be on a single line to be properly parsed aka newlines escaped if included
// - String logs must contain a datetime as close to the beginning of the line as possible
// - Multi-line string logs should not contain datetimes near the beginning of the string
//   to be grouped properly
//
// Pseudo code for multi-line string logs:
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
// Note: last timestamp and multiline buffer must be passed to and received from the calling
//       function to properly mark timestamps and group logs between calls.
func (ft *FollowedTask) processMessage(frame *nomadApi.StreamFrame, streamState StreamState) ([]string, StreamState) {
	messages := strings.Split(string(frame.Data[:]), "\n")
	jsons := make([]string, 0, INITIAL_OUTPUT_CAP)
	for _, message := range messages {
		if message != "" && message != "\n" {
			if isJSON(message) {
				//fmt.Printf("found single-line json log: %s", message)
				// no multi-line buffering for valid single-line json
				s, err := wrapJsonLog(ft.LogTemplate, message)
				if err != nil {
					// TODO dropping log data here, add DLQ?
					ft.ErrorChan <- fmt.Sprintf("Error building log message json Error:%v", err)
				} else {
					jsons = append(jsons, s)
				}
			} else {
				timestamp := findTimestamp(message)
				if timestamp != "" {
					//fmt.Printf("Found message with timestamp: %s, msg: %s\n", timestamp, message)
					if len(streamState.MultiLineBuf) == 0 {
						//fmt.Printf("beginning multiline, msg: %s\n", message)
						streamState.BufAdd(message)
					} else {
						// flush multiline buf + start over
						//fmt.Printf("flushing multiline:\n%s\n, new multiline: %s\n", multilineBuf, message)
						s, err := createJsonLog(ft.LogTemplate, streamState.MultiLineBuf, streamState.LastTimestamp)
						if err != nil {
							// TODO dropping log data here, add DLQ?
							ft.ErrorChan <- fmt.Sprintf("Error building json log message: %v", err)
						} else {
							jsons = append(jsons, s)
						}
						streamState.BufReset()
						streamState.BufAdd(message)
					}
					// don't update until prior multi-line flush uses it
					streamState.LastTimestamp = timestamp
				} else {
					if len(streamState.MultiLineBuf) == 0 {
						// log fragment case, something wrong with parsing?
						//fmt.Printf("log fragment case, msg: %s\n", message)
						header := createFragmentHeader(streamState.LastTimestamp)
						streamState.BufAdd(header)
						streamState.BufAdd(message)
					} else {
						//fmt.Printf("appending to existing multiline, msg: %s\n", message)
						streamState.BufAdd(message)
					}
				}
			}
		}
	}
	return jsons, streamState
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

func createFragmentHeader(timestamp string) string {
	return fmt.Sprintf("%s Log Fragment Header")
}

func wrapJsonLog(logTmpl NomadLog, line string) (string, error) {
	data := getJSONMessage(line)
	timestamp := findJsonTimestamp(data)
	logTmpl.Data = data
	logTmpl.Timestamp = timestamp

	result, err := json.Marshal(logTmpl)
	if err != nil {
		return "", err
	}
	return string(result[:]), nil
}

func createJsonLog(logTmpl NomadLog, lines []string, timestamp string) (string, error) {
	logTmpl.Message = strings.Join(lines, "\n")
	logTmpl.Timestamp = timestamp

	result, err := json.Marshal(logTmpl)
	if err != nil {
		return "", err
	}
	return string(result[:]), nil
}

// findJsonTimestamp looks at top level keys in a json log to find a timestamp.
//
// Returns the first parse-able timestamp in the list of JSON_DATETIME_FIELDS
// searched.
func findJsonTimestamp(data map[string]interface{}) string {
	for _, field := range JSON_DATETIME_FIELDS {
		value, ok := data[field]
		if ok {
			s, ok := value.(string)
			if !ok {
				continue
			}
			t, _, err := fuzzytime.Extract(s)
			if err != nil {
				return ""
			}
			if t.ISOFormat() != "" {
				return t.ISOFormat()
			}
		}
	}
	return ""
}

// findTimestamp determines if a string starts with a timestamp, and returns it.
func findTimestamp(line string) string {
	var maxStartPos = MAX_TIMESTAMP_START_POS
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
