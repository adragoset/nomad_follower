package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dmwilcox/fuzzytime"
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

// BACKOFF_DELAY rate-limits streaming logs of tasks that error (BACKOFF_DELAY << error-count) seconds
var BACKOFF_DELAY = 8

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

func (n *NomadLog) ToJSON() (string, error) {
	result, err := json.Marshal(n)
	if err != nil {
		return "", err
	}
	return string(result[:]), nil
}

//FollowedTask a container for a followed task log process
type FollowedTask struct {
	Alloc       *nomadApi.Allocation
	TaskGroup   string
	Task        *nomadApi.Task
	Nomad       NomadConfig
	Quit        chan struct{}
	OutputChan  chan string
	log         Logger
	logTemplate NomadLog
	errState    StreamState
	outState    StreamState
}

//NewFollowedTask creates a new followed task
func NewFollowedTask(alloc *nomadApi.Allocation, taskGroup string, task *nomadApi.Task, nomad NomadConfig, quit chan struct{}, output chan string, logger Logger) *FollowedTask {
	logTemplate := createLogTemplate(alloc, task)
	return &FollowedTask{
		Alloc: alloc,
		TaskGroup: taskGroup,
		Task: task,
		Nomad: nomad,
		Quit: quit,
		OutputChan: output,
		log: logger,
		logTemplate: logTemplate,
	}
}

type StreamState struct {
	MultiLineBuf []string
	LastTimestamp string
	ConsecErrors uint
	FileOffsets map[string]int64
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

func (s *StreamState) SetOffsets(offsets map[string]int64) {
	s.FileOffsets = offsets
}

// GetOffset returns the current total log stream offset.
func (s *StreamState) GetOffset() int64 {
	var offset int64
	offset = 0
	// sum the byte offsets for all of the file streams
	for _, o := range s.FileOffsets {
		offset += o
	}
	return offset
}

func (s *StreamState) Start(alloc *nomadApi.Allocation, task, logType string) (<-chan *nomadApi.StreamFrame, <-chan error) {
	return s.allocFS.Logs(alloc, true, task, logType, "start", s.GetOffset(), s.quit, &nomadApi.QueryOptions{})
}

func NewStreamState(allocFS *nomadApi.AllocFS, quit chan struct{}, initialBufferCap int) StreamState {
	s := StreamState{}
	s.initialBufferCap = initialBufferCap
	s.quit = quit
	s.allocFS = allocFS
	s.MultiLineBuf = make([]string, 0, initialBufferCap)
	s.FileOffsets = make(map[string]int64)
	return s
}

// SpeculativeOffset returns the total log stream offset if file had a given offset.
// Function exists primarily for debugging purposes.
func SpeculativeOffset(state StreamState, file string, offset int64) int64 {
	var speculativeOffset int64
	speculativeOffset = offset
	for f, o := range state.FileOffsets {
		if f == file {
			continue
		}
		speculativeOffset += o
	}
	return speculativeOffset
}

// CalculateOffset returns the total log stream offset based on given message size.
// Function exists primarily for debugging purposes.
func CalculateOffset(state StreamState, file string, size int64) int64 {
	var calculatedOffset int64
	calculatedOffset = 0
	for f, o := range state.FileOffsets {
		if f == file {
			calculatedOffset += o + size
			continue
		}
		calculatedOffset += o
	}
	return calculatedOffset
}

//Start starts following a task for an allocation
func (ft *FollowedTask) Start(save *SavedTask) {
	//config := nomadApi.DefaultConfig()
	//config.WaitTime = 5 * time.Minute
	//client, err := nomadApi.NewClient(config)
	//if err != nil {
		//ft.ErrorChan <- fmt.Sprintf("{ \"message\":\"%s\"}", err)
	//}
	//fs := client.AllocFS()

	logContext := "FollowedTask.Start"
	fs := ft.Nomad.Client().AllocFS()
	ft.outState = NewStreamState(fs, ft.Quit, MULTILINE_BUF_CAP)
	ft.errState = NewStreamState(fs, ft.Quit, MULTILINE_BUF_CAP)
	if save != nil {
		ft.log.Debugf(logContext, "Restoring log offsets for %s", ft.Task.Name)
		ft.outState.SetOffsets(save.StdOutOffsets)
		ft.errState.SetOffsets(save.StdErrOffsets)
		ft.log.Debugf(logContext, "Set StdOut Offsets to %#v", ft.outState.FileOffsets)
		ft.log.Debugf(logContext, "Set StdErr Offsets to %#v", ft.errState.FileOffsets)
	}
	stdOutCh, stdOutErr := ft.outState.Start(ft.Alloc, ft.Task.Name, "stdout")
	stdErrCh, stdErrErr := ft.errState.Start(ft.Alloc, ft.Task.Name, "stderr")

	go func() {
		var messages []string
		for {
			// TODO need some max possible sleep duration
			// Keeps dead allocs from crash looping (until 403/404 error differentiated)
			if ft.outState.ConsecErrors > 0 || ft.errState.ConsecErrors > 0 {
				delay := BACKOFF_DELAY << (ft.outState.ConsecErrors + ft.errState.ConsecErrors)
				ft.log.Debugf(
					logContext,
					"Inserting delay of %ds for alloc: %s task: %s",
					delay,
					ft.Alloc.ID,
					ft.Task.Name,
				)
				time.Sleep(time.Duration(delay) * time.Second)
			}

			select {
			case _, ok := <-ft.Quit:
				// TODO handle quit case gracefully, flush buffer to out chan
				if !ok {
					return
				}
			case stdErrFrame, stdErrOk := <-stdErrCh:
				if stdErrOk {
					messages, ft.errState = ft.processFrame(stdErrFrame, ft.errState)
					for _, message := range messages {
						ft.OutputChan <- message
					}
					// TODO handle log file deletion + truncation events
					ft.log.Tracef(
						logContext,
						"StdErr Data size: %d Offsets Prior Total: %d Calculated: %d Reported: %d",
						len(stdErrFrame.Data),
						ft.errState.GetOffset(),
						CalculateOffset(ft.errState, stdErrFrame.File, int64(len(stdErrFrame.Data))),
						SpeculativeOffset(ft.errState, stdErrFrame.File, stdErrFrame.Offset),
					)
					// using reported offsets can lead to gaps when restoring a savepoint
					ft.errState.FileOffsets[stdErrFrame.File] += int64(len(stdErrFrame.Data))
					ft.errState.ConsecErrors = 0
					ft.outState.ConsecErrors = 0
				} else {
					stdErrCh, stdErrErr = ft.errState.Start(ft.Alloc, ft.Task.Name, "stderr")
					ft.errState.ConsecErrors += 1
				}

			case stdOutFrame, stdOutOk := <-stdOutCh:
				if stdOutOk {
					messages, ft.outState = ft.processFrame(stdOutFrame, ft.outState)
					for _, message := range messages {
						ft.OutputChan <- message
					}
					// TODO handle log file deletion + truncation events
					ft.log.Tracef(
						logContext,
						"StdOut Data size: %d Offsets Prior Total: %d Calculated: %d Reported: %d",
						len(stdOutFrame.Data),
						ft.outState.GetOffset(),
						CalculateOffset(ft.outState, stdOutFrame.File, int64(len(stdOutFrame.Data))),
						SpeculativeOffset(ft.outState, stdOutFrame.File, stdOutFrame.Offset),
					)
					// using reported offsets can lead to gaps when restoring a savepoint
					ft.outState.FileOffsets[stdOutFrame.File] += int64(len(stdOutFrame.Data))
					ft.outState.ConsecErrors = 0
					ft.errState.ConsecErrors = 0
				} else {
					stdOutCh, stdOutErr = ft.outState.Start(ft.Alloc, ft.Task.Name, "stdout")
					ft.outState.ConsecErrors += 1
				}


			case errErr := <-stdErrErr:
				ft.log.Debugf(
					logContext,
					"Error following stderr alloc: %s task: %s error: %s",
					ft.Alloc.ID,
					ft.Task.Name,
					errErr,
				)
				// TODO handle 403 case separately from 404 case
				// Handle task starting while client has invalid token
				ft.Nomad.RenewToken()
				stdErrCh, stdErrErr = ft.errState.Start(ft.Alloc, ft.Task.Name, "stderr")
				ft.errState.ConsecErrors += 1

			case outErr := <-stdOutErr:
				ft.log.Debugf(
					logContext,
					"Error following stdout alloc: %s task: %s error: %s",
					ft.Alloc.ID,
					ft.Task.Name,
					outErr,
				)
				// TODO handle 403 case separately from 404 case
				// Handle task starting while client has invalid token
				ft.Nomad.RenewToken()
				stdOutCh, stdOutErr = ft.outState.Start(ft.Alloc, ft.Task.Name, "stdout")
				ft.outState.ConsecErrors += 1
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

// processFrame takes a frame and determines if each line is JSON, or a single or multi-line log.
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
func (ft *FollowedTask) processFrame(frame *nomadApi.StreamFrame, streamState StreamState) ([]string, StreamState) {
	logContext := "FollowedTask.processFrame"
	messages := strings.Split(string(frame.Data[:]), "\n")
	jsons := make([]string, 0, INITIAL_OUTPUT_CAP)
	for _, message := range messages {
		if message == "" || message == "\n" {
			continue
		}
		if isJSON(message) {
			ft.log.Tracef(
				logContext,
				"Found single-line json log: %s",
				message,
			)
			// no multi-line buffering for valid single-line json
			l := wrapJsonLog(ft.logTemplate, message)
			s, err := l.ToJSON()
			if err != nil {
				ft.log.DeadLetterf(
					logContext,
					l,
					"Error building log message JSON Error: %v",
					err,
				)
			} else {
				jsons = append(jsons, s)
			}
		} else {
			timestamp := findTimestamp(message)
			if timestamp != "" {
				ft.log.Tracef(
					logContext,
					"Found message with timestamp: %s, msg: %s",
					timestamp,
					message,
				)
				if len(streamState.MultiLineBuf) == 0 {
					ft.log.Tracef(
						logContext,
						"Beginning multiline, msg: %s",
						message,
					)
					streamState.BufAdd(message)
				} else {
					// flush multiline buf + start over
					ft.log.Tracef(
						logContext,
						"Flushing multiline:\n%s\n, new multiline: %s\n",
						streamState.MultiLineBuf,
						message,
					)
					l := createJsonLog(ft.logTemplate, streamState.MultiLineBuf, streamState.LastTimestamp)
					s, err := l.ToJSON()
					if err != nil {
						ft.log.DeadLetterf(
							logContext,
							l,
							"Error building json log message: %v",
							err,
						)
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
					// log fragment -- bad parsing?
					ft.log.Tracef(
						logContext,
						"Log fragment case, msg: %s",
						message,
					)
					header := createFragmentHeader(streamState.LastTimestamp)
					streamState.BufAdd(header)
					streamState.BufAdd(message)
				} else {
					ft.log.Tracef(
						logContext,
						"Appending to existing multiline, msg: %s",
						message,
					)
					streamState.BufAdd(message)
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

func wrapJsonLog(logTmpl NomadLog, line string) NomadLog {
	data := getJSONMessage(line)
	timestamp := findJsonTimestamp(data)
	logTmpl.Data = data
	logTmpl.Timestamp = timestamp
	return logTmpl
}

func createJsonLog(logTmpl NomadLog, lines []string, timestamp string) NomadLog {
	logTmpl.Message = strings.Join(lines, "\n")
	logTmpl.Timestamp = timestamp
	return logTmpl
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
