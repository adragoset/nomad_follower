package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/mitchellh/mapstructure"
)

const RFC3339Milli = "2006-01-02T15:04:05.000Z07:00"

// LogLevel provides compariable levels and a string representation.
type LogLevel int
const (
	_ LogLevel = iota * 10
	TRACE
	DEBUG
	INFO
	ERROR
	DEADLETTER
)

// String formats LogLevels as a string for readability.
func (l LogLevel) String() string {
	var values = make(map[LogLevel]string)
	values[TRACE] = "trace"
	values[DEBUG] = "debug"
	values[INFO] = "info"
	values[ERROR] = "error"
	values[DEADLETTER] = "deadletter"

	s, ok := values[l]
	if !ok {
		return "unknown"
	}
	return s
}

func (l LogLevel) MarshalJSON() ([]byte, error) {
	return []byte(`"` + l.String() + `"`), nil
}

// FollowerLog structures log output from Nomad Follower and dead letters.
type FollowerLog struct {
	Name string `json:"name"`
	Message string `json:"message"`
	Datetime string `json:"datetime"`
	Level LogLevel `json:"log_level,string"`
	Data map[string]interface{} `json:"data"`
}

func (f FollowerLog) String() string {
	// function to handle retries
	marshal := func(l FollowerLog) ([]byte, error) {
		return json.Marshal(l)
	}
	s, err := marshal(f)
	if err != nil {
		// error likely with data map
		retryMsg := fmt.Sprintf(
			"Original Msg: '%s' Original Level: '%s' Original Data: '%#v'",
			f.Message,
			f.Level,
			f.Data,
		)
		retryLog := NewFollowerLog(
			fmt.Sprintf("DeadLetter.%s", f.Name),
			retryMsg,
			DEADLETTER,
			nil,
		)
		s, err = marshal(retryLog)
		if err != nil {
			// should really never get here
			panic("Unparsable dead letter")
		}
	}
	return string(s)
}

// NewFollowerLog creates a new FollowerLog and attempts to recover dead letters.
func NewFollowerLog(name, message string, level LogLevel, data map[string]interface{}) FollowerLog {
	now := time.Now()
	datetime := now.Format(RFC3339Milli)
	l := FollowerLog{}
	l.Name = name
	l.Message = message
	l.Level = level
	l.Data = data
	l.Datetime = datetime
	return l
}

func NewDeadLetter(name, message string, rawLog NomadLog) FollowerLog {
	data := make(map[string]interface{})
	// TODO determine what to do if err occurs here
	_ = mapstructure.Decode(rawLog, &data)
	return NewFollowerLog(name, message, DEADLETTER, data)
}

// Logger acts as a single config point for emitting FollowerLogs as JSON.
type Logger struct {
	verbosity LogLevel
}

func (l Logger) logAtLevel(name string, level LogLevel, message string) {
	n := NewFollowerLog(
		name,
		message,
		level,
		make(map[string]interface{}),
	)
	if level >= l.verbosity {
		fmt.Println(n)
	}
}

func (l Logger) logFormatAtLevel(name string, level LogLevel, message string, f ...interface{}) {
	msg := fmt.Sprintf(message, f...)
	l.logAtLevel(name, level, msg)
}

func (l Logger) Trace(name, message string) {
	l.logAtLevel(name, TRACE, message)
}

func (l Logger) Tracef(name, message string, f ...interface{}) {
	l.logFormatAtLevel(name, TRACE, message, f...)
}

func (l Logger) Debug(name, message string) {
	l.logAtLevel(name, DEBUG, message)
}

func (l Logger) Debugf(name, message string, f ...interface{}) {
	l.logFormatAtLevel(name, DEBUG, message, f...)
}

func (l Logger) Info(name, message string) {
	l.logAtLevel(name, INFO, message)
}

func (l Logger) Infof(name, message string, f ...interface{}) {
	l.logFormatAtLevel(name, INFO, message, f...)
}

func (l Logger) Error(name, message string) {
	l.logAtLevel(name, ERROR, message)
}

func (l Logger) Errorf(name, message string, f ...interface{}) {
	l.logFormatAtLevel(name, ERROR, message, f...)
}

func (l Logger) DeadLetter(name string, rawLog NomadLog, message string) {
	n := NewDeadLetter(
		name,
		message,
		rawLog,
	)
	fmt.Println(n)
}

func (l Logger) DeadLetterf(name string, rawLog NomadLog, message string, f ...interface{}) {
	msg := fmt.Sprintf(message, f...)
	n := NewDeadLetter(
		name,
		msg,
		rawLog,
	)
	fmt.Println(n)
}
