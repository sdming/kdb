package kdb

import (
	"errors"
	"log"
)

// Logger
var Logger *log.Logger

// LogLevel
var LogLevel int = 0

const (
	// LogNone means doesn't log
	LogNone int = 0

	// LogDebug means log err & debug information
	LogDebug int = 1

	// LogError means log err
	LogError int = 3
)

func logDebug(args ...interface{}) {
	if LogLevel >= LogDebug && Logger != nil {
		Logger.Println(args)
	}
}

func logError(args ...interface{}) {
	if LogLevel >= LogError && Logger != nil {
		Logger.Println(args)
	}
}

// Getter is wrap of Get(name string) (interface{}, bool)
type Getter interface {
	// Get return inner field value by name, return [nil, false] if name doesn't exist 
	Get(name string) (interface{}, bool)
}

// Iterater iterat fields 
type Iterater interface {
	// Fields return all field name
	Fields() []string
}

// Map is alias of map[string]interface{}
type Map map[string]interface{}

// Get return map element by name
func (m Map) Get(name string) (interface{}, bool) {
	v, ok := m[name]
	return v, ok
}

// Fields return all map keys
func (m Map) Fields() []string {
	if m == nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k, _ := range m {
		keys = append(keys, k)
	}
	return keys
}

// ErrNoResult means rows doesn't have result
var ErrNoResult = errors.New("rows no result")

// ExplictSchema is true mean must use schema when insert/update
var ExplictSchema = true
