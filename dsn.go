package kdb

import ()

var _dsnData map[string]*DSN = make(map[string]*DSN)

// DSN is data souce config
type DSN struct {
	// Name is name of data source
	Name string

	// Driver is driver name
	Driver string

	// Source is driver-specific data source name
	Source string
}

// String
func (dsn *DSN) String() string {
	if dsn == nil {
		return "<nil>"
	}
	return dsn.Name
}

// RegisterDSN register a DSN
func RegisterDSN(name, driver, source string) {
	dsn := &DSN{
		Name:   name,
		Driver: driver,
		Source: source,
	}
	_dsnData[name] = dsn
}

func getDSN(name string) (*DSN, bool) {
	dsn, ok := _dsnData[name]
	return dsn, ok
}

func mustGetDSN(name string) *DSN {
	dsn, ok := _dsnData[name]
	if !ok {
		panic("DSN doesn't exists:" + name)
	}
	return dsn
}
