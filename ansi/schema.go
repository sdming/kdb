package ansi

import (
	"fmt"
)

// DbTable is schema of table
type DbTable struct {
	// Name is table name
	Name string

	// Catalog is catalog name
	Catalog string

	// Schema is schema name
	Schema string

	// Type is table,view,...
	Type string

	// Columns is columns of this table
	Columns []DbColumn
}

func (t *DbTable) String() string {
	if t == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%#v", t)
}

func NewTable() *DbTable {
	return &DbTable{
		Columns: make([]DbColumn, 0, 11),
	}
}

// DbColumn is schema of column
type DbColumn struct {
	// Name is column name
	Name string

	// Position is position in table
	Position int

	// DbType is data type of this column
	DbType DbType

	// NativeType is native data type
	NativeType string

	// Precision
	Precision int

	// Scale	
	Scale int

	// Size
	Size int

	// IsNullable
	IsNullable bool

	// IsAutoIncrement
	IsAutoIncrement bool

	// IsReadOnly
	IsReadOnly bool

	// IsPrimaryKey
	IsPrimaryKey bool
}

// DbFunction is schema of procedure / function
type DbFunction struct {
	// Name is name of procedure
	Name string

	// Catalog 
	Catalog string

	// Schema
	Schema string

	// Parameters is parameters of this procedure
	Parameters []DbParameter
}

func (f *DbFunction) String() string {
	if f == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%#v", f)
}

func NewFunction() *DbFunction {
	return &DbFunction{
		Parameters: make([]DbParameter, 0, 11),
	}
}

// DbParameter is schema of procedure parameter
type DbParameter struct {
	// Name
	Name string

	// Position is position in procedure
	Position int

	// DbType is data type of parameter
	DbType DbType

	// NativeType is native data type 
	NativeType string

	// Dir is parameter direction
	Dir Dir

	// Precision
	Precision int

	// Scale
	Scale int

	// Size
	Size int
}

// Schemaer is interface of database schema provider
type Schemaer interface {
	// Table return schema of table
	Table(name string) DbTable

	// Procedure return schema of procedure
	Procedure(name string) DbTable
}
