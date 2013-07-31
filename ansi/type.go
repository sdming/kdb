package ansi

// DbType is data type of sql engine 
type DbType int

const (
	Zero     DbType = 0
	String   DbType = 1
	Boolean  DbType = 2
	Bytes    DbType = 3
	Date     DbType = 4
	DateTime DbType = 5
	Guid     DbType = 6

	Int     = 11
	Numeric = 12
	Float   = 13

	Var = 21

	//Int8            
	//Int16           
	//Int32           
	//Int64           
	//UInt8           
	//UInt16          
	//UInt32          
	//UInt64          
	//Float32
	//Float64 

)

// String
func (t DbType) String() string {
	switch t {
	case Zero:
		return "zero"
	case String:
		return "string"
	case Boolean:
		return "boolean"
	case Bytes:
		return "bytes"
	case Date:
		return "date"
	case DateTime:
		return "dateTime"
	case Guid:
		return "guid"

	case Int:
		return "int"
	case Numeric:
		return "numeric"
	case Float:
		return "float"
	case Var:
		return "var"
	}
	return "unknow"
}

// IsBoolean return true if t is Boolean 
func (t DbType) IsBoolean() bool {
	return t == Boolean
}

// IsInteger return true if t is Int 
func (t DbType) IsInteger() bool {
	return t == Int
}

// IsFloat return true if t is Float 
func (t DbType) IsFloat() bool {
	return t == Float
}

// IsNumeric return true if t is Float,Int,Numeric
func (t DbType) IsNumeric() bool {
	switch t {
	case Int, Numeric, Float:
		return true
	}
	return false
}

// IsDateTime return true if t is Date,DateTime
func (t DbType) IsDateTime() bool {
	return t == Date || t == DateTime
}

// IsString return true if t is String
func (t DbType) IsString() bool {
	return t == String
}

// HasPrecisionAndScale return true if t is Float,Numeric
func (t DbType) HasPrecisionAndScale() bool {
	return t == Float || t == Numeric
}
