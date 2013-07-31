package ansi

/*
sql key words

*/

const (
	Split          = "."
	StatementSplit = ";"
	WildcardAll    = "*"
	WildcardAny    = "%"
	WildcardOne    = "_"
	Blank          = " "
	Comma          = ","
	LineBreak      = "\n"

	Select     = "SELECT"
	Top        = "TOP"
	Distinct   = "DISTINCT"
	From       = "FROM"
	Where      = "WHERE"
	GroupBy    = "GROUP BY"
	Having     = "HAVING"
	OrderBy    = "ORDER BY"
	Asc        = "ASC"
	Desc       = "DESC"
	Limit      = "LIMIT"
	Insert     = "INSERT"
	InsertInto = "INSERT INTO"
	Values     = "VALUES"
	Update     = "UPDATE"
	Set        = "SET"
	Delete     = "DELETE"
	Output     = "OUTPUT"
	Using      = "USING"

	Join      = "JOIN"
	As        = "AS"
	On        = "ON"
	CrossJoin = "CROSS JOIN"
	FullJoin  = "FULL JOIN"
	InnerJoin = "INNER JOIN"
	OuterJoin = "OUTER JOIN"
	LeftJoin  = "LEFT JOIN"
	RightJoin = "RIGHT JOIN"

	And              = "AND"
	Or               = "OR"
	OpenParentheses  = "("
	CloseParentheses = ")"
	Null             = "NULL"
	IsNull           = "IS NULL"
	IsNotNull        = "IS NOT NULL"
	Is               = "IS"
	IsNot            = "IS NOT"
	LessThan         = "<"
	LessOrEquals     = "<="
	GreaterThan      = ">"
	GreaterOrEquals  = ">="
	Equals           = "="
	NotEquals        = "<>"
	Between          = "BETWEEN"
	Like             = "LIKE"
	NotLike          = "NOT LIKE"
	In               = "IN"
	NotIn            = "NOT IN"
	All              = "ALL"
	Some             = "SOME"
	Any              = "ANY"
	Exists           = "EXISTS"
	NotExists        = "NOT EXISTS"

	Count = "COUNT"
	Sum   = "SUM"
	Avg   = "AVG"
	Min   = "MIN"
	Max   = "MAX"

	BeginTran = "BEGIN TRAN"
	Commit    = "COMMIT"
	Rollback  = "ROLLBACK"
)

// Dir is direction of parameter
type Dir int

const (
	DirIn     Dir = 0
	DirOut    Dir = 1
	DirInOut  Dir = 2
	DirReturn Dir = 3
)

// String
func (d Dir) String() string {
	switch d {
	case DirIn:
		return "IN"
	case DirOut:
		return "OUT"
	case DirInOut:
		return "INOUT"
	case DirReturn:
		return "RETURN"
	}
	return "Unknow"
}
