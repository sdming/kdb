package kdb

import (
	"fmt"
	"github.com/sdming/kdb/ansi"
)

const nilStr string = "<nil>"

// Parameter is parameter of sql statement or store procedure
type Parameter struct {
	// Name is parameter name, some driver don't support named parameter
	Name string

	// Value is value of this parameter
	Value interface{}

	// // DbType is data type
	// DbType ansi.DbType

	// Dir is direction, in,out, inout or return 
	Dir ansi.Dir
}

// String
func (p *Parameter) String() string {
	if p == nil {
		return nilStr
	}
	return fmt.Sprint(p.Name, " = ", p.Value)
}

// IsIn return true if parameter is input/inputoutput parameter
func (p *Parameter) IsIn() bool {
	return p.Dir == ansi.DirIn || p.Dir == ansi.DirInOut

}

// IsOut return true if parameter is output/inputoutput parameter
func (p *Parameter) IsOut() bool {
	return p.Dir == ansi.DirOut || p.Dir == ansi.DirInOut
}

// Node return NodeParameter
func (p *Parameter) Node() NodeType {
	return NodeParameter
}

// Text is sql statement
type Text struct {
	// Sql is raw sql statement
	Sql string

	// Parameters is parameters of Sql
	Parameters []*Parameter
}

// String
func (t *Text) String() string {
	if t == nil {
		return nilStr
	}
	return fmt.Sprint(t.Sql, t.Parameters)
}

// Node return NodeText
func (t *Text) Node() NodeType {
	return NodeText
}

// Set is shortcut of Parameter
func (t *Text) Set(name string, value interface{}) *Text {
	t.Parameter(&Parameter{Name: name, Value: value})
	return t
}

// Parameter append a paramter 
func (t *Text) Parameter(p *Parameter) {
	if p == nil {
		return
	}

	if t.Parameters == nil {
		t.Parameters = make([]*Parameter, 0, _defaultCapicity)
	}
	t.Parameters = append(t.Parameters, p)
}

// FindParameter return a paramter by name
func (t *Text) FindParameter(name string) (*Parameter, bool) {
	l := len(t.Parameters)

	for i := 0; i < l; i++ {
		p := t.Parameters[i]
		if p.Name == name {
			return p, true
		}
	}

	return nil, false
}

// NewText return a *Text with provided sql statement
func NewText(sql string) *Text {
	return &Text{Sql: sql}
}

// Procedure is sql store procedure
type Procedure struct {
	// Name is name of store procedure ot function
	Name string

	// Parameters is parameters of store procedure
	Parameters []*Parameter
}

// String
func (pc *Procedure) String() string {
	if pc == nil {
		return nilStr
	}
	return fmt.Sprint(pc.Name, pc.Parameters)
}

// Node return NodeProcedure
func (pc *Procedure) Node() NodeType {
	return NodeProcedure
}

// Set is shortcut of Parameter
func (pc *Procedure) Set(name string, value interface{}) *Procedure {
	pc.Parameter(&Parameter{Name: name, Value: value})
	return pc
}

// ReturnParameterName return parameter name if parameter is ansi.DirReturn
func (pc *Procedure) ReturnParameterName() string {
	l := len(pc.Parameters)
	for i := 0; i < l; i++ {
		p := pc.Parameters[i]
		if p.Dir == ansi.DirReturn {
			return p.Name
		}
	}
	return ""
}

// HasOutParameter return true if any parameter is output/inputoutput
func (t *Procedure) HasOutParameter() bool {
	for i := 0; i < len(t.Parameters); i++ {
		if t.Parameters[i].IsOut() {
			return true
		}
	}
	return false
}

// FindParameter return a parameter by name
func (pc *Procedure) FindParameter(name string) (*Parameter, bool) {
	l := len(pc.Parameters)

	for i := 0; i < l; i++ {
		p := pc.Parameters[i]
		if p.Name == name {
			return p, true
		}
	}

	return nil, false
}

// Parameter append a paramter
func (pc *Procedure) Parameter(p *Parameter) {
	if p == nil {
		return
	}

	if pc.Parameters == nil {
		pc.Parameters = make([]*Parameter, 0, _defaultCapicity)
	}
	pc.Parameters = append(pc.Parameters, p)
}

// NewProcedure return a *Procedure with provided name
func NewProcedure(name string) *Procedure {
	return &Procedure{Name: name}
}

// Insert is sql "insert into x values(...)" clause
type Insert struct {
	// Table is table to insert
	Table *Table

	// Sets is set[column=value]
	Sets []*Set
}

// String
func (ist *Insert) String() string {
	if ist == nil {
		return nilStr
	}

	return fmt.Sprint(ansi.Insert, " ", ist.Table, " ", ist.Sets)
}

// Node return NodeInsert
func (ist *Insert) Node() NodeType {
	return NodeInsert
}

// Set is shortcut of Append
func (ist *Insert) Set(column string, value interface{}) *Insert {
	ist.Append(newSet(column, asExpression(value)))
	return ist
}

// Append Append an *Set
func (ist *Insert) Append(a *Set) {
	if a == nil {
		return
	}
	if ist.Sets == nil {
		ist.Sets = make([]*Set, 0, _defaultCapicity)
	}
	ist.Sets = append(ist.Sets, a)
}

// NewInsert return *Insert with provided table
func NewInsert(table string) *Insert {
	return &Insert{Table: newTable(table, ""), Sets: make([]*Set, 0, _defaultCapicity)}
}

// Update is sql update clause
type Update struct {
	//T able is table to update
	Table *Table

	// Sets is set[column=value]
	Sets []*Set

	// Where is where clause
	Where *Where

	// OrderBy is order by clause
	OrderBy *OrderBy

	// Count is limit count
	Count int

	//Output      *Output
}

// String
func (u *Update) String() string {
	if u == nil {
		return nilStr
	}
	return fmt.Sprint(ansi.Update, " ", u.Table, " ", ansi.Set, " ", u.Sets, "\n", u.Where, "\n", u.OrderBy, "\n", ansi.Limit, u.Count)
}

// Node return NodeUpdate
func (u *Update) Node() NodeType {
	return NodeUpdate
}

// Set is shortcut of Append
func (u *Update) Set(column string, value interface{}) *Update {
	u.Append(newSet(column, asExpression(value)))
	return u
}

// Append Append an *Set
func (u *Update) Append(a *Set) {
	if u.Sets == nil {
		u.Sets = make([]*Set, 0, _defaultCapicity)
	}
	u.Sets = append(u.Sets, a)
}

// Limit set rows count to update
func (u *Update) Limit(count int) *Update {
	u.Count = count
	return u
}

// NotImplemented
// func (u *Update) Output(sql string) *Update {
// 	u.Output = newOutput(sql)
// 	return u
// }

func NewUpdate(table string) *Update {
	return &Update{
		Table:   newTable(table, ""),
		Sets:    make([]*Set, 0, _defaultCapicity),
		Where:   NewWhere(),
		OrderBy: &OrderBy{},
	}
}

// Delete is sql delete clause
type Delete struct {
	//Table is the table to delete
	Table *Table

	// From is from clause
	From *From

	// Where is where clause
	Where *Where

	// OrderBy is order by clause
	OrderBy *OrderBy

	// Count is limit count
	Count int

	//Output  *Output
}

// String
func (d *Delete) String() string {
	if d == nil {
		return nilStr
	}
	return fmt.Sprint(ansi.Delete, " ", d.Table, "\n", d.From, "\n", d.Where, "\n", d.OrderBy, "\n", ansi.Limit, d.Count)

}

// Node return NodeDelete
func (d *Delete) Node() NodeType {
	return NodeDelete
}

// Limit set rows count to delete
func (d *Delete) Limit(count int) *Delete {
	d.Count = count
	return d
}

// UseFrom new a *From and set to d.From
func (d *Delete) UseFrom(table, alias string) *From {
	d.From = NewFrom(table, alias)
	return d.From
}

// UseOrderBy new a *OrderBy and set to d.OrderBy
func (d *Delete) UseOrderBy() *OrderBy {
	d.OrderBy = NewOrderBy()
	return d.OrderBy
}

// NotImplemented
// func (d *Delete) Output(sql string) *Delete {
// 	d.Output = newOutput(sql)
// 	return d
// }

// NewDelete return a *Delete with provided table
func NewDelete(table string) *Delete {
	return &Delete{
		Table:   newTable(table, ""),
		Where:   NewWhere(),
		OrderBy: NewOrderBy(),
	}
}

// Query is sql query clause
type Query struct {
	Select     *Select
	From       *From
	Where      *Where
	GroupBy    *GroupBy
	Having     *Having
	OrderBy    *OrderBy
	IsDistinct bool
	Offset     int
	Count      int
}

// String
func (q *Query) String() string {
	if q == nil {
		return nilStr
	}
	distinct := ""
	if q.IsDistinct {
		distinct = ansi.Distinct
	}
	return fmt.Sprint(ansi.Select, " ", distinct, " ", q.Select, "\n", q.From, "\n", q.Where, q.GroupBy, "\n", q.Having, "\n", q.OrderBy, "\n", ansi.Limit, q.Offset, q.Count)
}

// Node return NodeQuery
func (q *Query) Node() NodeType {
	return NodeQuery
}

// Limit set offset and count
func (q *Query) Limit(offset, count int) *Query {
	q.Offset = offset
	q.Count = count
	return q
}

// Distinct set IsDistinct = true
func (q *Query) Distinct() *Query {
	q.IsDistinct = true
	return q
}

// UseGroupBy initialize q.GroupBy then return it
func (q *Query) UseGroupBy() *GroupBy {
	if q.GroupBy == nil {
		q.GroupBy = NewGroupBy()
	}
	return q.GroupBy
}

// UseHaving initialize q.Having then return it
func (q *Query) UseHaving() *Having {
	if q.Having == nil {
		q.Having = NewHaving()
	}
	return q.Having
}

// UseOrderBy initialize q.OrderBy then return it
func (q *Query) UseOrderBy() *OrderBy {
	if q.OrderBy == nil {
		q.OrderBy = NewOrderBy()
	}
	return q.OrderBy
}

// NewQuery return  *Query
func NewQuery(table, alias string) *Query {
	return &Query{
		From:   NewFrom(table, alias),
		Where:  NewWhere(),
		Select: NewSelect(),
	}
}
