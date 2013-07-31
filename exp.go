package kdb

import (
	"bytes"
	"fmt"
	"github.com/sdming/kdb/ansi"
	"strings"
)

const (
	_defaultCapicity int    = 7
	_nilStr          string = "<nil>"
)

// RawSqler is wrap of ToSql() string
type RawSqler interface {
	// ToSql return native sql
	ToSql() string
}

// Expression is interface of sql expression
type Expression interface {
	Node() NodeType
}

func asExpression(v interface{}) Expression {
	if v == nil {
		return DbNull
	}

	e, ok := v.(Expression)
	if ok {
		return e
	}

	return &Value{Value: v}
}

// SortDir is direction of orderby
type SortDir string

// String
func (sd SortDir) String() string {
	return string(sd)
}

// ToSql return native sql of SortDir(asc/desc)
func (sd SortDir) ToSql() string {
	return string(sd)
}

const (
	Asc  SortDir = ansi.Asc
	Desc SortDir = ansi.Desc
)

// JoinType is type of sql table join
type JoinType string

// String
func (j JoinType) String() string {
	return string(j)
}

// ToSql return vative sql of JoinType(left join, right join...)
func (j JoinType) ToSql() string {
	return string(j)
}

const (
	CrossJoin JoinType = ansi.CrossJoin
	InnerJoin JoinType = ansi.InnerJoin
	LeftJoin  JoinType = ansi.LeftJoin
	RightJoin JoinType = ansi.RightJoin
	//FullJoin  JoinType = ansi.FullJoin
)

// Func is sql function
type Func string

// String
func (f Func) String() string {
	return string(f)
}

// Node return NodeFunc
func (f Func) Node() NodeType {
	return NodeFunc
}

const (
	Count       Func = ansi.Count
	Sum         Func = ansi.Sum
	Avg         Func = ansi.Avg
	Min         Func = ansi.Min
	Max         Func = ansi.Max
	CurrentTime Func = "currenttime"
)

// Operator is operator in sql
type Operator string

// String
func (op Operator) String() string {
	return string(op)
}

// ToSql return native sql of operator(>,<,=,<>,...)
func (op Operator) ToSql() string {
	return string(op)
}

// Node return NodeOperator
func (op Operator) Node() NodeType {
	return NodeOperator
}

const (
	IsNull           Operator = ansi.IsNull
	IsNotNull        Operator = ansi.IsNotNull
	LessThan         Operator = ansi.LessThan
	LessOrEquals     Operator = ansi.LessOrEquals
	GreaterThan      Operator = ansi.GreaterThan
	GreaterOrEquals  Operator = ansi.GreaterOrEquals
	Equals           Operator = ansi.Equals
	NotEquals        Operator = ansi.NotEquals
	Like             Operator = ansi.Like
	NotLike          Operator = ansi.NotLike
	In               Operator = ansi.In
	NotIn            Operator = ansi.NotIn
	Exists           Operator = ansi.Exists
	NotExists        Operator = ansi.NotExists
	All              Operator = ansi.All
	Some             Operator = ansi.Some
	Any              Operator = ansi.Any
	And              Operator = ansi.And
	Or               Operator = ansi.Or
	OpenParentheses  Operator = ansi.OpenParentheses
	CloseParentheses Operator = ansi.CloseParentheses
)

// NodeType 
type NodeType int

const (
	NodeZero      NodeType = 0
	NodeText      NodeType = 1
	NodeProcedure NodeType = 2
	NodeInsert    NodeType = 3
	NodeQuery     NodeType = 4
	NodeUpdate    NodeType = 5
	NodeDelete    NodeType = 6

	NodeNull  NodeType = 11
	NodeValue NodeType = 12
	NodeSql   NodeType = 13

	NodeTable     NodeType = 31
	NodeColumn    NodeType = 32
	NodeAlias     NodeType = 33
	NodeCondition NodeType = 34
	NodeSet       NodeType = 35
	NodeAggregate NodeType = 36

	NodeSelect  NodeType = 41
	NodeFrom    NodeType = 42
	NodeJoin    NodeType = 43
	NodeWhere   NodeType = 44
	NodeGroupBy NodeType = 45
	NodeHaving  NodeType = 46
	NodeOrderBy NodeType = 47
	NodeOutput  NodeType = 48

	NodeOperator  = 61
	NodeFunc      = 62
	NodeParameter = 63
)

// String
func (n NodeType) String() string {
	switch n {
	case NodeZero:
		return "Zero"
	case NodeText:
		return "Text"
	case NodeProcedure:
		return "Procedure"
	case NodeInsert:
		return "Insert"
	case NodeQuery:
		return "Query"
	case NodeUpdate:
		return "Update"
	case NodeDelete:
		return "Delete"
	case NodeNull:
		return "Null"
	case NodeValue:
		return "Value"
	case NodeSql:
		return "Sql"
	case NodeTable:
		return "Table"
	case NodeColumn:
		return "Column"
	case NodeAlias:
		return "Alias"
	case NodeCondition:
		return "Condition"
	case NodeSet:
		return "Set"
	case NodeAggregate:
		return "Aggregate"
	case NodeSelect:
		return "Select"
	case NodeFrom:
		return "From"
	case NodeJoin:
		return "Join"
	case NodeWhere:
		return "Where"
	case NodeGroupBy:
		return "GroupBy"
	case NodeHaving:
		return "Having"
	case NodeOrderBy:
		return "OrderBy"
	case NodeOutput:
		return "Output "
	case NodeOperator:
		return "Operator"
	case NodeFunc:
		return "Func"
	}

	return "Unknow"
}

// Null is null in database
type Null string

// String
func (n Null) String() string {
	return string(n)
}

// ToSql return null
func (n Null) ToSql() string {
	return string(n)
}

// Node return NodeNull
func (n Null) Node() NodeType {
	return NodeNull
}

// DbNull mean null in database
const DbNull Null = ansi.Null

// Sql is sql statement
type Sql string

// String
func (s Sql) String() string {
	return string(s)
}

// ToSql return Sql
func (s Sql) ToSql() string {
	return string(s)
}

// Node return NodeSql
func (s Sql) Node() NodeType {
	return NodeSql
}

// Column is an column, like, table.coumn, column, table.*, *
type Column string

// String
func (c Column) String() string {
	return string(c)
}

// Split split column and return table and column
func (c Column) Split() (table string, column string) {
	s := string(c)
	if s == "" {
		return "", ""
	}
	i := strings.Index(s, ".")
	if i < 0 {
		return "", s
	}
	return s[0:i], s[i+1:]
}

// Node return NodeColumn
func (c Column) Node() NodeType {
	return NodeColumn
}

// Value is raw value
type Value struct {
	// Value is embed value
	Value interface{}
}

// String
func (v *Value) String() string {
	if v == nil {
		return _nilStr
	}
	return fmt.Sprint(v.Value)
}

// Node return NodeValue
func (v *Value) Node() NodeType {
	return NodeValue
}

// Set is set clause in update or insert
type Set struct {
	Column Column
	Value  Expression
}

// String
func (a *Set) String() string {
	if a == nil {
		return _nilStr
	}
	return fmt.Sprintf("%v = %v", a.Column, a.Value)
}

// Node return NodeSet
func (a *Set) Node() NodeType {
	return NodeSet
}

// newSet return  *Set
func newSet(column string, value Expression) *Set {
	return &Set{
		Column: Column(column),
		Value:  value,
	}
}

// Condition is condition in where or having
type Condition struct {
	Right Expression
	Left  Expression
	Op    Operator
}

// String
func (c *Condition) String() string {
	if c == nil {
		return _nilStr
	}
	if c.Right == nil && c.Left == nil {
		return fmt.Sprint(c.Op)
	} else if c.Left == nil {
		return fmt.Sprint(c.Op, "(", c.Right, ")")
	} else if c.Right == nil {
		return fmt.Sprint(c.Left, " ", c.Op)
	}
	return fmt.Sprintf("%v %v %v", c.Left, c.Op, c.Right)
}

// Node return NodeCondition
func (c *Condition) Node() NodeType {
	return NodeCondition
}

// Conditions is collection of condition
type Conditions struct {
	Conditions        []Expression
	needLogicOperator bool
}

// isEmpty
func (c *Conditions) isEmpty() bool {
	if c == nil || c.Conditions == nil || len(c.Conditions) == 0 {
		return true
	}
	return false
}

// String
func (c *Conditions) String() string {
	if c == nil {
		return _nilStr
	}

	buf := bytes.Buffer{}
	deep := 0

	for i := 0; i < len(c.Conditions); i++ {
		item := c.Conditions[i]
		if i > 0 {
			buf.WriteString("\n")
		}

		if item == CloseParentheses {
			deep--
		}

		if deep > 0 {
			buf.WriteString(strings.Repeat("\t", deep))
		}

		buf.WriteString(fmt.Sprint(item))
		if item == OpenParentheses {
			deep++
		}
	}
	return buf.String()
}

func (c *Conditions) checkLogicOperator() {
	if c.needLogicOperator {
		c.Conditions = append(c.Conditions, And)
	}
}

func (c *Conditions) set(exp Expression) {
	c.checkLogicOperator()
	if c.Conditions == nil {
		c.Conditions = make([]Expression, 0, _defaultCapicity)
	}
	c.Conditions = append(c.Conditions, exp)
	c.needLogicOperator = true
}

// Condition append a condition
func (c *Conditions) Condition(op Operator, left, right Expression) *Conditions {
	c.set(&Condition{
		Op:    op,
		Right: right,
		Left:  left,
	})
	return c
}

// And append logic operation And
func (c *Conditions) And() *Conditions {
	if c.needLogicOperator {
		c.Conditions = append(c.Conditions, And)
		c.needLogicOperator = false
	}
	return c
}

// Or append logic operation Or
func (c *Conditions) Or() *Conditions {
	if c.needLogicOperator {
		c.Conditions = append(c.Conditions, Or)
		c.needLogicOperator = false
	}
	return c
}

// OpenParentheses append a '('
func (c *Conditions) OpenParentheses() *Conditions {
	c.checkLogicOperator()
	c.Conditions = append(c.Conditions, OpenParentheses)
	c.needLogicOperator = false
	return c
}

// CloseParentheses append a ')'
func (c *Conditions) CloseParentheses() *Conditions {
	c.Conditions = append(c.Conditions, CloseParentheses)
	c.needLogicOperator = true
	return c
}

// Sql append raw sql
func (c *Conditions) Sql(sqlStr string) *Conditions {
	c.set(Sql(sqlStr))
	return c
}

// Exists append operation Exists
func (c *Conditions) Exists(exp Expression) *Conditions {
	return c.Condition(Exists, nil, exp)
}

// NotExists append operation NotExists
func (c *Conditions) NotExists(exp Expression) *Conditions {
	return c.Condition(NotExists, nil, exp)
}

// Compare append compare operation
func (c *Conditions) Compare(op Operator, column string, value interface{}) *Conditions {
	return c.Condition(op, Column(column), asExpression(value))
}

// Like append Like operation
func (c *Conditions) Like(column string, value string) *Conditions {
	//return c.Condition(Like, Column(column), Sql(value))
	return c.Condition(Like, Column(column), &Value{Value: value})
}

// NotLike append NotLike operation
func (c *Conditions) NotLike(column string, value string) *Conditions {
	//return c.Condition(NotLike, Column(column), Sql(value))
	return c.Condition(NotLike, Column(column), &Value{Value: value})
}

// LessOrEquals append <= operation
func (c *Conditions) LessOrEquals(column string, value interface{}) *Conditions {
	return c.Condition(LessOrEquals, Column(column), asExpression(value))
}

// LessThan append < operation
func (c *Conditions) LessThan(column string, value interface{}) *Conditions {
	return c.Condition(LessThan, Column(column), asExpression(value))
}

// GreaterOrEquals append >= operation
func (c *Conditions) GreaterOrEquals(column string, value interface{}) *Conditions {
	return c.Condition(GreaterOrEquals, Column(column), asExpression(value))
}

// GreaterThan append > operation
func (c *Conditions) GreaterThan(column string, value interface{}) *Conditions {
	return c.Condition(GreaterThan, Column(column), asExpression(value))
}

// GreaterThan append = operation
func (c *Conditions) Equals(column string, value interface{}) *Conditions {
	return c.Condition(Equals, Column(column), asExpression(value))
}

// NotEquals append <> operation
func (c *Conditions) NotEquals(column string, value interface{}) *Conditions {
	return c.Condition(NotEquals, Column(column), asExpression(value))
}

// IsNull append is null operation
func (c *Conditions) IsNull(column string) *Conditions {
	return c.Condition(IsNull, Column(column), nil)
}

// IsNotNull append is not null operation
func (c *Conditions) IsNotNull(column string) *Conditions {
	return c.Condition(IsNotNull, Column(column), nil)
}

// In append in(...) operation
func (c *Conditions) In(column string, value interface{}) *Conditions {
	return c.Condition(In, Column(column), asExpression(value))
}

// NotIn append not in(...) operation
func (c *Conditions) NotIn(column string, value interface{}) *Conditions {
	return c.Condition(NotIn, Column(column), asExpression(value))
}

func newConditions() *Conditions {
	return &Conditions{
		Conditions: make([]Expression, 0, _defaultCapicity),
	}
}

//Aggregate is sql aggregate Func
type Aggregate struct {
	Name Func
	Exp  Expression
}

// String
func (a *Aggregate) String() string {
	if a == nil {
		return _nilStr
	}
	return fmt.Sprintf("%v (%v)", a.Name, a.Exp)
}

// Node return Node
func (a *Aggregate) Node() NodeType {
	return NodeAggregate
}

// NewAggregate return *Aggregate
func NewAggregate(name Func, exp Expression) *Aggregate {
	return &Aggregate{
		Name: name,
		Exp:  exp,
	}
}

// Where is sql where clause
type Where struct {
	*Conditions
}

// String
func (w *Where) String() string {
	if w == nil {
		return _nilStr
	}

	return fmt.Sprint(ansi.Where, "\n", w.Conditions)
}

// Node return NodeWhere
func (w *Where) Node() NodeType {
	return NodeWhere
}

// NewWhere return *Where 
func NewWhere() *Where {
	return &Where{newConditions()}
}

// Having is sql having clause
type Having struct {
	*Conditions
}

// String
func (h *Having) String() string {
	if h == nil {
		return _nilStr
	}

	return fmt.Sprint(ansi.Having, "\n", h.Conditions)
}

// Node return NodeHaving
func (h *Having) Node() NodeType {
	return NodeHaving
}

func (h *Having) addAggregate(op Operator, name Func, column string, value Expression) {
	h.Condition(op, NewAggregate(name, Column(column)), value)
}

// Avg append avg(...)
func (h *Having) Avg(op Operator, column string, value interface{}) *Having {
	h.addAggregate(op, Avg, column, asExpression(value))
	return h
}

// Count append count(...)
func (h *Having) Count(op Operator, column string, value interface{}) *Having {
	h.addAggregate(op, Count, column, asExpression(value))
	return h
}

// Sum append sum(...)
func (h *Having) Sum(op Operator, column string, value interface{}) *Having {
	h.addAggregate(op, Sum, column, asExpression(value))
	return h
}

// Min append min(...)
func (h *Having) Min(op Operator, column string, value interface{}) *Having {
	h.addAggregate(op, Min, column, asExpression(value))
	return h
}

// Max append max(...)
func (h *Having) Max(op Operator, column string, value interface{}) *Having {
	h.addAggregate(op, Max, column, asExpression(value))
	return h
}

// NewHaving return *Having
func NewHaving() *Having {
	return &Having{newConditions()}
}

// GroupBy is sql group by clause
type GroupBy struct {
	Fields []Expression
}

// String
func (g *GroupBy) String() string {
	if g == nil {
		return _nilStr
	}

	return fmt.Sprint(ansi.GroupBy, " ", g.Fields)
}

// Node return Node()
func (g *GroupBy) Node() NodeType {
	return NodeGroupBy
}

func (g *GroupBy) add(exp Expression) {
	if g.Fields == nil {
		g.Fields = make([]Expression, 0, _defaultCapicity)
	}
	g.Fields = append(g.Fields, exp)
}

// By append a expression
func (g *GroupBy) By(exp Expression) *GroupBy {
	g.add(exp)
	return g
}

// Column append columns 
func (g *GroupBy) Column(columns ...string) *GroupBy {
	for i := 0; i < len(columns); i++ {
		g.add(Column(columns[i]))
	}
	return g
}

// NewGroupBy return  *GroupBy
func NewGroupBy() *GroupBy {
	return &GroupBy{Fields: make([]Expression, 0, _defaultCapicity)}
}

// Table is tables in sql from clasuse
type Table struct {
	Name  string
	Alias string
}

// String
func (t *Table) String() string {
	if t == nil {
		return _nilStr
	}

	if t.Alias == "" {
		return t.Name
	}
	return fmt.Sprint(t.Name, " AS ", t.Alias)
}

// Node return NodeTable
func (t *Table) Node() NodeType {
	return NodeTable
}

func newTable(name string, alias string) *Table {
	return &Table{
		Name:  name,
		Alias: alias,
	}
}

// Field is each field in sql select clause
type Field struct {
	Exp   Expression
	Alias string
}

// String
func (f *Field) String() string {
	if f == nil {
		return _nilStr
	}

	if f.Alias == "" {
		return fmt.Sprint(f.Exp)
	}
	return fmt.Sprint(f.Exp, " AS ", f.Alias)
}

// Select is sql select clause
type Select struct {
	Fields []*Field
}

// String
func (s *Select) String() string {
	if s == nil {
		return _nilStr
	}

	buf := bytes.Buffer{}
	for i := 0; i < len(s.Fields); i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprint(s.Fields[i]))
	}
	return buf.String()
}

// Node return NodeSelect
func (s *Select) Node() NodeType {
	return NodeSelect
}

// NewSelect return *Select 
func NewSelect() *Select {
	return &Select{Fields: make([]*Field, 0, _defaultCapicity)}
}

func (s *Select) addField(exp Expression, alias string) *Select {
	if s.Fields == nil {
		s.Fields = make([]*Field, 0, 5)
	}
	s.Fields = append(s.Fields, &Field{
		Exp:   exp,
		Alias: alias,
	})
	return s
}

// Column append columns to select list
func (s *Select) Column(columns ...string) *Select {
	if len(columns) == 0 {
		return s
	}
	for i := 0; i < len(columns); i++ {
		s.addField(Column(columns[i]), "")
	}
	return s
}

// ColumnAs append [column] as [alias] to select list
func (s *Select) ColumnAs(column, alias string) *Select {
	return s.addField(Column(column), alias)
}

// All append * 
func (s *Select) All() *Select {
	return s.addField(Sql(ansi.WildcardAll), "")
}

// Exp append a expression
func (s *Select) Exp(exp Expression, alias string) *Select {
	return s.addField(exp, alias)
}

// Aggregate append a aggregate function
func (s *Select) Aggregate(name Func, exp Expression, alias string) *Select {
	return s.addField(NewAggregate(name, exp), alias)
}

// Avg append avg(...) 
func (s *Select) Avg(column string, alias string) *Select {
	return s.Aggregate(Avg, Column(column), alias)
}

// Count append count(...) 
func (s *Select) Count(column string, alias string) *Select {
	return s.Aggregate(Count, Column(column), alias)
}

// Sum append sum(...) 
func (s *Select) Sum(column string, alias string) *Select {
	return s.Aggregate(Sum, Column(column), alias)
}

// Min append min(...) 
func (s *Select) Min(column string, alias string) *Select {
	return s.Aggregate(Min, Column(column), alias)
}

// Max append max(...) 
func (s *Select) Max(column string, alias string) *Select {
	return s.Aggregate(Max, Column(column), alias)
}

// OrderByField is each field in sql order by clause
type OrderByField struct {
	Exp       Expression
	Direction SortDir
}

// String
func (oi *OrderByField) String() string {
	if oi == nil {
		return _nilStr
	}

	return fmt.Sprint(oi.Exp, " ", oi.Direction)
}

// OrderBy is sql order by clause
type OrderBy struct {
	Fields []*OrderByField
}

// String
func (od *OrderBy) String() string {
	if od == nil {
		return _nilStr
	}
	if len(od.Fields) == 0 {
		return ""
	}

	buf := bytes.Buffer{}
	buf.WriteString(ansi.OrderBy)
	for i := 0; i < len(od.Fields); i++ {
		if i > 0 {
			buf.WriteString(", ")
		} else {
			buf.WriteString(" ")
		}
		buf.WriteString(fmt.Sprint(od.Fields[i]))
	}
	return buf.String()
}

// Node return NodeOrderBy
func (od *OrderBy) Node() NodeType {
	return NodeOrderBy
}

func (od *OrderBy) isEmpty() bool {
	return len(od.Fields) == 0
}

// By append a orderby field with direction
func (od *OrderBy) By(direction SortDir, exp Expression) *OrderBy {
	if od.Fields == nil {
		od.Fields = make([]*OrderByField, 0, _defaultCapicity)
	}
	od.Fields = append(od.Fields, &OrderByField{Exp: exp, Direction: direction})
	return od
}

// Asc append a column to order by as asc
func (od *OrderBy) Asc(columns ...string) *OrderBy {
	for i := 0; i < len(columns); i++ {
		od.By(Asc, Column(columns[i]))
	}
	return od
}

// Desc append a column to order by as desc
func (od *OrderBy) Desc(columns ...string) *OrderBy {
	for i := 0; i < len(columns); i++ {
		od.By(Desc, Column(columns[i]))
	}
	return od
}

// NewOrderBy return  *OrderBy
func NewOrderBy() *OrderBy {
	return &OrderBy{Fields: make([]*OrderByField, 0, _defaultCapicity)}
}

// From is sql from clause
type From struct {
	Table  *Table
	Tables []*Table
	Joins  []*Join
}

// String
func (f *From) String() string {
	if f == nil {
		return _nilStr
	}

	var buf bytes.Buffer
	buf.WriteString(ansi.From)
	buf.WriteString(" ")

	if f.Table != nil {
		buf.WriteString(fmt.Sprint(f.Table))
	}

	for i := 0; i < len(f.Tables); i++ {
		buf.WriteString(", ")
		buf.WriteString(fmt.Sprint(f.Tables[i]))
	}
	for i := 0; i < len(f.Joins); i++ {
		buf.WriteString("\n")
		buf.WriteString(fmt.Sprint(f.Joins[i]))
	}
	return buf.String()
}

// Node return NodeFrom
func (f *From) Node() NodeType {
	return NodeFrom
}

// NewFrom return *From
func NewFrom(table, alias string) *From {
	return &From{
		Table: newTable(table, alias),
	}
}

// ThenFrom append a table to from 
func (f *From) ThenFrom(table, alias string) *From {
	if f.Tables == nil {
		f.Tables = make([]*Table, 0, _defaultCapicity)
	}
	f.Tables = append(f.Tables, newTable(table, alias))
	return f
}

// FindTable return table from *From according name
func (f *From) FindTable(name string) *Table {
	if strings.EqualFold(f.Table.Alias, name) || strings.EqualFold(f.Table.Name, name) {
		return f.Table
	}

	var t *Table
	for i := 0; i < len(f.Tables); i++ {
		t = f.Tables[i]
		if strings.EqualFold(t.Alias, name) || strings.EqualFold(t.Name, name) {
			return t
		}
	}

	var j *Join
	for i := 0; i < len(f.Joins); i++ {
		j = f.Joins[i]

		if strings.EqualFold(j.Left.Alias, name) || strings.EqualFold(j.Left.Name, name) {
			return j.Left
		}
		if strings.EqualFold(j.Right.Alias, name) || strings.EqualFold(j.Right.Name, name) {
			return j.Right
		}
	}

	return nil
}

func (f *From) addJoin(joinType JoinType, toTable, toTableAlias string) *Join {
	j := NewJoinTable(joinType, f.Table, newTable(toTable, toTableAlias))
	f.Join(j)
	return j
}

// Join append *Join to *From
func (f *From) Join(join *Join) *From {
	if f.Joins == nil {
		f.Joins = make([]*Join, 0, _defaultCapicity)
	}
	f.Joins = append(f.Joins, join)
	return f
}

// Join append cross join to *From
func (f *From) CrossJoin(toTable, toTableAlias string) *Join {
	return f.addJoin(CrossJoin, toTable, toTableAlias)
}

// Join append inner join to *From
func (f *From) InnerJoin(toTable, toTableAlias string) *Join {
	return f.addJoin(InnerJoin, toTable, toTableAlias)
}

// Join append left join to *From
func (f *From) LeftJoin(toTable, toTableAlias string) *Join {
	return f.addJoin(LeftJoin, toTable, toTableAlias)
}

// Join append right join to *From
func (f *From) RightJoin(toTable, toTableAlias string) *Join {
	return f.addJoin(RightJoin, toTable, toTableAlias)
}

// func (f *From) FullJoin(toTable, toTableAlias string) *Join {
// 	return f.addJoin(FullJoin, toTable, toTableAlias)
// }

// Join is sql join clause
type Join struct {
	JoinType JoinType
	Left     *Table
	Right    *Table
	*Conditions
}

// String
func (j *Join) String() string {
	if j == nil {
		return _nilStr
	}

	buf := bytes.Buffer{}
	if j.Conditions != nil && j.Conditions.Conditions != nil {
		for i := 0; i < len(j.Conditions.Conditions); i++ {
			item := j.Conditions.Conditions[i]
			if i > 0 {
				buf.WriteString(" ")
			}
			buf.WriteString(fmt.Sprint(item))
		}
	}
	return fmt.Sprint(ansi.Join, " ", j.Left, " ", j.JoinType, " ", j.Right, " on (", buf.String(), ")")

}

// Node return NodeJoin
func (j *Join) Node() NodeType {
	return NodeJoin
}

// On means on leftColumn = rightColumn
func (j *Join) On(leftColumn, rightColumn string) {
	j.Condition(Equals, Column(leftColumn), Column(rightColumn))
}

// On means on leftColumn1 = rightColumn1 and leftColumn2 = rightColumn2
func (j *Join) On2(leftColumn1, rightColumn1, leftColumn2, rightColumn2 string) {
	j.Condition(Equals, Column(leftColumn1), Column(rightColumn1))
	j.Condition(Equals, Column(leftColumn2), Column(rightColumn2))
}

// NewJoin means [left] as [leftAlias] join [right] as [rightAlias]
func NewJoin(joinType JoinType, left, leftAlias, right, rightAlias string) *Join {
	return &Join{
		JoinType:   joinType,
		Left:       newTable(left, leftAlias),
		Right:      newTable(right, rightAlias),
		Conditions: newConditions(),
	}
}

// NewJoinTable means [left] join [right] 
func NewJoinTable(joinType JoinType, left, right *Table) *Join {
	return &Join{
		JoinType:   joinType,
		Left:       left,
		Right:      right,
		Conditions: newConditions(),
	}
}
