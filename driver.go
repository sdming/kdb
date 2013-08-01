package kdb

import (
	"bytes"
	_ "database/sql"
	"errors"
	"fmt"
	"github.com/sdming/kdb/ansi"
	"reflect"
	"strconv"
	"strings"
)

// // Queryer is a interface that query expression
// type Queryer interface {
// 	Query(source string, exp Expression) (sql.Rows, error)
// }

// // Execer is a interface that execute expression
// type Execer interface {
// 	Exec(source string, exp Expression) (sql.Result, error)
// }

type Driver interface {
	Compiler
}

// Compiler is a interface that compile expression to native sql & args
type Compiler interface {
	Compile(source string, exp Expression) (query string, args []interface{}, err error)
}

var _compilers = make(map[string]Compiler)

// RegisterCompiler makes a compiler available by the provided driver name.
func RegisterCompiler(driver string, compiler Compiler) {
	if compiler == nil {
		panic("register compiler is nil")
	}
	_compilers[driver] = compiler
}

// GetCompiler return a a compiler by driver name
func GetCompiler(driver string) (Compiler, error) {
	c, ok := _compilers[driver]
	if !ok {
		return nil, errors.New(fmt.Sprint("can not get compiler:", driver))
	}
	return c, nil
}

// Schemaer is a interface that get schema of table,view,function
type Schemaer interface {
	// Table return schema of table,view
	Table(source string, name string) (*ansi.DbTable, error)

	// Function return schema of store procedure,function
	Function(source string, name string) (*ansi.DbFunction, error)
}

var _schemaers = make(map[string]Schemaer)

// RegisterSchemaer makes a schemaer available by the provided driver name.
func RegisterSchemaer(driver string, schemaer Schemaer) {
	if schemaer == nil {
		panic("register schemaer is nil")
	}
	_schemaers[driver] = schemaer
}

// GetSchemaer return a a schemaer by driver name
func GetSchemaer(driver string) (Schemaer, error) {
	schema, ok := _schemaers[driver]
	if !ok {
		return nil, errors.New(fmt.Sprint("can not get schemaer:", driver))
	}
	return schema, nil
}

// Dialecter is interface of sql dialect
type Dialecter interface {
	// Name return mysql,postgres,oracle,mssql,sqlite,...
	Name() string

	// SupportNamedParameter, like @para1
	SupportNamedParameter() bool

	// SupportIndexedParameter, like $1
	SupportIndexedParameter() bool

	// ParameterPlaceHolder, like ?, $, @
	ParameterPlaceHolder() string

	// QuoteString quote s as sql native string 
	QuoteString(s string) string

	// Quote quote object name, like 'table', [table]
	Quote(string) string

	// Table return sql to query table schema of name
	Table(name string) string

	// Columns return sql to query table columns schema
	Columns(name string) string

	// Function return sql to query function schema of name
	Function(name string) string

	// Parameters return sql to query procedure paramters schema
	Parameters(name string) string

	// DbType convert native data type to ansi.DbType
	DbType(nativeType string) ansi.DbType
}

var _dialecters = make(map[string]Dialecter)

// RegisterDialecter makes a dialecter available by the provided driver name.
func RegisterDialecter(driver string, dialecter Dialecter) {
	if dialecter == nil {
		panic("register dialecter is nil")
	}
	_dialecters[driver] = dialecter
}

// GetDialecter return a a dialecter by driver name
func GetDialecter(driver string) (Dialecter, error) {
	d, ok := _dialecters[driver]
	if !ok {
		return nil, errors.New(fmt.Sprint("can not get dialecter:", driver))
	}
	return d, nil
}

// DefaultDialecter return AnsiDialecter
func DefaultDialecter() Dialecter {
	return AnsiDialecter{}
}

// AnsiDialecter is ansi sql dialect
type AnsiDialecter struct {
}

// Name return "ansi"
func (ad AnsiDialecter) Name() string {
	return "ansi"
}

// SupportNamedParameter return false
func (ad AnsiDialecter) SupportNamedParameter() bool {
	return false
}

// SupportIndexedParameter return false
func (ad AnsiDialecter) SupportIndexedParameter() bool {
	return false
}

// ParameterPlaceHolder return ?
func (ad AnsiDialecter) ParameterPlaceHolder() string {
	return " ? "
}

// QuoteString quote s as sql native string 
func (ad AnsiDialecter) QuoteString(s string) string {
	return "'" + s + "'"
}

// Quote quote s as "s"
func (ad AnsiDialecter) Quote(s string) string {
	return "\"" + s + "\""
}

// Table return ""
func (ansi AnsiDialecter) Table(name string) string {
	return ""
}

// Columns return sql to query table columns schema
func (ansi AnsiDialecter) Columns(name string) string {
	return ""
}

// Function return ""
func (ad AnsiDialecter) Function(s string) string {
	return ""
}

// Parameters return sql to query procedure paramters schema
func (ad AnsiDialecter) Parameters(name string) string {
	return ""
}

func (ad AnsiDialecter) DbType(nativeType string) ansi.DbType {
	switch strings.ToLower(nativeType) {
	case "xml", "tinytext", "mediumtext", "longtext", "ntext", "text", "sysname", "sql_variant", "note", "memo", "clob":
		return ansi.String
	case "char", "character", "nchar", "varchar", "nvarchar", "string", "longvarchar", "longchar", "varyingcharacter":
		return ansi.String
	case "nativecharacter", "nativevaryingcharacter", "character varying":
		return ansi.String
	case "bit", "bool", "boolean", "yesno", "logical":
		return ansi.Boolean
	case "tinyint unsigned", "uint16", "smallint unsigned", "uint32", "integer unsigned", "uint64", "bigint unsigned":
		return ansi.Int
	case "tinyint", "smallint", "int", "mediumint", "bigint", "int16", "int32", "int64", "integer", "long":
		return ansi.Int
	case "bigserial", "serial", "smallserial":
		return ansi.Int
	case "identity", "counter", "autoincrement":
		return ansi.Int
	case "decimal", "newdecimal", "numeric":
		return ansi.Numeric
	case "currency", "money", "smallmoney":
		return ansi.Numeric
	case "float", "real", "double", "double precision":
		return ansi.Float
	case "date", "smalldate":
		return ansi.Date
	case "time", "datetime", "datetime2", "smalldatetime", "timestamp", "timestamp without time zone", "timestamp with time zone":
		return ansi.DateTime
	case "year":
		return ansi.Int
	case "image", "varbinary", "binary", "blob", "tinyblob", "mediumblob", "longblob", "oleobject", "general", "bit varying", "bytea":
		return ansi.Bytes
	case "uniqueidentifier", "guid", "uuid":
		return ansi.Guid
	default:
		return ansi.Var
	}
}

// MssqlDialecter is ms sql server dialect
type MssqlDialecter struct {
	AnsiDialecter
}

// Name return "mssql"
func (mssql MssqlDialecter) Name() string {
	return "mssql"
}

// Quote quote s as [s]
func (mssql MssqlDialecter) Quote(s string) string {
	return "[" + s + "]"
}

// Table return sql to query table schema
func (mssql MssqlDialecter) Table(name string) string {
	return fmt.Sprintf("SELECT TABLE_CATALOG AS [catalog], TABLE_SCHEMA AS [schema], TABLE_NAME AS [name], TABLE_TYPE AS [type] FROM information_schema.[TABLES] WHERE TABLE_NAME = '%s' ", name)
}

// Columns return sql to query table columns schema
func (mssql MssqlDialecter) Columns(name string) string {
	return fmt.Sprintf(`
select c.[name], c.column_id as [position], c.is_nullable as [nullable], 
t.name as [datatype],
c.max_length as [length],
c.[precision],
c.[scale],
c.is_identity as [autoincrement],
case when (c.is_identity = 1 or c.is_computed = 1) then 1 else 0 end as [readonly],
isnull(ict.primarykey,0) AS [primarykey]
from 
	sys.columns c
	inner join sys.types t on c.user_type_id = t.user_type_id
	left join 
	(
		select ic.column_id, 1 primarykey
		from sys.indexes i
		inner join  sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id 
		where i.object_id = object_id('%s') and i.is_primary_key = 1
	)  as ict on c.column_id = ict.column_id
where
	c.object_id = object_id('%s')
order by 
	c.column_id
	;
`, name, name)
}

// Function return sql to query procedure schema
func (mssql MssqlDialecter) Function(name string) string {
	return fmt.Sprintf("SELECT ROUTINE_CATALOG AS [catalog], ROUTINE_SCHEMA AS [schema], ROUTINE_NAME as [name] FROM information_schema.ROUTINES WHERE ROUTINE_NAME = '%s' ;", name)
}

// Parameters return sql to query procedure paramters schema
func (mssql MssqlDialecter) Parameters(name string) string {
	return fmt.Sprintf("SELECT Substring(PARAMETER_NAME,2,len(PARAMETER_NAME)-1) as [name], ORDINAL_POSITION as [position], PARAMETER_MODE as [dirmode], DATA_TYPE as [datatype],ISNULL(CHARACTER_MAXIMUM_LENGTH,0) as [length], ISNULL(NUMERIC_PRECISION,0) as [precision], ISNULL(NUMERIC_SCALE,0) as [scale] FROM information_schema.PARAMETERS WHERE SPECIFIC_NAME = '%s' ORDER BY ORDINAL_POSITION", name)
}

// MysqlDialecter is Mysql dialect
type MysqlDialecter struct {
	AnsiDialecter
}

// Name return "mysql"
func (mysql MysqlDialecter) Name() string {
	return "mysql"
}

// QuoteString quote s as sql native string 
func (mysql MysqlDialecter) QuoteString(s string) string {
	return "\"" + s + "\""
}

// Quote quote s as 's'
func (mysql MysqlDialecter) Quote(s string) string {
	return "'" + s + "'"
}

// Table return sql to query table schema
func (mysql MysqlDialecter) Table(name string) string {
	// http://dev.mysql.com/doc/refman/5.1/en/tables-table.html
	return fmt.Sprintf("SELECT TABLE_CATALOG AS `catalog`, TABLE_SCHEMA AS `schema`, TABLE_NAME AS `name`, TABLE_TYPE AS `type` FROM information_schema.`TABLES` WHERE TABLE_NAME = '%s' AND TABLE_SCHEMA= DATABASE() ", name)
}

// Columns return sql to query table columns schema
func (mysql MysqlDialecter) Columns(name string) string {
	// http://dev.mysql.com/doc/refman/5.0/en/show-columns.html
	// show columns from ttable 
	return fmt.Sprintf("SELECT COLUMN_NAME as `name`, ORDINAL_POSITION as `position`, CASE IS_NULLABLE WHEN 'YES' THEN TRUE ELSE FALSE END as `nullable`, DATA_TYPE as `datatype`, IFNULL(CHARACTER_MAXIMUM_LENGTH,0) as `length`, IFNULL(NUMERIC_PRECISION,0) as `precision`, IFNULL(NUMERIC_SCALE,0) as `scale`, CASE WHEN EXTRA LIKE '%%auto_increment%%' THEN TRUE ELSE FALSE END AS `autoincrement`, CASE WHEN EXTRA LIKE '%%auto_increment%%' THEN TRUE ELSE FALSE END AS `readonly`, CASE WHEN COLUMN_KEY = 'PRI' THEN TRUE ELSE FALSE END AS `primarykey` FROM information_schema.COLUMNS WHERE TABLE_NAME = '%s' and TABLE_SCHEMA= DATABASE() ORDER BY ORDINAL_POSITION ;", name)
}

// Function return sql to query procedure schema
func (mysql MysqlDialecter) Function(name string) string {
	//http://dev.mysql.com/doc/refman/5.1/en/routines-table.html
	return fmt.Sprintf("SELECT  ROUTINE_CATALOG AS `catalog`, ROUTINE_SCHEMA AS `schema`, ROUTINE_NAME as `name` FROM information_schema.ROUTINES WHERE ROUTINE_NAME = '%s' AND ROUTINE_SCHEMA = DATABASE(); ", name)
}

// Parameters return sql to query procedure paramters schema
func (mysql MysqlDialecter) Parameters(name string) string {
	return fmt.Sprintf("SELECT PARAMETER_NAME as `name`, ORDINAL_POSITION as `position`, PARAMETER_MODE as `dirmode`, DATA_TYPE as `datatype`, IFNULL(CHARACTER_MAXIMUM_LENGTH,0) as `length`, IFNULL(NUMERIC_PRECISION,0) as `precision`, IFNULL(NUMERIC_SCALE,0) as `scale` FROM information_schema.PARAMETERS WHERE SPECIFIC_NAME = '%s' and SPECIFIC_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION", name)
}

// PostgreSQLDialecter is PostgreSQL dialect
type PostgreSQLDialecter struct {
	AnsiDialecter
}

// Name return "postgres"
func (pgsql PostgreSQLDialecter) Name() string {
	return "postgres"
}

// SupportIndexedParameter regturn true
func (pgsql PostgreSQLDialecter) SupportIndexedParameter() bool {
	return true
}

// ParameterPlaceHolder return $
func (pgsql PostgreSQLDialecter) ParameterPlaceHolder() string {
	return "$"
}

// QuoteString quote s as sql native string 
func (pgsql PostgreSQLDialecter) QuoteString(s string) string {
	return "'" + s + "'"
}

// Quote quote s as 's'
func (pgsql PostgreSQLDialecter) Quote(s string) string {
	return "\"" + s + "\""
}

// Table return sql to query table schema
func (pgsql PostgreSQLDialecter) Table(name string) string {
	// http://www.postgresql.org/docs/9.2/static/infoschema-tables.html
	return fmt.Sprintf(`
select 
	table_catalog as "catalog", 
	table_schema as "schema", 
	table_name as "name", 
	table_type as "type" 
from 
	information_schema.tables 
where 
	table_name = '%s' 
	and table_schema = current_schema() 
	and table_schema not in ('pg_catalog', 'information_schema'); `, name)
}

// Columns return sql to query table columns schema
func (pgsql PostgreSQLDialecter) Columns(name string) string {
	// http://www.postgresql.org/docs/9.2/static/infoschema-columns.html
	return fmt.Sprintf(`
select 
	column_name as "name", 
	ordinal_position as "position", 
	case is_nullable when 'YES' then true else false end as "nullable", 
	data_type as "datatype",
	COALESCE(character_maximum_length,0) as "length", 
	COALESCE(numeric_precision,0) as "precision", 
	COALESCE(numeric_scale,0) as "scale",
	case when pg_get_serial_sequence(table_name, column_name) is null then false else true end as "autoincrement",
	case is_updatable when 'YES' then false else true end as "readonly",
	case when exists (
		select 
			kc.column_name 
		from  
			information_schema.table_constraints tc,  
			information_schema.key_column_usage kc  
		where 
			kc.table_name = c.table_name and kc.table_schema =c.table_schema and kc.column_name = c.column_name
			and tc.constraint_type = 'PRIMARY KEY' 
			and kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name
			
	) then true else false end as "primarykey"
 from 
	information_schema.columns  c
 where 
	table_name = '%s' 
	and table_schema = current_schema()
 order by 
	ordinal_position ;
`, name)
}

// Function return sql to query procedure schema
func (pgsql PostgreSQLDialecter) Function(name string) string {
	//http://www.postgresql.org/docs/9.2/static/infoschema-routines.html
	return fmt.Sprintf(`select routine_catalog as "catalog", routine_schema as "schema", routine_name as "name" from information_schema.routines where routine_name = '%s' and routine_schema = current_schema() ;`, name)
}

// Parameters return sql to query procedure paramters schema
func (pgsql PostgreSQLDialecter) Parameters(name string) string {
	return fmt.Sprintf(`
select 
	p.parameter_name as "name", p.ordinal_position as "position", p.parameter_mode as "dirmode", p.data_type as "datatype", COALESCE(p.character_maximum_length,0) as "length", COALESCE(p.numeric_precision,0) as "precision", COALESCE(p.numeric_scale,0) as "scale" 
from 
	information_schema.parameters p,
	information_schema.routines r
where
	p.specific_catalog = r.specific_catalog and p.specific_schema = r. specific_schema and p.specific_name = r.specific_name
	and r.routine_name = '%s' and r.routine_schema = current_schema()
order by 
	ordinal_position ;
`, name)
}

// SqlDriver is ansi sql compiler
type SqlDriver struct {
	Dialecter Dialecter
}

// NewSqlDriver return a SqlDriver
func NewSqlDriver(dialecter Dialecter) Compiler {
	return &SqlDriver{Dialecter: dialecter}
}

// Compile compile expression to ansi sql
func (c *SqlDriver) Compile(source string, exp Expression) (query string, args []interface{}, err error) {
	if exp == nil {
		err = errors.New("compile expression is nil")
		return
	}

	switch exp.Node() {
	case NodeText:
		t, _ := exp.(*Text)
		return c.compileText(t, source)
	case NodeProcedure:
		p, _ := exp.(*Procedure)
		return c.compileProcedure(p, source)
	case NodeQuery, NodeUpdate, NodeInsert, NodeDelete:
		return NewStmtCompiler(c.Dialecter).Compile(exp, source)
	}

	err = errors.New(fmt.Sprint("compile expression does support type:", exp.Node()))
	return
}

func (c *SqlDriver) compileText(text *Text, source string) (query string, args []interface{}, err error) {
	if text == nil || text.Sql == "" {
		err = errors.New("text is nil or sql of text is empty")
		return
	}

	if len(text.Parameters) == 0 {
		query = text.Sql
		return
	}

	placeHolder := c.Dialecter.ParameterPlaceHolder()
	paramters := make([]interface{}, 0, len(text.Parameters))
	mode := 0
	paraIndex := 1

	switch {
	case c.Dialecter.SupportNamedParameter():
		mode = 1
	case c.Dialecter.SupportIndexedParameter():
		mode = 2
	}

	b := []byte(text.Sql)
	buffer := &bytes.Buffer{}
	state := 0

	for {
		if state == 0 {
			index := bytes.IndexByte(b, '{')
			if index >= 0 {
				buffer.Write(b[:index])
				b = b[index+1:]
				state = 1
			} else {
				break
			}
		} else {
			index := bytes.IndexByte(b, '}')
			if index > 0 {
				name := string(bytes.TrimSpace((b[:index])))
				p, ok := text.FindParameter(name)
				if !ok {
					err = errors.New("text can not find parameter:" + name)
					return
				}
				buffer.WriteString(placeHolder)

				switch mode {
				case 0:
					paramters = append(paramters, p.Value)
				case 1:
					buffer.WriteString(name)
					paramters = append(paramters, p.Value)
				case 2:
					buffer.WriteString(strconv.Itoa(paraIndex))
					paraIndex++
					paramters = append(paramters, p.Value)
				}
				b = b[index+1:]
				state = 0
			} else {
				err = errors.New("text sql format is invalid")
				return
			}
		}
	}

	buffer.Write(b)
	query = buffer.String()
	args = paramters

	return
}

func (c *SqlDriver) compileMysqlProcedure(sp *Procedure, source string) (query string, args []interface{}, err error) {
	l := len(sp.Parameters)
	paramters := make([]interface{}, 0, l)
	buffer := &sqlWriter{}
	returnName := sp.ReturnParameterName()
	hasOut := false

	for i := 0; i < l; i++ {
		p := sp.Parameters[i]
		if p.Dir == ansi.DirInOut {
			buffer.Print("SET @", p.Name, " = ?; \n")
			paramters = append(paramters, p.Value)
		}
	}

	if returnName == "" {
		buffer.WriteString("CALL ")
	} else {
		buffer.WriteString("SET @" + returnName)
	}
	buffer.WriteString(sp.Name)
	buffer.WriteString(" ( ")
	for i := 0; i < l; i++ {
		if i > 0 {
			buffer.WriteString(", ")
		}
		p := sp.Parameters[i]
		if p.Dir == ansi.DirIn {
			buffer.WriteString("?")
			paramters = append(paramters, p.Value)
		} else if p.Dir == ansi.DirInOut {
			buffer.Print("@", p.Name)
			hasOut = true
		} else if p.Dir == ansi.DirOut {
			buffer.Print("@", p.Name)
			hasOut = true
		}
	}
	buffer.WriteString(" );")

	if hasOut || returnName != "" {
		buffer.WriteString("\nSELECT ")
		delimit := false
		for i := 0; i < l; i++ {
			p := sp.Parameters[i]
			if p.IsOut() || p.Dir == ansi.DirReturn {
				if delimit {
					buffer.WriteString(", ")
				}
				buffer.Print("@", p.Name)
				delimit = true
			}
		}
		buffer.WriteString("; ")
	}

	query = buffer.String()
	args = paramters
	return
}

//exec sp_executesql N'update ttable set cdatetime=getdate() where cint >  @P1 ',N'@P1 bigint',42

func (c *SqlDriver) compileMssqlProcedure(sp *Procedure, source string) (query string, args []interface{}, err error) {
	l := len(sp.Parameters)
	paramters := make([]interface{}, 0, l)
	split := false
	w := &sqlWriter{}

	if !sp.HasOutParameter() {
		w.Print("exec ", sp.Name, " ")

		for i := 0; i < l; i++ {
			p := sp.Parameters[i]
			if p.Dir == ansi.DirReturn {
				continue
			}
			if split {
				w.Comma()
			}
			split = true
			w.WriteString("?")
			paramters = append(paramters, p.Value)
		}

		query = w.String()
		args = paramters
		return
	}

	for i := 0; i < l; i++ {
		p := sp.Parameters[i]
		if p.IsOut() {
			w.Print("declare @kdbp", strconv.Itoa(i), " int\n")
			w.Print("set @kdbp", strconv.Itoa(i), "= ?\n")
			paramters = append(paramters, p.Value)
		}
	}

	split = false
	w.Print("exec ", sp.Name, " ")
	for i := 0; i < l; i++ {
		p := sp.Parameters[i]
		if p.Dir == ansi.DirReturn {
			continue
		}
		if split {
			w.Comma()
		}
		split = true
		if p.Dir == ansi.DirIn {
			w.Print("@", p.Name, "=? ")
			paramters = append(paramters, p.Value)
		} else {
			w.Print("@", p.Name, "=@kdbp", strconv.Itoa(i), " output")
		}
	}

	split = false
	w.LineBreak()
	w.WriteString("select ")
	for i := 0; i < l; i++ {
		p := sp.Parameters[i]

		if p.IsOut() {
			if split {
				w.Comma()
			}
			split = true
			w.Print("@kdbp", strconv.Itoa(i))
		}
	}

	// declare @p2 int
	// set @p2=4
	// declare @p3 int
	// set @p3=3
	// exec usp_inout @x=1,@y=@p2 output,@sum=@p3 output
	// select @p2, @p3
	query = w.String()
	args = paramters
	fmt.Print(query)
	return
}

func (c *SqlDriver) compilePostgresProcedure(sp *Procedure, source string) (query string, args []interface{}, err error) {
	l := len(sp.Parameters)
	paramters := make([]interface{}, 0, l)
	w := &sqlWriter{}
	index := 1

	w.WriteString("SELECT * FROM ")
	w.WriteString(sp.Name)
	w.OpenParentheses()

	for i := 0; i < l; i++ {
		p := sp.Parameters[i]
		if p.IsIn() {
			if index > 1 {
				w.Comma()
			}

			w.WriteString(c.Dialecter.ParameterPlaceHolder())
			w.WriteString(strconv.Itoa(index))
			paramters = append(paramters, p.Value)
			index++
		}
	}

	w.CloseParentheses()
	w.WriteString(ansi.StatementSplit)

	query = w.String()
	args = paramters
	return
}

func (c *SqlDriver) compileProcedure(sp *Procedure, source string) (query string, args []interface{}, err error) {
	if sp == nil || sp.Name == "" {
		err = errors.New("procedure is nil or name of procedure is empty")
		return
	}

	switch c.Dialecter.Name() {
	case "mysql":
		return c.compileMysqlProcedure(sp, source)
	case "postgres":
		return c.compilePostgresProcedure(sp, source)
	case "mssql":
		return c.compileMssqlProcedure(sp, source)
	}
	err = errors.New("driver dones't support procedure:" + c.Dialecter.Name())
	return
}

// StmtCompiler can compile Update, Insert, Delete, Query
type StmtCompiler struct {
	// Dialecter is a provided Dialecter
	Dialecter   Dialecter
	exp         Expression
	source      string
	w           *sqlWriter
	args        []interface{}
	paraIndex   int
	placeHolder string
}

// NewStmtCompiler return  *StmtCompiler with provided Dialecter
func NewStmtCompiler(dialecter Dialecter) *StmtCompiler {
	return &StmtCompiler{
		Dialecter: dialecter,
		args:      make([]interface{}, 0, _defaultCapicity),
	}
}

// Compile compile expression to ansi sql
func (sc *StmtCompiler) Compile(exp Expression, source string) (query string, args []interface{}, err error) {
	if exp == nil {
		err = errors.New("compile expression is nil")
	}

	sc.w = &sqlWriter{}
	sc.source = source
	sc.placeHolder = sc.Dialecter.ParameterPlaceHolder()

	switch exp.Node() {
	case NodeQuery:
		sc.visitQuery(exp)
	case NodeUpdate:
		sc.visitUpdate(exp)
	case NodeInsert:
		sc.visitInsert(exp)
	case NodeDelete:
		sc.visitDelete(exp)
	default:
		err = errors.New("doesn't support expression type:" + exp.Node().String())
	}

	if err != nil {
		return
	}

	query = sc.w.String()
	args = sc.args

	return
}

func (sc *StmtCompiler) writeQuote(s string) {
	sc.w.WriteString(sc.Dialecter.Quote(s))
}

func (sc *StmtCompiler) visitExp(exp Expression) {
	if exp == nil {
		return
	}

	switch exp.Node() {
	case NodeZero:
		return
	case NodeText, NodeProcedure, NodeParameter, NodeOutput:
		panic("doesn't support this expression type:" + exp.Node().String())
	case NodeNull, NodeSql, NodeOperator:
		sql, ok := exp.(RawSqler)
		if !ok {
			panic("should be a RawSqler:" + exp.Node().String())
		}
		sc.w.WriteString(sql.ToSql())
		return
	}

	switch exp := exp.(type) {
	case *Insert:
		sc.visitInsert(exp)
	case *Query:
		sc.visitQuery(exp)
	case *Update:
		sc.visitUpdate(exp)
	case *Delete:
		sc.visitDelete(exp)
	case *Value:
		sc.visitValue(exp)
	case *Table:
		sc.visitTable(exp)
	case *Column:
		sc.visitColumn(*exp)
	case Column:
		sc.visitColumn(exp)
	// case *Alias:
	// 	sc.visitAlias(exp)
	case *Condition:
		sc.visitCondition(exp)
	// case *Set:
	// 	sc.visitSet(exp)
	case *Aggregate:
		sc.visitAggregate(exp)
	case *Select:
		sc.visitSelect(exp)
	case *From:
		sc.visitFrom(exp)
	case *Join:
		sc.visitJoin(exp)
	case *Where:
		sc.visitWhere(exp)
	case *GroupBy:
		sc.visitGroupBy(exp)
	case *Having:
		sc.visitHaving(exp)
	case *OrderBy:
		sc.visitOrderBy(exp)
		// case *Func:
		// 	sc.visitFunc(exp)
	}
}

func (sc *StmtCompiler) visitAggregate(a *Aggregate) {
	if a == nil || a.Exp == nil || a.Name == "" {
		return
	}

	sc.w.WriteString(a.Name.String())
	sc.w.OpenParentheses()
	sc.visitExp(a.Exp)
	sc.w.CloseParentheses()
}

func (sc *StmtCompiler) writeValue(v interface{}) {
	if v == nil {
		sc.w.WriteString(ansi.Null)
		return
	}

	if sc.args == nil {
		sc.args = make([]interface{}, 0, _defaultCapicity)
	}

	mode := 0
	switch {
	case sc.Dialecter.SupportNamedParameter():
		mode = 1
	case sc.Dialecter.SupportIndexedParameter():
		mode = 2
	}

	p := sc.placeHolder
	switch mode {
	case 0:
		sc.w.WriteString(p)
	case 1:
		sc.paraIndex++
		sc.w.WriteString(p + strconv.Itoa(sc.paraIndex))
	case 2:
		sc.paraIndex++
		sc.w.WriteString(p + strconv.Itoa(sc.paraIndex))
	}
	sc.args = append(sc.args, v)

}

func (sc *StmtCompiler) visitValue(v *Value) {
	if v == nil || v.Value == nil {
		sc.w.WriteString(ansi.Null)
		return
	}
	sc.writeValue(v.Value)
}

func (sc *StmtCompiler) visitColumn(c Column) {
	sc.w.WriteString(c.String())

	// table, column := c.Split()
	// if table == "" {
	// 	sc.WriteQuote(column)
	// } else {
	// 	sc.WriteQuote(table)
	// 	sc.w.WriteString(".")
	// 	sc.WriteQuote(column)
	// }

}
func (sc *StmtCompiler) visitTable(t *Table) {
	if t == nil || (t.Name == "" && t.Alias == "") {
		return
	} else if t.Name != "" && t.Alias != "" {
		sc.w.Print(t.Name, " ", ansi.As, " ", t.Alias)
	} else if t.Alias == "" {
		sc.w.WriteString(t.Name)
	} else if t.Name == "" {
		sc.w.WriteString(t.Alias)
	}

	return
}

func (sc *StmtCompiler) visitCondition(c *Condition) {
	if c == nil {
		return
	}

	if c.Right == nil && c.Left == nil {
		sc.w.WriteString(c.Op.String())
	} else if c.Left == nil {
		sc.w.Print(c.Op.String(), "(")
		sc.visitExp(c.Right)
		sc.w.Print(")")
	} else if c.Right == nil {
		sc.visitExp(c.Left)
		sc.w.Print(" ", c.Op.String())
	} else {
		if c.Op == In || c.Op == NotIn {
			sc.visitIn(c)
		} else {
			sc.visitExp(c.Left)
			sc.w.Print(" ", c.Op.String(), " ")
			sc.visitExp(c.Right)
		}
	}
}

func (sc *StmtCompiler) visitIn(c *Condition) {
	sc.visitExp(c.Left)
	sc.w.Print(" ", c.Op.String(), " ")

	sc.w.OpenParentheses()
	switch exp := c.Right.(type) {
	case *Value:
		if exp.Value != nil {
			sc.visitSlice(exp.Value)
		}
	default:
		sc.visitExp(exp)
	}

	sc.w.CloseParentheses()
}

func (sc *StmtCompiler) visitSlice(v interface{}) {
	switch v := v.(type) {
	case []int:
		for i := 0; i < len(v); i++ {
			if i > 0 {
				sc.w.Comma()
			}
			sc.w.WriteString(strconv.Itoa(v[i]))
		}
	case []int64:
		for i := 0; i < len(v); i++ {
			if i > 0 {
				sc.w.Comma()
			}
			sc.w.WriteString(strconv.FormatInt(v[i], 10))
		}
	case []float32:
		for i := 0; i < len(v); i++ {
			if i > 0 {
				sc.w.Comma()
			}
			sc.w.WriteString(strconv.FormatFloat(float64(v[i]), 'g', -1, 32))
		}
	case []float64:
		for i := 0; i < len(v); i++ {
			if i > 0 {
				sc.w.Comma()
			}
			sc.w.WriteString(strconv.FormatFloat(v[i], 'g', -1, 64))

		}
	case []string:
		for i := 0; i < len(v); i++ {
			if i > 0 {
				sc.w.Comma()
			}
			sc.writeValue(v[i])
		}
	case []interface{}:
		for i := 0; i < len(v); i++ {
			if i > 0 {
				sc.w.Comma()
			}
			sc.writeValue(v[i])

		}
	default:
		rv := reflect.Indirect(reflect.ValueOf(v))
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			for i := 0; i < rv.Len(); i++ {
				if i > 0 {
					sc.w.Comma()
				}
				sc.writeValue(rv.Index(i).Interface())

			}

		} else {
			sc.writeValue(v)
		}
	}
}

func (sc *StmtCompiler) visitConditions(c *Conditions) {
	if c == nil {
		return
	}

	deep := 0
	l := len(c.Conditions)

	for i := 0; i < l; i++ {
		item := c.Conditions[i]
		if item == nil {
			continue
		}

		if i > 0 {
			sc.w.LineBreak()
		}

		if item == CloseParentheses {
			deep--
		}

		if deep > 0 {
			sc.w.WriteString(strings.Repeat("\t", deep))
		}

		sc.visitExp(item)
		if item == OpenParentheses {
			deep++
		}
	}
	sc.w.Blank()
}

func (sc *StmtCompiler) visitJoin(j *Join) {
	if j == nil {
		return
	}

	sc.w.WriteString(j.JoinType.String())
	sc.w.Blank()
	sc.visitTable(j.Right)
	sc.w.Blank()

	if !j.Conditions.isEmpty() {
		sc.w.WriteString(ansi.On)
		for i := 0; i < len(j.Conditions.Conditions); i++ {
			sc.w.Blank()
			sc.visitExp(j.Conditions.Conditions[i])
			sc.w.Blank()
		}
	}

}

func (sc *StmtCompiler) visitFrom(f *From) {
	if f == nil {
		return
	}

	sc.w.Print("\n", ansi.From, " ")
	split := false

	if f.Table != nil {
		sc.visitTable(f.Table)
		split = true
	}

	for i := 0; i < len(f.Tables); i++ {
		if split {
			sc.w.Comma()
		}
		split = true
		sc.visitTable(f.Tables[i])
	}

	for i := 0; i < len(f.Joins); i++ {
		sc.w.LineBreak()
		sc.visitJoin(f.Joins[i])
	}
	sc.w.Blank()
}

func (sc *StmtCompiler) visitWhere(where *Where) {
	if where == nil || where.isEmpty() {
		return
	}
	sc.w.Print("\n", ansi.Where, "\n")
	sc.visitConditions(where.Conditions)
}

func (sc *StmtCompiler) visitField(f *Field) {
	if f == nil {
		return
	}

	sc.visitExp(f.Exp)
	if f.Alias != "" {
		sc.w.Print(" ", ansi.As, " ")
		sc.writeQuote(f.Alias)
	}
}

func (sc *StmtCompiler) visitSelect(slt *Select) {
	if slt == nil || len(slt.Fields) == 0 {
		sc.w.WriteString(ansi.WildcardAll)
		return
	}

	l := len(slt.Fields)
	split := false
	for i := 0; i < l; i++ {
		if split {
			sc.w.Comma()
		}
		split = true
		sc.visitField(slt.Fields[i])
	}

	sc.w.Blank()
}

func (sc *StmtCompiler) visitHaving(having *Having) {
	if having == nil {
		return
	}

	if having.Conditions.isEmpty() {
		return
	}

	sc.w.Print("\n", ansi.Having, "\n")
	sc.visitConditions(having.Conditions)
}

func (sc *StmtCompiler) visitGroupBy(groupBy *GroupBy) {
	if groupBy == nil {
		return
	}

	l := len(groupBy.Fields)
	if l <= 0 {
		return
	}

	sc.w.LineBreak()
	sc.w.WriteString(ansi.GroupBy)
	sc.w.Blank()

	split := false
	for i := 0; i < l; i++ {
		item := groupBy.Fields[i]
		if split {
			sc.w.Comma()
		}
		split = true
		sc.visitExp(item)
	}
	sc.w.Blank()
}

func (sc *StmtCompiler) visitOrderBy(orderBy *OrderBy) {
	if orderBy == nil {
		return
	}

	l := len(orderBy.Fields)
	if l <= 0 {
		return
	}

	sc.w.LineBreak()
	sc.w.WriteString(ansi.OrderBy)
	sc.w.Blank()
	split := false

	for i := 0; i < l; i++ {
		item := orderBy.Fields[i]
		if split {
			sc.w.Comma()
		}
		split = true
		sc.visitExp(item.Exp)
		sc.w.Blank()
		sc.w.WriteString(item.Direction.String())
	}
	sc.w.Blank()
}

func (sc *StmtCompiler) visitQuery(exp Expression) {
	query, _ := exp.(*Query)

	sc.w.WriteString(ansi.Select)
	sc.w.Blank()
	if query.IsDistinct {
		sc.w.WriteString(ansi.Distinct)
		sc.w.Blank()
	}

	sc.visitSelect(query.Select)
	sc.visitFrom(query.From)
	sc.visitWhere(query.Where)
	sc.visitGroupBy(query.GroupBy)
	if query.GroupBy != nil && len(query.GroupBy.Fields) > 0 {
		sc.visitHaving(query.Having)
	}
	sc.visitOrderBy(query.OrderBy)

	// limit, mssql doesn't support limit, need change to select * from (ROW_NUMBER(),...) where ...
	if query.Offset > 0 || query.Count > 0 {
		sc.w.LineBreak()
		sc.w.Print(ansi.Limit, " ", strconv.Itoa(query.Offset), ",", strconv.Itoa(query.Count))
	}

	sc.w.WriteString(ansi.StatementSplit)

}

func (sc *StmtCompiler) visitInsert(exp Expression) {
	insert, _ := exp.(*Insert)

	sc.w.Print(ansi.InsertInto, ansi.Blank, insert.Table.Name)

	l := len(insert.Sets)
	sc.w.OpenParentheses()
	for i := 0; i < l; i++ {
		if i > 0 {
			sc.w.Comma()
		}

		set := insert.Sets[i]
		sc.visitColumn(set.Column)
	}
	sc.w.CloseParentheses()

	sc.w.LineBreak()
	sc.w.WriteString(ansi.Values)
	sc.w.OpenParentheses()
	for i := 0; i < l; i++ {
		if i > 0 {
			sc.w.Comma()
		}

		set := insert.Sets[i]
		sc.visitExp(set.Value)
	}
	sc.w.CloseParentheses()

	sc.w.WriteString(ansi.StatementSplit)
}

func (sc *StmtCompiler) visitUpdate(exp Expression) {
	u, _ := exp.(*Update)

	sc.w.PrintSplit(ansi.Blank, ansi.Update, u.Table.Name, ansi.Set, ansi.LineBreak)
	l := len(u.Sets)
	for i := 0; i < l; i++ {
		if i > 0 {
			sc.w.Comma()
		}

		set := u.Sets[i]
		sc.visitColumn(set.Column)
		sc.w.WriteString(ansi.Equals)
		sc.visitExp(set.Value)
	}
	sc.visitWhere(u.Where)
	sc.visitOrderBy(u.OrderBy)
	if u.Count > 0 {
		sc.w.LineBreak()
		sc.w.PrintSplit(" ", ansi.Limit, strconv.Itoa(u.Count))
	}
	sc.w.WriteString(ansi.StatementSplit)

}

func (sc *StmtCompiler) visitDelete(exp Expression) {
	d, _ := exp.(*Delete)

	sc.w.PrintSplit(ansi.Blank, ansi.Delete, ansi.From, d.Table.Name)
	sc.visitWhere(d.Where)
	sc.visitOrderBy(d.OrderBy)
	if d.Count > 0 {
		sc.w.LineBreak()
		sc.w.PrintSplit(" ", ansi.Limit, strconv.Itoa(d.Count))
	}
	sc.w.WriteString(ansi.StatementSplit)

}

func MySql() Driver {
	return NewSqlDriver(MysqlDialecter{})
}

func PostgreSQL() Driver {
	return NewSqlDriver(PostgreSQLDialecter{})
}

func DefaultSQL() Driver {
	return NewSqlDriver(AnsiDialecter{})
}

func MSSQL() Driver {
	return NewSqlDriver(MssqlDialecter{})
}

func init() {
	RegisterDialecter("ansi", AnsiDialecter{})
	RegisterCompiler("ansi", DefaultSQL())

	RegisterDialecter("mysql", MysqlDialecter{})
	RegisterCompiler("mysql", MySql())

	RegisterDialecter("postgres", PostgreSQLDialecter{})
	RegisterCompiler("postgres", PostgreSQL())

	RegisterDialecter("adodb", MssqlDialecter{})
	RegisterCompiler("adodb", MSSQL())

	RegisterDialecter("lodbc", MssqlDialecter{})
	RegisterCompiler("lodbc", MSSQL())
}
