package kdb

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/sdming/kdb/ansi"
	"strings"
	"sync"
)

// State is connection state
type state int

const (
	// Opened means conection opened
	Opened state = 3

	// Closed means connection closed
	Closed state = 9
)

// DB is wrap of *sql.DB
type DB struct {
	DSN     *DSN
	innerdb *sql.DB
	state   state
}

// NewDB return *DB, initialize DSN with provided name
func NewDB(name string) *DB {
	dsn, _ := getDSN(name)
	return &DB{
		DSN: dsn,
	}
}

// DB return internal *sql.DB
func (db *DB) DB() *sql.DB {
	return db.innerdb
}

// Open open a database conection
func (db *DB) Open() error {
	if db.state == Opened {
		return nil
	}

	if db.DSN == nil || db.DSN.Driver == "" || db.DSN.Source == "" {
		logError("DB dsn is invalid", db.DSN)
		return errors.New("DB dsn is invalid")
	}

	if d, err := sql.Open(db.DSN.Driver, db.DSN.Source); err != nil {
		logError("DB open error", db.DSN, err)
		return err
	} else {
		db.state = Opened
		db.innerdb = d
	}

	if LogLevel >= LogDebug {
		logDebug("DB Open:", db.DSN)
	}

	return nil

}

// Close close database connection
func (db *DB) Close() error {
	if db.state != Opened {
		return nil
	}
	if err := db.innerdb.Close(); err != nil {
		logError("DB close error", db.DSN, err)
		return err
	} else {
		db.state = Closed
		db.innerdb = nil
	}

	if LogLevel >= LogDebug {
		logDebug("DB close:", db.DSN)
	}

	return nil
}

func (db *DB) dialecter() (dialect Dialecter, err error) {
	if db.DSN == nil || db.DSN.Driver == "" || db.DSN.Source == "" {
		err = errors.New("DB dsn is invalid")
		return
	}

	if dialect, err = GetDialecter(db.DSN.Driver); err != nil {
		return
	}
	return dialect, nil
}

// Function return schema of store procedure
func (db *DB) Function(name string) (fn *ansi.DbFunction, err error) {
	if err := db.Open(); err != nil {
		return nil, err
	}

	var dialect Dialecter
	if dialect, err = db.dialecter(); err != nil {
		return
	}
	query := dialect.FunctionSql(name)
	if query == "" {
		if schm, ok := dialect.(Schemaer); ok {
			return schm.Function(db.innerdb, name)
		}
		err = errors.New("driver doesn't support function schema:" + db.DSN.Driver)
		return
	}

	var rows *sql.Rows
	if rows, err = db.Query(query); err != nil {
		return
	}

	var f *ansi.DbFunction
	for rows.Next() {
		ff := ansi.NewFunction()

		if err = rows.Scan(&ff.Catalog, &ff.Schema, &ff.Name); err != nil {
			return
		} else {
			f = ff
		}
	}
	if err = rows.Err(); err != nil {
		return
	}

	if f == nil {
		err = errors.New("function doesn't exist:" + name)
		return
	}

	query = dialect.ParametersSql(name)
	if query == "" {
		err = errors.New("driver doesn't support function parameters schema:" + db.DSN.Driver)
		return
	}
	if rows, err = db.Query(query); err != nil {
		return
	}

	for rows.Next() {
		p := ansi.DbParameter{}
		dir := ""
		if err = rows.Scan(&p.Name, &p.Position, &dir, &p.NativeType, &p.Size, &p.Precision, &p.Scale); err != nil {
			return
		} else {
			p.DbType = dialect.DbType(p.NativeType)
			switch dir {
			case "IN":
				p.Dir = ansi.DirIn
			case "INOUT":
				p.Dir = ansi.DirInOut
			case "OUT":
				p.Dir = ansi.DirOut
			default:
				p.Dir = ansi.DirIn
			}
			f.Parameters = append(f.Parameters, p)
		}
	}
	if err = rows.Err(); err != nil {
		return
	}

	fn = f
	return

}

// Table return schema of table,view
func (db *DB) Table(name string) (table *ansi.DbTable, err error) {
	if err := db.Open(); err != nil {
		return nil, err
	}

	var dialect Dialecter
	if dialect, err = db.dialecter(); err != nil {
		return
	}
	query := dialect.TableSql(name)
	if query == "" {
		if schm, ok := dialect.(Schemaer); ok {
			return schm.Table(db.innerdb, name)
		}
		err = errors.New("driver doesn't support table schema:" + db.DSN.Driver)
		return
	}

	var rows *sql.Rows
	if rows, err = db.Query(query); err != nil {
		return
	}

	var t *ansi.DbTable
	for rows.Next() {
		tt := ansi.NewTable()

		if err = rows.Scan(&tt.Catalog, &tt.Schema, &tt.Name, &tt.Type); err != nil {
			//TODO:
		} else {
			t = tt
		}
	}
	if err = rows.Err(); err != nil {
		return
	}

	if t == nil {
		err = errors.New("table doesn't exist:" + name)
		return
	}

	query = dialect.ColumnsSql(name)
	if query == "" {
		err = errors.New("driver doesn't support columns schema:" + db.DSN.Driver)
		return
	}
	if rows, err = db.Query(query); err != nil {
		return
	}

	for rows.Next() {
		col := ansi.DbColumn{}

		if err = rows.Scan(&col.Name, &col.Position, &col.IsNullable, &col.NativeType, &col.Size, &col.Precision, &col.Scale, &col.IsAutoIncrement, &col.IsReadOnly, &col.IsPrimaryKey); err != nil {
			return
		} else {
			col.DbType = dialect.DbType(col.NativeType)
			t.Columns = append(t.Columns, col)
		}
	}

	if err = rows.Err(); err != nil {
		return
	}

	if len(t.Columns) == 0 {
		err = errors.New("table columns doesn't exist:" + name)
		return
	}

	table = t
	return

}

// Query executes a query that returns *sql.Rows
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if err := db.Open(); err != nil {
		return nil, err
	}
	rows, err := db.innerdb.Query(query, args...)
	if LogLevel >= LogDebug {
		logDebug("DB query:", query, args, err)
	}

	return rows, err
}

// Exec executes a query that return sql.Result
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	if err := db.Open(); err != nil {
		return nil, err
	}

	result, err := db.innerdb.Exec(query, args...)
	if LogLevel >= LogDebug {
		logDebug("DB exec:", query, args, result, err)
	}

	return result, err
}

// QueryText query a sql text templete
func (db *DB) QueryText(template string, args Getter) (*sql.Rows, error) {
	text, err := db.parseText(template, args)
	if err != nil {
		return nil, err
	}

	return db.QueryExp(text)
}

// ExecText exec a sql text template
func (db *DB) ExecText(query string, args Getter) (sql.Result, error) {
	text, err := db.parseText(query, args)
	if err != nil {
		return nil, err
	}

	return db.ExecExp(text)
}

func (db *DB) parseText(query string, args Getter) (*Text, error) {
	sql, names, err := CompileTemplate(query)
	if err != nil {
		return nil, err
	}

	l := len(names)
	var parameters []*Parameter
	if l > 0 {
		if args == nil {
			return nil, errors.New("query contains parameter, but args is nil")
		}

		parameters = make([]*Parameter, 0, l)
		for i := 0; i < l; i++ {
			name := names[i]

			if v, ok := args.Get(name); !ok {
				return nil, errors.New(fmt.Sprint("Can not get parameter:", name))
			} else {
				parameters = append(parameters, &Parameter{Name: name, Value: v})
			}
		}
	}

	return &Text{Sql: sql, Parameters: parameters}, nil
}

// QueryExp query a expression
func (db *DB) QueryExp(exp Expression) (*sql.Rows, error) {
	sql, args, err := db.Compile(exp)
	if err != nil {
		return nil, err
	}

	return db.Query(sql, args...)
}

// ExecExp execute a expression
func (db *DB) ExecExp(exp Expression) (sql.Result, error) {
	sql, args, err := db.Compile(exp)
	if err != nil {
		return nil, err
	}

	return db.Exec(sql, args...)
}

// Compile compile expression to native sql
func (db *DB) Compile(exp Expression) (sql string, args []interface{}, err error) {
	if db.DSN == nil {
		err = errors.New("kdb compile expression error, DSN is nil")
		return
	}

	var compiler Compiler
	compiler, err = GetCompiler(db.DSN.Driver)
	if err != nil {
		return
	}
	sql, args, err = compiler.Compile(db.DSN.Source, exp)
	return
}

func (db *DB) getFnSchema(name string) (fn *ansi.DbFunction, err error) {
	key := db.DSN.Name + ":" + name

	if f, ok := _schemaCache.function(key); ok {
		fn = f
		return
	}

	fn, err = db.Function(name)
	if LogLevel >= LogDebug {
		logDebug("DB get schema:", name, fn, err)
	}

	if err != nil {
		return
	}
	_schemaCache.setFunction(key, fn)
	return
}

func (db *DB) buildProcedure(name string, args Getter) (*Procedure, error) {
	fn, err := db.getFnSchema(name)
	if err != nil {
		return nil, err
	}

	sp := NewProcedure(name)
	sp.Name = fn.Name
	l := len(fn.Parameters)

	for i := 0; i < l; i++ {
		p := fn.Parameters[i]
		spp := &Parameter{
			Name: p.Name,
			Dir:  fn.Parameters[i].Dir,
		}

		var v interface{}
		var ok bool
		if args != nil {
			v, ok = args.Get(p.Name)
		}
		if ok {
			spp.Value = v
		} else if spp.IsIn() {
			return nil, errors.New("can not find parameter:" + p.Name)
		}
		sp.Parameter(spp)
	}

	return sp, nil
}

// QueryFunc query a store procedure
func (db *DB) QueryFunc(name string, args Getter) (*sql.Rows, error) {
	sp, err := db.buildProcedure(name, args)
	if err != nil {
		return nil, err
	}

	var rows *sql.Rows
	rows, err = db.QueryExp(sp)
	// TODO: output parameter
	return rows, err
}

// ExecFunc exec a store procedure
func (db *DB) ExecFunc(name string, args Getter) (sql.Result, error) {
	sp, err := db.buildProcedure(name, args)
	if err != nil {
		return nil, err
	}

	var result sql.Result
	result, err = db.ExecExp(sp)
	// TODO: output parameter
	return result, err
}

// Delete delete table by conditions, conditions format is column, operator, value, ...
func (db *DB) Delete(table string, conditions ...interface{}) (sql.Result, error) {
	d := NewDelete(table)
	if err := db.buildWhere(d.Where, conditions); err != nil {
		return nil, err
	}

	return db.ExecExp(d)
}

// DeleteByCol delete table with condition column = value
func (db *DB) DeleteByCol(table string, column string, value interface{}) (sql.Result, error) {
	d := NewDelete(table)
	d.Where.Compare(Equals, column, value)

	return db.ExecExp(d)
}

func (db *DB) buildWhere(w *Where, conditions []interface{}) error {
	l := len(conditions)
	if l%3 != 0 {
		return errors.New("conditions is invalid")
	}

	for i := 0; i < l; i++ {
		column, ok := conditions[i].(string)
		if !ok {
			return errors.New("conditions is invalid")
		}
		i++

		op, ok := conditions[i].(Operator)
		if !ok {
			if ops, ok := conditions[i].(string); !ok {
				return errors.New("conditions is invalid")
			} else {
				op = Operator(ops)
			}
		}
		i++

		value := conditions[i]
		w.Compare(op, column, value)
	}
	return nil
}

// SelectAll return table.*  by conditions, conditions format is column, operator, value, ...
func (db *DB) SelectAll(table string, conditions ...interface{}) (*sql.Rows, error) {
	q := NewQuery(table, "")
	if err := db.buildWhere(q.Where, conditions); err != nil {
		return nil, err
	}
	return db.QueryExp(q)
}

// SelectExists return true if exists conditions
func (db *DB) SelectExists(table string, conditions ...interface{}) (bool, error) {
	count, err := db.SelectCount(table, conditions...)
	if err != nil {
		return false, nil
	}
	return count > 0, nil
}

// SelectCount query select count(*) from [table] where conditions...
func (db *DB) SelectCount(table string, conditions ...interface{}) (count int64, err error) {
	q := NewQuery(table, "")
	q.Select.Aggregate(Count, Sql("*"), "countof")
	if err = db.buildWhere(q.Where, conditions); err != nil {
		return
	}

	var rows *sql.Rows
	rows, err = db.QueryExp(q)
	if err != nil {
		return
	}

	if err = scanScalar(rows, &count); err != nil {
		return
	}

	return
}

func (db *DB) getTableSchema(name string) (table *ansi.DbTable, err error) {
	key := db.DSN.Name + ":" + name
	if t, ok := _schemaCache.table(key); ok {
		table = t
		return
	}

	table, err = db.Table(name)
	if LogLevel >= LogDebug {
		logDebug("DB get schema:", name, table, err)
	}

	if err != nil {
		return
	}
	_schemaCache.setTable(key, table)
	return
}

// Update update a table to data with conditions...
func (db *DB) Update(table string, data Getter, conditions ...interface{}) (sql.Result, error) {
	var u *Update
	t, err := db.getTableSchema(table)
	if err != nil && ExplictSchema {
		return nil, err
	}

	if t == nil {
		iterater, ok := data.(Iterater)
		if !ok {
			return nil, errors.New("data isn't a Iterater")
		}
		fields := iterater.Fields()
		l := len(fields)
		if l == 0 {
			return nil, errors.New("data doesn't has any field")
		}
		u = NewUpdate(table)
		for i := 0; i < l; i++ {
			if v, ok := data.Get(fields[i]); ok {
				u.Set(fields[i], v)
			}
		}

	} else {
		u = NewUpdate(t.Name)
		l := len(t.Columns)
		for i := 0; i < l; i++ {
			col := t.Columns[i]
			if col.IsReadOnly || col.IsAutoIncrement {
				continue
			}
			if v, ok := data.Get(col.Name); ok {
				u.Set(col.Name, v)
			}
		}
	}

	db.buildWhere(u.Where, conditions)
	return db.ExecExp(u)
}

// // Update update a table to data with conditions...
// func (db *DB) Update(table string, data Getter, conditions ...interface{}) (int64, error) {
// 	var u *Update
// 	t, err := db.getTableSchema(table)
// 	if err != nil && ExplictSchema {
// 		return 0, err
// 	}

// 	if t == nil {
// 		iterater, ok := data.(Iterater)
// 		if !ok {
// 			return 0, errors.New("data isn't a Iterater")
// 		}
// 		fields := iterater.Fields()
// 		l := len(fields)
// 		if l == 0 {
// 			return 0, errors.New("data doesn't has any field")
// 		}
// 		u = NewUpdate(table)
// 		for i := 0; i < l; i++ {
// 			if v, ok := data.Get(fields[i]); ok {
// 				u.Set(fields[i], v)
// 			}
// 		}

// 	} else {
// 		u = NewUpdate(t.Name)
// 		l := len(t.Columns)
// 		for i := 0; i < l; i++ {
// 			col := t.Columns[i]
// 			if col.IsReadOnly || col.IsAutoIncrement {
// 				continue
// 			}
// 			if v, ok := data.Get(col.Name); ok {
// 				u.Set(col.Name, v)
// 			}
// 		}
// 	}

// 	db.buildWhere(u.Where, conditions)
// 	return rowsAffectedErr(db.ExecExp(u))
// }

// UpdateColumn exec table.column = value where conditions...
func (db *DB) UpdateColumn(table string, column string, value interface{}, conditions ...interface{}) (int64, error) {
	u := NewUpdate(table)
	u.Set(column, value)
	db.buildWhere(u.Where, conditions)
	return rowsAffectedErr(db.ExecExp(u))
}

// Insert insert data to table
func (db *DB) Insert(table string, data Getter) (sql.Result, error) {
	var insert *Insert
	t, err := db.getTableSchema(table)
	if err != nil && ExplictSchema {
		return nil, err
	}

	if t == nil {
		iterater, ok := data.(Iterater)
		if !ok {
			return nil, errors.New("data isn't a Iterater")
		}
		fields := iterater.Fields()
		l := len(fields)
		if l == 0 {
			return nil, errors.New("data doesn't has any field")
		}
		insert = NewInsert(table)
		for i := 0; i < l; i++ {
			if v, ok := data.Get(fields[i]); ok {
				insert.Set(fields[i], v)
			}
		}
	} else {
		insert = NewInsert(t.Name)
		l := len(t.Columns)
		for i := 0; i < l; i++ {
			col := t.Columns[i]
			if col.IsReadOnly || col.IsAutoIncrement {
				continue
			}
			if v, ok := data.Get(col.Name); ok {
				insert.Set(col.Name, v)
			}
		}
	}

	return db.ExecExp(insert)
}

// // Insert insert data to table
// func (db *DB) Insert(table string, data Getter) (int64, error) {
// 	var insert *Insert
// 	t, err := db.getTableSchema(table)
// 	if err != nil && ExplictSchema {
// 		return 0, err
// 	}

// 	if t == nil {
// 		iterater, ok := data.(Iterater)
// 		if !ok {
// 			return 0, errors.New("data isn't a Iterater")
// 		}
// 		fields := iterater.Fields()
// 		l := len(fields)
// 		if l == 0 {
// 			return 0, errors.New("data doesn't has any field")
// 		}
// 		insert = NewInsert(table)
// 		for i := 0; i < l; i++ {
// 			if v, ok := data.Get(fields[i]); ok {
// 				insert.Set(fields[i], v)
// 			}
// 		}
// 	} else {
// 		insert = NewInsert(t.Name)
// 		l := len(t.Columns)
// 		for i := 0; i < l; i++ {
// 			col := t.Columns[i]
// 			if col.IsReadOnly || col.IsAutoIncrement {
// 				continue
// 			}
// 			if v, ok := data.Get(col.Name); ok {
// 				insert.Set(col.Name, v)
// 			}
// 		}
// 	}

// 	return lastInsertIdErr(db.ExecExp(insert))
// }

type schemaCache struct {
	tables    map[string]*ansi.DbTable
	functions map[string]*ansi.DbFunction
	sync.RWMutex
}

func (sc *schemaCache) setFunction(key string, f *ansi.DbFunction) {
	if key == "" || f == nil {
		return
	}
	key = strings.ToLower(key)

	sc.Lock()
	sc.functions[key] = f
	sc.Unlock()
}

func (sc *schemaCache) setTable(key string, t *ansi.DbTable) {
	if key == "" || t == nil {
		return
	}
	key = strings.ToLower(key)

	sc.Lock()
	sc.tables[key] = t
	sc.Unlock()
}

func (sc *schemaCache) table(key string) (*ansi.DbTable, bool) {
	key = strings.ToLower(key)
	sc.RLock()
	t, ok := sc.tables[key]
	sc.RUnlock()

	return t, ok
}

func (sc *schemaCache) function(key string) (*ansi.DbFunction, bool) {
	key = strings.ToLower(key)
	sc.RLock()
	f, ok := sc.functions[key]
	sc.RUnlock()

	return f, ok
}

var _schemaCache *schemaCache = &schemaCache{
	tables:    make(map[string]*ansi.DbTable, 100),
	functions: make(map[string]*ansi.DbFunction, 100),
}
