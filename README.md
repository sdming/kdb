kdb
====

kdb is a package to wrap Go's [database/sql](http://golang.org/pkg/database/sql)

## Version

Current release: version 0.3 (2013-08-16)

## Document

[godoc](http://godoc.org/github.com/sdming/kdb)  

Sorry for bad english, if you want to improve documents, please contact me.  

## Features  

* lightweight, flexible and fast  
* orm  
* suppoort Sql template  
* support Store procedure, and out parameter   
* support get schema of table and store procedure  
* support sql expression  
* ingore NULL when scan rows  
* test on sql server, mysql, olite, postgres, oracle   
  
## Requirements

Go 1.1+  

## Installation

go get github.com/sdming/kdb 

## Register a new driver

Need to call RegisterDialecter/RegisterCompiler to bind your sql driver to a kdb.Dialecter and kdb.Compiler.  

example :


	RegisterDialecter("mysql", MysqlDialecter{})
	RegisterCompiler("mysql", MySql())

	RegisterDialecter("postgres", PostgreSQLDialecter{})
	RegisterCompiler("postgres", PostgreSQL())


## Register a DSN

Call RegisterDSN to register a DSN, example:


	kdb.RegisterDSN("demo", "postgres", "user=postgres password=sa dbname=postgres sslmode=disable client_encoding=UTF8")


## Example 

demo of how to query or execute sql in Go way:
		

	func basic() {
		db := kdb.NewDB("demo")
		defer db.Close()

		var query string

		query = "select * from ttable where cint > $1"
		fmt.Println("\nQuery:", query)
		printRows(db.Query(query, 1))

		query = "update ttable set cdatetime=NOW() where cint > $1"
		fmt.Println("\nExec", query)
		printResult(db.Exec(query, 1))
	}

demo of how to bind parameters to a map or struct


	query = "select * from ttable where cint > {cint}"
	fmt.Println("\nQueryText", query)
	printRows(db.QueryText(query, kdb.Map(data)))

	query = "update ttable set cdatetime=NOW() where cint > {cint}"
	fmt.Println("\nExecText", query)
	printResult(db.ExecText(query, kdb.Map(data)))


## Template

demo of how to run a template sql.  


	func text() {
		db := kdb.NewDB("demo")
		defer db.Close()

		query := "select * from ttable where cint > {cint}"
		text := kdb.NewText(query).Set("cint", 1)
		fmt.Println("\nText query", query)
		printRows(db.QueryExp(text))

		query = "update ttable set cdatetime=NOW() where cint > {cint}"
		text = kdb.NewText(query).Set("cint", 42)
		fmt.Println("\nText exec", query)
		printResult(db.ExecExp(text))

	}


## Store Procedure  

demo of how to run a store procedure.  

	
	func procedure() {
		db := kdb.NewDB("demo")
		fmt.Println("\nQueryFunc", "fn_query")
		printRows(db.QueryFunc("fn_query", kdb.Map(data)))
		db.Close()

		db = kdb.NewDB("demo")
		fmt.Println("\nExecFunc", "fn_exec")
		printRows(db.QueryFunc("fn_exec", kdb.Map(data)))
		db.Close()

		db = kdb.NewDB("demo")
		fmt.Println("\nProcedure", "fn_inout")
		sp := kdb.NewProcedure("fn_inout").
			Set("x", 3).
			SetDir("y", 5, ansi.DirInOut).
			SetDir("sum", nil, ansi.DirOut)
		printRows(db.QueryExp(sp))
		db.Close()
	}


## Select

demo of how to select from a table.  

	func selectTable() {
		db := kdb.NewDB("demo")
		defer db.Close()

		fmt.Println("\nSelectAll")
		printRows(db.SelectAll("ttable",
			"cint", kdb.GreaterThan, 1,
			"cfloat", kdb.LessThan, 123456,
			"cdatetime", "<>", time.Now(),
		))

		fmt.Println("\nSelectCount")
		fmt.Println(db.SelectCount("ttable"))

		fmt.Println("\nSelectExists")
		fmt.Println(db.SelectExists("ttable", "cint", kdb.GreaterThan, 12345))

		fmt.Println("\nQuery ttable")
		q := kdb.NewQuery("ttable", "")
		q.Select.
			Column("cbool", "cint").
			ColumnAs("cstring", "astring").
			Exp(kdb.Sql("cfloat-1"), "afloat").
			Avg("cnumeric", "aavg").
			Count("cnumeric", "acount").
			Max("cnumeric", "amax").
			Min("cnumeric", "amin").
			Sum("cnumeric", "asum")

		q.Where.
			OpenParentheses().
			IsNull("cbytes").
			Or().
			IsNotNull("cbytes").
			CloseParentheses()

		q.UseGroupBy().
			Column("cbool", "cint", "cstring").
			By(kdb.Sql("cfloat-1"))

		q.UseHaving().
			Count(kdb.GreaterThan, "cnumeric", 0).
			LessThan("cint", 12345)

		q.UseOrderBy().Asc("cbool", "cint").Desc("cstring")

		printRows(db.QueryExp(q))

	}


## Update 

demo of how to update a table.  

	func updateTable() {
		db := kdb.NewDB("demo")
		defer db.Close()

		fmt.Println("\nUpdateColumn")
		fmt.Println(db.UpdateColumn("ttable", "cstring", "cstring_update", "cint", kdb.Equals, 42))

		fmt.Println("\nUpdate")
		fmt.Println(db.Update("ttable", kdb.Map(data), "cint", kdb.Equals, 42))

		fmt.Println("\nUpdate ttable")
		u := kdb.NewUpdate("ttable")
		u.Set("cstring", "cstring new").
			Set("cfloat", 6.28)
		u.Where.Equals("cint", 42)

		printResult(db.ExecExp(u))
	}


## Delete

demo of how to delete from a table.  


	func deleteTable() {
		db := kdb.NewDB("demo")
		defer db.Close()

		fmt.Println("\nDelete ttable by column")
		fmt.Println(db.DeleteByCol("ttable", "cint", 101))

		fmt.Println("\nDelete ttable by conditions")
		fmt.Println(db.Delete("ttable",
			"cint", kdb.Equals, 101,
			"cfloat", kdb.NotEquals, 3.14,
			"cstring", kdb.GreaterThan, "cstring",
			"cdatetime", "=", "2001-01-01",
		))

		fmt.Println("\nDelete ttable")
		del := kdb.NewDelete("ttable")
		del.Where.
			LessThan("cint", 10000).
			GreaterThan("cint", 101).
			NotEquals("cint", 12345).
			NotIn("cint", [5]int{5, 6, 7, 8, 9})
		printResult(db.ExecExp(del))

	}


## Insert 

demo of how to insert into a table.  


	func insertTable() {
		db := kdb.NewDB("demo")
		defer db.Close()

		d1 := map[string]interface{}{
			"cbool":     true,
			"cint":      123,
			"cfloat":    1.23,
			"cnumeric":  12.30,
			"cstring":   "a string",
			"cdate":     "2000-01-23",
			"cdatetime": time.Now(),
		}

		fmt.Println("\nInsert")
		fmt.Println(db.Insert("ttable", kdb.Map(d1)))

		fmt.Println("\nInsert ttable")
		insert := kdb.NewInsert("ttable")
		insert.Set("cbool", 0).
			Set("cint", 12345).
			Set("cfloat", 12.345).
			Set("cnumeric", 1234.50).
			Set("cstring", "string insert").
			Set("cdate", "1979-01-02")

		printResult(db.ExecExp(insert))

	}


## Schema

demo of how to get schema of table\store procedure

	func schema() {
		db := kdb.NewDB("demo")
		defer db.Close()

		fmt.Println("\nTable", "ttable")
		if table, err := db.Table("ttable"); err != nil {
			fmt.Println("Table", err)
		} else {
			fmt.Println(table)
		}

		fmt.Println("\nFunction", "fn_query")
		if fn, err := db.Function("fn_query"); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(fn)
		}

		fmt.Println("\nFunction", "fn_exec")
		if fn, err := db.Function("fn_exec"); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(fn)
		}

		fmt.Println("\nFunction", "fn_inout")
		if fn, err := db.Function("fn_inout"); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(fn)
		}

		fmt.Println("\nFunction", "fn_notexists")
		if fn, err := db.Function("fn_notexists"); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(fn)
		}

	}

ReadRow(Scan)
===

ReadRow scan current row to a interface{}. kdb use NullInt64, NullBool... to scan values, it means it's safe to scan NULL .

data

	CREATE TABLE ttypes (
		id 			int, 
		cbool 	int,
		cint 		int,
		cfloat 	float,
		cstring varchar(100)
	);

	insert ttypes(id,cbool,cint,cfloat,cstring) values(1, 1, 123, 3.14, 'string');
	insert ttypes(id,cbool,cint,cfloat,cstring) values(2, null, null, null, null);


read row to a *T

	var v int
	query := fmt.Sprintf("select cint from ttypes where id = 1 ")
	queryAndReadRow(db, query, &v)
	fmt.Println("read to int:", v == 123)

read row to a []T

	l := []int{0}
	queryAndReadRow(db, query, l)
	fmt.Println("read to []int:", l[0] == 123)

read row to a []*T

	v = 0
	lp := []*int{&v}
	queryAndReadRow(db, query, lp)
	fmt.Println("read to []*int:", v == 123)

read row to to a map[string]T

	m := map[string]int{}
	queryAndReadRow(db, query, m)
	fmt.Println("read to map[string]int:", m["cint"] == 123)

read row to a map[string]T, with default value

	mv := map[string]int{"cint": 0}
	queryAndReadRow(db, query, mv)
	fmt.Println("read to map[string]int:", mv["cint"] == 123)

read row to a map[string]*T

	v = 0
	mp := map[string]*int{"cint": &v}
	queryAndReadRow(db, query, mp)
	fmt.Println("read to map[string]*int:", v == 123)

read row (null) to *T

	v = 321
	query = "select cint from ttypes where id = 2"
	queryAndReadRow(db, query, &v)
	fmt.Println("read null to int:", v == 321)

read row (null) to []T

	l = []int{321}
	queryAndReadRow(db, query, l)
	fmt.Println("read null to []int:", l[0] == 321)

read row (null) to []*T

	lp = []*int{&v}
	queryAndReadRow(db, query, l)
	fmt.Println("read null to []int:", v == 321)

read row (null) to map[string]T

	m = map[string]int{}
	queryAndReadRow(db, query, m)
	fmt.Println("read null to map[string]int:", m["cint"] == 0)

read row (null) to map[string]T, with default value

	mv = map[string]int{"cint": 321}
	queryAndReadRow(db, query, mv)
	fmt.Println("read null to map[string]int:", mv["cint"] == 321)

read row (null) to map[string]*T

	v = 321
	mp = map[string]*int{"cint": &v}
	queryAndReadRow(db, query, mp)
	fmt.Println("read null to map[string]int:", v == 321)


Read
---

Read copy rows to a slice []T.

read rows to []T

	var v []int
	query := "select cint from ttypes where id in (1,2) order by id "
	queryAndRead(db, query, &v)
	fmt.Println("read rows to []int:", v[0] == 123, v[1] == 0)

read rows to []map[string]T

	m := make([]map[string]int, 0)
	queryAndRead(db, query, &m)
	fmt.Println("read rows to []map[string]int:", m[0]["cint"] == 123, m[1]["cint"] == 0)

read rows to [][]T

	l := make([][]int, 0)
	queryAndRead(db, query, &l)
	fmt.Println("read rows to [][]int:", l[0][0] == 123, l[1][0] == 0)


Map rows to a struct
===

When use Read/ReadRow to copy rows to a struct, by default kdb map columns to struct fields with name, for example, copy column "foo" to field named "foo", you can use tags to change. 

tags demo:

	type InfoTag struct {
		Id     int     "kdb:{pk}"
		Bool   bool    "kdb:{name=cbool}"
		Int    int     "kdb:{name=cint}"
		Float  float32 "kdb:{name=cfloat}"
		String string  "kdb:{name=cstring}"
	}

	type Info struct {
		Id      int
		CBool   bool
		CInt    int
		CFloat  float32
		CString string
	}

map rows to []T

	var v []Info
	query := "select * from ttypes where id in (1,2) order by id "
	queryAndRead(db, query, &v)
	fmt.Println("map rows to []Info", v[0], v[1])

map rows to []*T

	var vptr []*Info
	queryAndRead(db, query, &vptr)
	fmt.Println("map rows to []*Info", *(vptr[0]), *(vptr[1]))

map rows to []T, use tags

	var vtag []InfoTag
	queryAndRead(db, query, &vtag)
	fmt.Println("map rows to []InfoTag", vtag[0], vtag[1])

## More examples

*[mysql](https://github.com/sdming/kdb/blob/master/example/mysql.go)  
*[postgres](https://github.com/sdming/kdb/blob/master/example/postgres.go)  
*[mssql](https://github.com/sdming/kdb/blob/master/example/mssql.go)  
*[sqlite](https://github.com/sdming/kdb/blob/master/example/sqlite.go)  
*[oracle](https://github.com/sdming/kdb/blob/master/example/oracle.go)  
*[read](https://github.com/sdming/kdb/blob/master/example/read.go)  

## Driver

*Mysql: https://github.com/Go-SQL-Driver/MySQL       
*PostgreSQL: https://github.com/bmizerany/pq      
*lodbc: https://github.com/LukeMauldin/lodbc   
*SQLite: https://github.com/changkong/go-sqlite3s       
*Oracle: https://github.com/mattn/go-oci8    


## TODO  

* ORM  


## License

MIT