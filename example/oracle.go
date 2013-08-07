package main

import (
	"database/sql"
	"fmt"
	//_ "github.com/mattn/go-oci8"
	"github.com/sdming/kdb"
	"github.com/sdming/kdb/ansi"
	"github.com/tgulacsi/goracle/godrv"
	"log"
	"os"
	"time"
)

var data map[string]interface{} = map[string]interface{}{
	"CBOOL":    1,
	"CINT":     42,
	"CFLOAT":   3.14,
	"CNUMERIC": 101.1,
	"CSTRING":  "string",
	"CDATE":    time.Now(),
}

func init() {
	kdb.RegisterDSN("demo", "goracle", "system/sa@kdbdemo")
	kdb.LogLevel = kdb.LogDebug
	kdb.Logger = log.New(os.Stdout, "kdb", log.Ldate|log.Ltime)

	godrv.SetAutoCommit(true)
}

func procedure() {
	db := kdb.NewDB("demo")
	fmt.Println("\nprocedure", "SP_EXEC")
	printResult(db.Exec("begin sp_exec(:v0); end; ", 1))
	db.Close()

	db = kdb.NewDB("demo")
	fmt.Println("\nExecFunc", "SP_EXEC")
	printResult(db.ExecFunc("SP_EXEC", kdb.Map(map[string]interface{}{
		"V_CINT": 123,
	})))
	db.Close()

	db = kdb.NewDB("demo")
	fmt.Println("\nProcedure", "SP_INOUT")
	sp := kdb.NewProcedure("SP_INOUT").
		Set("X", 3).
		SetDir("Y", 5, ansi.DirInOut).
		SetDir("S", nil, ansi.DirOut)
	printResult(db.ExecExp(sp))
	db.Close()

	//todo:
	//output, refcursor
}

func queryall() {
	db := kdb.NewDB("demo")
	defer db.Close()

	var query string

	query = "select * from ttable"
	fmt.Println("\nQuery:", query)
	printRows(db.Query(query))
}

func query() {
	db := kdb.NewDB("demo")
	defer db.Close()

	var query string

	query = "select * from ttable where cint > :1 "
	fmt.Println("\nQuery:", query)
	printRows(db.Query(query, 1))
}

func basic() {
	db := kdb.NewDB("demo")
	defer db.Close()

	var query string

	query = "select * from ttable where cint > :cint "
	fmt.Println("\nQuery:", query)
	printRows(db.Query(query, 1))

	query = "update ttable set cdate=sysdate where cint > :cint "
	fmt.Println("\nExec", query)
	printResult(db.Exec(query, 1))

	query = "select * from ttable where cint > {cint}"
	fmt.Println("\nQueryText", query)
	printRows(db.QueryText(query, kdb.Map(data)))

	query = "update ttable set cdate=sysdate where cint > {cint}"
	fmt.Println("\nExecText", query)
	printResult(db.ExecText(query, kdb.Map(data)))
}

func text() {
	db := kdb.NewDB("demo")
	defer db.Close()

	query := "select * from ttable where cint > {cint}"
	text := kdb.NewText(query).Set("cint", 1)
	fmt.Println("\nText query", query)
	printRows(db.QueryExp(text))

	query = "update ttable set cdate=sysdate where cint > {cint}"
	text = kdb.NewText(query).Set("cint", 42)
	fmt.Println("\nText exec", query)
	printResult(db.ExecExp(text))

}

func schema() {
	db := kdb.NewDB("demo")
	defer db.Close()

	fmt.Println("\nTable", "TTABLE")
	if table, err := db.Table("TTABLE"); err != nil {
		fmt.Println("Table", err)
	} else {
		fmt.Println(table)
	}

	fmt.Println("\nFunction", "SP_QUERY")
	if fn, err := db.Function("SP_QUERY"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(fn)
	}

	fmt.Println("\nFunction", "SP_EXEC")
	if fn, err := db.Function("SP_EXEC"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(fn)
	}

	fmt.Println("\nFunction", "SP_INOUT")
	if fn, err := db.Function("SP_INOUT"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(fn)
	}
}

func deleteTable() {
	db := kdb.NewDB("demo")
	defer db.Close()

	fmt.Println("\nDelete ttable by column")
	fmt.Println(db.DeleteByCol("TTABLE", "cint", 101))

	fmt.Println("\nDelete ttable by conditions")
	fmt.Println(db.Delete("TTABLE",
		"cint", kdb.Equals, 101,
		"cfloat", kdb.NotEquals, 3.14,
		"cstring", kdb.GreaterThan, "cstring",
		"cdate", "=", "2001-01-01",
	))

	fmt.Println("\nDelete ttable")
	del := kdb.NewDelete("TTABLE")
	del.Where.
		LessThan("cint", 10000).
		GreaterThan("cint", 101).
		NotEquals("cint", 12345).
		NotIn("cint", [5]int{5, 6, 7, 8, 9})
	printResult(db.ExecExp(del))

}

func selectTable() {
	db := kdb.NewDB("demo")
	defer db.Close()

	fmt.Println("\nSelectAll")
	printRows(db.SelectAll("TTABLE",
		"cint", kdb.GreaterThan, 1,
		"cfloat", kdb.LessThan, 123456,
		"cdate", "<>", time.Now(),
	))

	fmt.Println("\nSelectCount")
	fmt.Println(db.SelectCount("TTABLE"))

	fmt.Println("\nSelectExists")
	fmt.Println(db.SelectExists("TTABLE", "cint", kdb.GreaterThan, 123))

	fmt.Println("\nQuery ttable")
	q := kdb.NewQuery("TTABLE", "")
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
		IsNull("cstring").
		Or().
		IsNotNull("cstring").
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

func updateTable() {
	db := kdb.NewDB("demo")
	defer db.Close()

	fmt.Println("\nUpdateColumn")
	fmt.Println(db.UpdateColumn("TTABLE", "CSTRING", "cstring_update 42", "CINT", kdb.Equals, 42))

	fmt.Println("\nUpdate")
	fmt.Println(db.Update("TTABLE", kdb.Map(data), "CINT", kdb.Equals, 101))

	fmt.Println("\nUpdate ttable")
	u := kdb.NewUpdate("TTABLE")
	u.Set("cstring", "cstring new 1").
		Set("cfloat", 6.28)
	u.Where.Equals("cint", 1)

	printResult(db.ExecExp(u))

}

func insertTable() {
	db := kdb.NewDB("demo")
	defer db.Close()

	d1 := map[string]interface{}{
		"CBOOL":    1,
		"CINT":     123,
		"CFLOAT":   1.23,
		"CNUMERIC": 12.30,
		"CSTRING":  "a string",
		"CDATE":    time.Now(),
	}

	fmt.Println("\nInsert")
	fmt.Println(db.Insert("TTABLE", kdb.Map(d1)))

	fmt.Println("\nInsert ttable")
	insert := kdb.NewInsert("TTABLE")
	insert.Set("cbool", 0).
		Set("cint", 12345).
		Set("cfloat", 12.345).
		Set("cnumeric", 1234.50).
		Set("cstring", "string insert").
		Set("cdate", time.Now())

	printResult(db.ExecExp(insert))

}

func main() {
	// queryall()
	// query()
	// basic()
	// text()
	// insertTable()
	// updateTable()
	// selectTable()
	//deleteTable()

	//schema()

	procedure()
}

var panicWhenErr bool = true

func printRows(rows *sql.Rows, err error) {
	if err != nil {
		fmt.Println(err)

		if panicWhenErr {
			panic(err)
		}
		return
	}
	fmt.Println(kdb.DumpRows(rows))
	fmt.Println()
}

func printResult(result sql.Result, err error) {
	if err != nil {
		fmt.Println(err)
		if panicWhenErr {
			panic(err)
		}
		return
	}
	fmt.Println(kdb.DumpResult(result))
	fmt.Println()
}
