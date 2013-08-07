/*
donesn't support guid & varbinary
*/

package main

import (
	"database/sql"
	"fmt"
	//_ "github.com/mattn/go-adodb"
	_ "github.com/LukeMauldin/lodbc"
	"github.com/sdming/kdb"
	"github.com/sdming/kdb/ansi"
	"log"
	"os"
	"time"
)

var data map[string]interface{} = map[string]interface{}{
	"cbool":     true,
	"cint":      42,
	"cfloat":    3.14,
	"cnumeric":  101.1,
	"cstring":   "string",
	"cdate":     "2004-07-24",
	"cdatetime": time.Now(),
	"cguid":     "550e8400-e29b-41d4-a716-446655440000",
}

func init() {
	//kdb.RegisterDSN("demo", "adodb", "Provider=sqloledb;Data Source=172.18.194.32;Initial Catalog=demo;User Id=sa;Password=sa;")
	kdb.RegisterDSN("demo", "lodbc", "Server=172.18.194.32;Database=demo;UID=sa;PWD=sa;Driver={SQL Server Native Client 10.0};")
	kdb.LogLevel = kdb.LogDebug
	kdb.Logger = log.New(os.Stdout, "kdb", log.Ldate|log.Ltime)
}

func procedure() {
	db := kdb.NewDB("demo")
	fmt.Println("\nQuery", "usp_query")
	printRows(db.Query("exec usp_query ?", 1))
	db.Close()

	db = kdb.NewDB("demo")
	fmt.Println("\nQueryFunc", "usp_query")
	printRows(db.QueryFunc("usp_query", kdb.Map(data)))
	db.Close()

	db = kdb.NewDB("demo")
	fmt.Println("\nExecFunc", "usp_exec")
	printResult(db.ExecFunc("usp_exec", kdb.Map(data)))
	db.Close()

	db = kdb.NewDB("demo")
	fmt.Println("\nProcedure", "usp_inout")
	sp := kdb.NewProcedure("usp_inout").
		Set("x", 3).
		SetDir("y", 5, ansi.DirOut).
		SetDir("sum", nil, ansi.DirOut)
	printRows(db.QueryExp(sp))
	db.Close()
}

func basic() {
	db := kdb.NewDB("demo")
	defer db.Close()

	var query string

	query = "select * from ttable where cint > ?"
	fmt.Println("\nQuery:", query)
	printRows(db.Query(query, 0))

	query = "update ttable set cdatetime=getdate() where cint > ?"
	fmt.Println("\nExec", query)
	printResult(db.Exec(query, 1))

	query = "select * from ttable where cint > {cint}"
	fmt.Println("\nQueryText", query)
	printRows(db.QueryText(query, kdb.Map(data)))

	query = "update ttable set cdatetime=getdate() where cint > {cint}"
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

	query = "update ttable set cdatetime=getdate() where cint > {cint}"
	text = kdb.NewText(query).Set("cint", 42)
	fmt.Println("\nText exec", query)
	printResult(db.ExecExp(text))

}

func schema() {
	db := kdb.NewDB("demo")
	defer db.Close()

	fmt.Println("\nTable", "ttable")
	if table, err := db.Table("ttable"); err != nil {
		fmt.Println("Table", err)
	} else {
		fmt.Println(table)
	}

	fmt.Println("\nFunction", "usp_query")
	if fn, err := db.Function("usp_query"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(fn)
	}

	fmt.Println("\nFunction", "usp_exec")
	if fn, err := db.Function("usp_exec"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(fn)
	}

	fmt.Println("\nFunction", "usp_inout")
	if fn, err := db.Function("usp_inout"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(fn)
	}

	fmt.Println("\nFunction", "usp_notexists")
	if fn, err := db.Function("usp_notexists"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(fn)
	}

}

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

func main() {
	basic()
	text()
	procedure()
	insertTable()
	updateTable()
	selectTable()
	deleteTable()
	schema()
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
