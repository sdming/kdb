package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sdming/kdb"
)

/*
CREATE TABLE ttypes (
	id 		int, 
	cbool 	int,
	cint 	int,
	cfloat 	float,
	cstring varchar(100)
);

insert ttypes(id,cbool,cint,cfloat,cstring) values(1, 1, 123, 3.14, 'string');
insert ttypes(id,cbool,cint,cfloat,cstring) values(2, null, null, null, null);

*/

func init() {
	kdb.RegisterDSN("demo", "mysql", "data:data@tcp(172.18.194.136:3306)/demo")
}

func readRow() {
	db := kdb.NewDB("demo")
	defer db.Close()

	fmt.Println("read row\n")

	// read to a *T
	var v int
	query := fmt.Sprintf("select cint from ttypes where id = 1 ")
	queryAndReadRow(db, query, &v)
	fmt.Println("read to int:", v == 123)

	// read to a []T
	l := []int{0}
	queryAndReadRow(db, query, l)
	fmt.Println("read to []int:", l[0] == 123)

	// read to a []*T
	v = 0
	lp := []*int{&v}
	queryAndReadRow(db, query, lp)
	fmt.Println("read to []*int:", v == 123)

	// read to a map[string]T
	m := map[string]int{}
	queryAndReadRow(db, query, m)
	fmt.Println("read to map[string]int:", m["cint"] == 123)

	// read to a map[string]T, with default value
	mv := map[string]int{"cint": 0}
	queryAndReadRow(db, query, mv)
	fmt.Println("read to map[string]int:", mv["cint"] == 123)

	// read to a map[string]*T
	v = 0
	mp := map[string]*int{"cint": &v}
	queryAndReadRow(db, query, mp)
	fmt.Println("read to map[string]*int:", v == 123)

	// read null to *T
	v = 321
	query = "select cint from ttypes where id = 2"
	queryAndReadRow(db, query, &v)
	fmt.Println("read null to int:", v == 321)

	// read null to []T
	l = []int{321}
	queryAndReadRow(db, query, l)
	fmt.Println("read null to []int:", l[0] == 321)

	// read null to []*T
	lp = []*int{&v}
	queryAndReadRow(db, query, l)
	fmt.Println("read null to []int:", v == 321)

	// read null to map[string]T
	m = map[string]int{}
	queryAndReadRow(db, query, m)
	fmt.Println("read null to map[string]int:", m["cint"] == 0)

	// read null to map[string]T, with default value
	mv = map[string]int{"cint": 321}
	queryAndReadRow(db, query, mv)
	fmt.Println("read null to map[string]int:", mv["cint"] == 321)

	// read null to map[string]*T
	v = 321
	mp = map[string]*int{"cint": &v}
	queryAndReadRow(db, query, mp)
	fmt.Println("read null to map[string]int:", v == 321)
}

func readRows() {
	db := kdb.NewDB("demo")
	defer db.Close()

	fmt.Println("\nread rows\n")

	// read rows to []T
	var v []int
	query := "select cint from ttypes where id in (1,2) order by id "
	queryAndRead(db, query, &v)
	fmt.Println("read rows to []int:", v[0] == 123, v[1] == 0)

	// read rows to []map[string]T
	m := make([]map[string]int, 0)
	queryAndRead(db, query, &m)
	fmt.Println("read rows to []map[string]int:", m[0]["cint"] == 123, m[1]["cint"] == 0)

	// read rows to [][]T
	l := make([][]int, 0)
	queryAndRead(db, query, &l)
	fmt.Println("read rows to [][]int:", l[0][0] == 123, l[1][0] == 0)

}

type Info struct {
	Id      int
	CBool   bool
	CInt    int
	CFloat  float32
	CString string
}

type InfoTag struct {
	Id     int     "kdb:{pk}"
	Bool   bool    "kdb:{name=cbool}"
	Int    int     "kdb:{name=cint}"
	Float  float32 "kdb:{name=cfloat}"
	String string  "kdb:{name=cstring}"
}

func readStruct() {
	db := kdb.NewDB("demo")
	defer db.Close()

	fmt.Println("\nread struct\n")

	// map rows to []T
	var v []Info
	query := "select * from ttypes where id in (1,2) order by id "
	queryAndRead(db, query, &v)
	fmt.Println("map rows to []Info", v[0], v[1])

	// map rows to []*T
	var vptr []*Info
	queryAndRead(db, query, &vptr)
	fmt.Println("map rows to []*Info", *(vptr[0]), *(vptr[1]))

	// map rows to []T, use tag
	var vtag []InfoTag
	queryAndRead(db, query, &vtag)
	fmt.Println("map rows to []InfoTag", vtag[0], vtag[1])
}

func main() {
	readRow()
	readRows()
	readStruct()
}

func queryAndReadRow(db *kdb.DB, query string, dest interface{}) {
	rows, err := db.Query(query)
	fmt.Println("query:", query, err)

	if err != nil {
		panic(err)
	}
	defer rows.Close()

	if rows.Next() {
		err = kdb.ReadRow(rows, dest)
		if err != nil {
			fmt.Println("ReadRow error", err)
			panic(err)
		}
	} else {
		fmt.Println("Query no result")
	}
}

func queryAndRead(db *kdb.DB, query string, dest interface{}) {
	rows, err := db.Query(query)
	fmt.Println("query:", query, err)

	if err != nil {
		panic(err)
	}
	defer rows.Close()

	err = kdb.Read(rows, dest)
	if err != nil {
		fmt.Println("Read error", err)
		panic(err)
	}

}
