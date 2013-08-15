package kdb

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"testing"
)

/*
CREATE TABLE ttypes (
	id 			int, 
	cbool 	int,
	cint 		int,
	cfloat 	float,
	cstring varchar(100)
);


delete from ttypes;

insert ttypes(id,cbool,cint,cfloat,cstring) values(1, 1, 123, 3.14, 'string');
insert ttypes(id,cbool,cint,cfloat,cstring) values(2, null, null, null, null);

select * from ttypes;

*/

func init() {
	RegisterDSN("demo", "mysql", "data:data@tcp(172.18.194.136:3306)/demo")
}

func queryRows(t *testing.T, query string) (*sql.Rows, error) {
	db := NewDB("demo")
	rows, err := db.Query(query)
	t.Log(query)

	if err != nil {
		t.Errorf("Query error: %s", query)
	}
	return rows, err
}

func queryAndRead(t *testing.T, query string, dest interface{}) bool {
	db := NewDB("demo")
	rows, err := db.Query(query)
	t.Log(query)

	if err != nil {
		t.Errorf("Query error: %s", query)
		return false
	}
	if rows.Next() {
		err = ReadRow(rows, dest)
		if err != nil {
			t.Errorf("ReadRow error: %v", err)
			return false
		}
	} else {
		t.Errorf("Query no result: %s", query)
		return false
	}

	return true
}

func testRows(t *testing.T, name string, dest interface{}, want interface{}) {
	query := fmt.Sprintf("select %s from ttypes where id in (1,2) order by id ", name)
	rows, err := queryRows(t, query)
	if err != nil {
		return
	}

	err = Read(rows, dest)
	if err != nil {
		t.Error("Read error", err)
		return
	}

	dv := reflect.Indirect(reflect.ValueOf(dest))
	t.Log("rows dest:", name, dv.Interface())

	wv := reflect.ValueOf(want)
	for i := 0; i < wv.Len(); i++ {
		if asString(dv.Index(i).Interface()) != asString(wv.Index(i).Interface()) {
			t.Errorf("want=[%v], actual=[%v]", wv.Index(i).Interface(), dv.Index(i).Interface())
		}
	}
}

func TestRowsBool(t *testing.T) {
	var dest []bool
	name := "cbool"
	want := []bool{true, false}
	testRows(t, name, &dest, want)
}

func TestRowBool(t *testing.T) {
	var v bool
	var want bool = true
	name := "cbool"

	t.Log(v, want)

	query := fmt.Sprintf("select %s from ttypes where id = 1 ", name)
	if queryAndRead(t, query, &v) && v != want {
		t.Errorf("Row row value error; want=[%v]; acutal=[%v] ", want, v)
	}

	l := []bool{false}
	if queryAndRead(t, query, l) && l[0] != want {
		t.Errorf("Row row slice value error; want=[%v]; acutal=[%v] ", want, v)
	}

	v = false
	lp := []*bool{&v}
	if queryAndRead(t, query, lp) && v != want {
		t.Errorf("Row row slice ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	m := map[string]bool{}
	if queryAndRead(t, query, m) && m[name] != want {
		t.Errorf("Row row map nvalue error; want=[%v]; acutal=[%v] ", want, m[name])
	}

	mv := map[string]bool{name: false}
	if queryAndRead(t, query, mv) && mv[name] != want {
		t.Errorf("Row row map value error; want=[%v]; acutal=[%v] ", want, mv[name])
	}

	v = false
	mp := map[string]*bool{name: &v}
	if queryAndRead(t, query, mp) && v != want {
		t.Errorf("Row row map ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	v = true
	query = fmt.Sprintf("select cbool from ttypes where id = 2")
	if queryAndRead(t, query, &v) && v != want {
		t.Errorf("Row row null value error; want=[%v]; acutal=[%v] ", want, v)
	}

	l = []bool{true}
	if queryAndRead(t, query, l) && l[0] != want {
		t.Errorf("Row row slice null value error; want=[%v]; acutal=[%v] ", want, l[0])
	}

	v = true
	lp = []*bool{&v}
	if queryAndRead(t, query, lp) && v != want {
		t.Errorf("Row row slice null ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	want = false
	m = map[string]bool{}
	if queryAndRead(t, query, m) && m[name] != want {
		t.Errorf("Row row map null nvalue error; want=[%v]; acutal=[%v] ", want, m[name])
	}

	want = true
	mv = map[string]bool{name: true}
	if queryAndRead(t, query, mv) && mv[name] != want {
		t.Errorf("Row row map null value error; want=[%v]; acutal=[%v] ", want, mv[name])
	}

	want = true
	v = true
	mp = map[string]*bool{name: &v}
	if queryAndRead(t, query, mp) && v != want {
		t.Errorf("Row row map null ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}
}

func TestRowsInt(t *testing.T) {
	var dest []int
	name := "cint"
	want := []int{123, 0}

	testRows(t, name, &dest, want)
}

func TestRowInt(t *testing.T) {
	var v int
	var want int = 123
	name := "cint"

	query := fmt.Sprintf("select %s from ttypes where id = 1 ", name)
	if queryAndRead(t, query, &v) && v != want {
		t.Errorf("Row row value error; want=[%v]; acutal=[%v] ", want, v)
	}

	l := []int{0}
	if queryAndRead(t, query, l) && l[0] != want {
		t.Errorf("Row row slice value error; want=[%v]; acutal=[%v] ", want, v)
	}

	v = 0
	lp := []*int{&v}
	if queryAndRead(t, query, lp) && l[0] != want {
		t.Errorf("Row row slice ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	m := map[string]int{}
	if queryAndRead(t, query, m) && m[name] != want {
		t.Errorf("Row row map nvalue error; want=[%v]; acutal=[%v] ", want, m[name])
	}

	mv := map[string]int{name: 0}
	if queryAndRead(t, query, mv) && mv[name] != want {
		t.Errorf("Row row map value error; want=[%v]; acutal=[%v] ", want, mv[name])
	}

	v = 0
	mp := map[string]*int{name: &v}
	if queryAndRead(t, query, mp) && v != want {
		t.Errorf("Row row map ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	v = 321
	want = 321
	query = fmt.Sprintf("select %s from ttypes where id = 2", name)
	if queryAndRead(t, query, &v) && v != want {
		t.Errorf("Row row null value error; want=[%v]; acutal=[%v] ", want, v)
	}

	l = []int{want}
	if queryAndRead(t, query, l) && l[0] != want {
		t.Errorf("Row row slice null value error; want=[%v]; acutal=[%v] ", want, l[0])
	}

	v = 321
	lp = []*int{&v}
	if queryAndRead(t, query, lp) && v != want {
		t.Errorf("Row row slice null ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	want = 0
	m = map[string]int{}
	if queryAndRead(t, query, m) && m[name] != want {
		t.Errorf("Row row map null nvalue error; want=[%v]; acutal=[%v] ", want, m[name])
	}

	want = 321
	mv = map[string]int{name: 321}
	if queryAndRead(t, query, mv) && mv[name] != want {
		t.Errorf("Row row map null value error; want=[%v]; acutal=[%v] ", want, mv[name])
	}

	want = 321
	v = 321
	mp = map[string]*int{name: &v}
	if queryAndRead(t, query, mp) && v != want {
		t.Errorf("Row row map null ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}
}

func TestRowsString(t *testing.T) {
	var dest []string
	name := "cstring"
	want := []string{"string", ""}

	testRows(t, name, &dest, want)
}

func TestRowString(t *testing.T) {
	var v string
	var want string = "string"
	name := "cstring"

	query := fmt.Sprintf("select %s from ttypes where id = 1 ", name)
	if queryAndRead(t, query, &v) && v != want {
		t.Errorf("Row row value error; want=[%v]; acutal=[%v] ", want, v)
	}

	l := []string{"a"}
	if queryAndRead(t, query, l) && l[0] != want {
		t.Errorf("Row row slice value error; want=[%v]; acutal=[%v] ", want, v)
	}

	v = "a"
	lp := []*string{&v}
	if queryAndRead(t, query, lp) && l[0] != want {
		t.Errorf("Row row slice ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	m := map[string]string{}
	if queryAndRead(t, query, m) && m[name] != want {
		t.Errorf("Row row map nvalue error; want=[%v]; acutal=[%v] ", want, m[name])
	}

	mv := map[string]string{name: "a"}
	if queryAndRead(t, query, mv) && mv[name] != want {
		t.Errorf("Row row map value error; want=[%v]; acutal=[%v] ", want, mv[name])
	}

	v = "a"
	mp := map[string]*string{name: &v}
	if queryAndRead(t, query, mp) && v != want {
		t.Errorf("Row row map ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	v = "a"
	want = "a"
	query = fmt.Sprintf("select %s from ttypes where id = 2", name)
	if queryAndRead(t, query, &v) && v != want {
		t.Errorf("Row row null value error; want=[%v]; acutal=[%v] ", want, v)
	}

	l = []string{want}
	if queryAndRead(t, query, l) && l[0] != want {
		t.Errorf("Row row slice null value error; want=[%v]; acutal=[%v] ", want, l[0])
	}

	v = "a"
	lp = []*string{&v}
	if queryAndRead(t, query, lp) && v != want {
		t.Errorf("Row row slice null ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	want = ""
	m = map[string]string{}
	if queryAndRead(t, query, m) && m[name] != want {
		t.Errorf("Row row map null nvalue error; want=[%v]; acutal=[%v] ", want, m[name])
	}

	want = "a"
	mv = map[string]string{name: "a"}
	if queryAndRead(t, query, mv) && mv[name] != want {
		t.Errorf("Row row map null value error; want=[%v]; acutal=[%v] ", want, mv[name])
	}

	want = "a"
	v = "a"
	mp = map[string]*string{name: &v}
	if queryAndRead(t, query, mp) && v != want {
		t.Errorf("Row row map null ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}
}

func TestRowsFloat(t *testing.T) {
	var dest []float32
	name := "cfloat"
	want := []float32{3.14, 0}

	testRows(t, name, &dest, want)
}

func TestRowFloat(t *testing.T) {
	var v float32
	var want string = "3.14"
	name := "cfloat"

	query := fmt.Sprintf("select %s from ttypes where id = 1 ", name)
	if queryAndRead(t, query, &v) && asString(v) != want {
		t.Errorf("Row row value error; want=[%v]; acutal=[%v] ", want, v)
	}

	l := []float32{1.1}
	if queryAndRead(t, query, l) && asString(l[0]) != want {
		t.Errorf("Row row slice value error; want=[%v]; acutal=[%v] ", want, v)
	}

	v = 1.1
	lp := []*float32{&v}
	if queryAndRead(t, query, lp) && asString(l[0]) != want {
		t.Errorf("Row row slice ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	m := map[string]float32{}
	if queryAndRead(t, query, m) && asString(m[name]) != want {
		t.Errorf("Row row map nvalue error; want=[%v]; acutal=[%v] ", want, m[name])
	}

	mv := map[string]float32{name: 1.1}
	if queryAndRead(t, query, mv) && asString(mv[name]) != want {
		t.Errorf("Row row map value error; want=[%v]; acutal=[%v] ", want, mv[name])
	}

	v = 1.1
	mp := map[string]*float32{name: &v}
	if queryAndRead(t, query, mp) && asString(v) != want {
		t.Errorf("Row row map ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	v = 1.1
	want = "1.1"
	query = fmt.Sprintf("select %s from ttypes where id = 2", name)
	if queryAndRead(t, query, &v) && asString(v) != want {
		t.Errorf("Row row null value error; want=[%v]; acutal=[%v] ", want, v)
	}

	l = []float32{1.1}
	if queryAndRead(t, query, l) && asString(l[0]) != want {
		t.Errorf("Row row slice null value error; want=[%v]; acutal=[%v] ", want, l[0])
	}

	v = 1.1
	lp = []*float32{&v}
	if queryAndRead(t, query, lp) && asString(v) != want {
		t.Errorf("Row row slice null ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}

	want = "0"
	m = map[string]float32{}
	if queryAndRead(t, query, m) && asString(m[name]) != want {
		t.Errorf("Row row map null nvalue error; want=[%v]; acutal=[%v] ", want, m[name])
	}

	want = "1.1"
	mv = map[string]float32{name: 1.1}
	if queryAndRead(t, query, mv) && asString(mv[name]) != want {
		t.Errorf("Row row map null value error; want=[%v]; acutal=[%v] ", want, mv[name])
	}

	want = "1.1"
	v = 1.1
	mp = map[string]*float32{name: &v}
	if queryAndRead(t, query, mp) && asString(v) != want {
		t.Errorf("Row row map null ptr value error; want=[%v]; acutal=[%v] ", want, v)
	}
}

func TestRowsMap(t *testing.T) {
	want := map[string]interface{}{
		"cbool":   "1",
		"cint":    "123",
		"cfloat":  "3.14",
		"cstring": "string",
	}
	dest := make([]map[string]interface{}, 0)

	query := fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id in (1,2) order by id ")
	rows, err := queryRows(t, query)
	if err != nil {
		return
	}

	err = Read(rows, &dest)
	if err != nil {
		t.Error("Read error", err)
		return
	}

	t.Log("rows dest:", dest)
	if len(dest) != 2 {
		t.Errorf("dest len error; want=[%v]; value=[%v]", 2, len(dest))
		return
	}

	d1 := dest[0]
	for k, _ := range want {
		if asString(d1[k]) != asString(want[k]) {
			t.Errorf("read map value error;key=[%v]; want=[%v]; value=[%v]", k, want[k], d1[k])
		}
	}

	d2 := dest[1]
	for k, _ := range want {
		if d2[k] != nil {
			t.Errorf("read map null value error;key=[%v]; want=[%v]; value=[%v]", k, nil, d2[k])
		}
	}

}

func TestRowMap(t *testing.T) {
	want := map[string]interface{}{
		"cbool":   "1",
		"cint":    "123",
		"cfloat":  "3.14",
		"cstring": "string",
	}
	v := make(map[string]interface{})

	query := fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id = 1 ")
	if queryAndRead(t, query, v) {
		for k, _ := range want {
			if asString(v[k]) != want[k] {
				t.Errorf("Read Map value error;key=[%v]; want=[%v]; value=[%v]", k, want[k], v[k])
			}
		}
	}

	query = fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id = 2 ")
	v = make(map[string]interface{})
	if queryAndRead(t, query, v) {
		for k, _ := range want {
			if v[k] != nil {
				t.Errorf("Read Map null value error;key=[%v]; want=[%v]; value=[%v]", k, nil, v[k])
			}
		}
	}
}

func TestRowMapPtr(t *testing.T) {
	var b bool
	var i int
	var f float32
	var s string

	v := map[string]interface{}{
		"cbool":   &b,
		"cint":    &i,
		"cfloat":  &f,
		"cstring": &s,
	}

	want := map[string]interface{}{
		"cbool":   true,
		"cint":    int(123),
		"cfloat":  float32(3.14),
		"cstring": "string",
	}

	query := fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id = 1 ")
	if queryAndRead(t, query, v) {

		vv := map[string]interface{}{
			"cbool":   b,
			"cint":    i,
			"cfloat":  f,
			"cstring": s,
		}

		for k, _ := range want {
			if asString(vv[k]) != asString(want[k]) {
				t.Errorf("Read Map ptr value error;key=[%v]; want=[%v]; value=[%v]", k, want[k], vv[k])
			}
		}
	}
}

func TestRowsSlice(t *testing.T) {
	want := []interface{}{"1", "123", "3.14", "string"}
	dest := make([][]interface{}, 0)

	query := fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id in (1,2) order by id ")
	rows, err := queryRows(t, query)
	if err != nil {
		return
	}

	err = Read(rows, &dest)
	if err != nil {
		t.Error("Read error", err)
		return
	}

	t.Log("rows dest:", dest)
	if len(dest) != 2 {
		t.Errorf("dest len error; want=[%v]; value=[%v]", 2, len(dest))
		return
	}

	d1 := dest[0]
	for i, _ := range want {
		if asString(d1[i]) != asString(want[i]) {
			t.Errorf("read slice value error; i=[%v]; want=[%v]; value=[%v]", i, want[i], d1[i])
		}
	}

	d2 := dest[1]
	for i, _ := range want {
		if d2[i] != nil {
			t.Errorf("read slice null value error; i=[%v]; want=[%v]; value=[%v]", i, nil, d2[i])
		}
	}

}

func TestRowSlice(t *testing.T) {
	want := []interface{}{"1", "123", "3.14", "string"}
	v := make([]interface{}, len(want))

	query := fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id = 1 ")
	if queryAndRead(t, query, v) {
		for i, _ := range want {
			if asString(v[i]) != want[i] {
				t.Errorf("Read Slice value error;i=[%v]; want=[%v]; value=[%v]", i, want[i], v[i])
			}
		}
	}

	query = fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id = 2 ")
	v = make([]interface{}, len(want))
	if queryAndRead(t, query, v) {
		for i, _ := range want {
			if v[i] != nil {
				t.Errorf("Read Slice null value error;i=[%v]; want=[%v]; value=[%v]", i, nil, v[i])
			}
		}
	}
}

func TestRowSlicePtr(t *testing.T) {
	var b bool
	var i int
	var f float32
	var s string

	v := []interface{}{&b, &i, &f, &s}

	want := []interface{}{true, int(123), float32(3.14), "string"}

	query := fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id = 1 ")
	if queryAndRead(t, query, v) {

		vv := []interface{}{b, i, f, s}

		for i, _ := range want {
			if asString(vv[i]) != asString(want[i]) {
				t.Errorf("Read Slice ptr value error;i=[%v]; want=[%v]; value=[%v]", i, want[i], vv[i])
			}
		}
	}
}

func TestReadScan(t *testing.T) {
	var b bool
	var i int
	var f float32
	var s string

	want := map[string]interface{}{
		"cbool":   true,
		"cint":    int(123),
		"cfloat":  float32(3.14),
		"cstring": "string",
	}

	query := fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id = 1 ")
	if rows, err := queryRows(t, query); err != nil {
		if rows.Next() {
			ReadRowScan(rows, &b, &f, &f, &s)
			vv := map[string]interface{}{
				"cbool":   b,
				"cint":    i,
				"cfloat":  f,
				"cstring": s,
			}

			for k, _ := range want {
				if asString(vv[k]) != asString(want[k]) {
					t.Errorf("Read Scan value error;key=[%v]; want=[%v]; value=[%v]", k, want[k], vv[k])
				}
			}

		}
	}

	b = true
	i = 321
	f = 1.1
	s = "a"
	want = map[string]interface{}{
		"cbool":   b,
		"cint":    i,
		"cfloat":  f,
		"cstring": s,
	}

	query = fmt.Sprintf("select cbool,cint,cfloat,cstring from ttypes where id = 2 ")
	if rows, err := queryRows(t, query); err != nil {
		if rows.Next() {
			ReadRowScan(rows, &b, &f, &f, &s)
			vv := map[string]interface{}{
				"cbool":   b,
				"cint":    i,
				"cfloat":  f,
				"cstring": s,
			}

			for k, _ := range want {
				if asString(vv[k]) != asString(want[k]) {
					t.Errorf("Read Scan null value error;key=[%v]; want=[%v]; value=[%v]", k, want[k], vv[k])
				}
			}

		}
	}

}

type TypeInfo struct {
	Id      int
	CBool   bool
	CInt    int
	CFloat  float32
	CString string
}

type TypeInfoTag struct {
	Id     int      "kdb:{pk}"
	Bool   *bool    "kdb:{name=cbool}"
	Int    *int     "kdb:{name=cint}"
	Float  *float32 "kdb:{name=cfloat}"
	String *string  "kdb:{name=cstring}"
}

func TestParseTag(t *testing.T) {
	s := "xx kdb:{nAme =test   ; pk; key=value} xx"
	tag := parseTag(s)

	t.Log(tag)

	if tag.Name() != "test" {
		t.Errorf("tag Name() error, want=[%v]; actual=[%v]", "test", tag.Name())
	}
	if !tag.Contains("pk") {
		t.Errorf("tag Contains error, want=[%v]; actual=[%v]", true, tag.Contains("pk"))
	}
	if v, _ := tag.Option("key"); v != "value" {
		t.Errorf("tag Option error, want=[%v]; actual=[%v]", "value", v)
	}

}

func TestParseStruct(t *testing.T) {
	var a interface{} = TypeInfoTag{}

	si, err := getStructInfo(reflect.TypeOf(a))
	if err != nil {
		t.Error("getStructInfo error", err)
		return
	}

	fi, ok := si.FieldByColName("CBOOL")
	if !ok {
		t.Errorf("FieldByColName error, want=[%v]; atucal=[%v]", true, ok)
	}
	if fi.fName != "Bool" {
		t.Errorf("fName error, want=[%v]; atucal=[%v]", "bool", fi.fName)
	}
	if fi.colName != "cbool" {
		t.Errorf("colName error, want=[%v]; atucal=[%v]", "cbool", fi.colName)
	}
}

func TestRowStruct(t *testing.T) {
	var v TypeInfo

	query := fmt.Sprintf("select * from ttypes where id = 1 ")
	if ok := queryAndRead(t, query, &v); !ok {
		t.Errorf("Read row struct error; want=[%v]; acutal=[%v] ", true, ok)
		return
	}

	t.Log(v)

	want := TypeInfo{
		CBool:   true,
		CFloat:  3.14,
		CInt:    123,
		CString: "string",
		Id:      1,
	}
	if v != want {
		t.Errorf("Read row struct value error; want=[%v]; acutal=[%v] ", want, v)
	}

	v = TypeInfo{
		CBool:   true,
		CFloat:  1.1,
		CInt:    321,
		CString: "a",
		Id:      2,
	}

	query = fmt.Sprintf("select * from ttypes where id = 2 ")
	if ok := queryAndRead(t, query, &v); !ok {
		t.Errorf("Read row struct null error; want=[%v]; acutal=[%v] ", true, ok)
		return
	}

	t.Log(v)

	want = TypeInfo{
		CBool:   true,
		CFloat:  1.1,
		CInt:    321,
		CString: "a",
		Id:      2,
	}
	if v != want {
		t.Errorf("Read row struct null value error; want=[%v]; acutal=[%v] ", want, v)
	}
}

func TestRowStructTag(t *testing.T) {
	var v TypeInfoTag
	v = TypeInfoTag{}

	query := fmt.Sprintf("select * from ttypes where id = 1 ")
	if ok := queryAndRead(t, query, &v); !ok {
		t.Errorf("Read row struct error; want=[%v]; acutal=[%v] ", true, ok)
		return
	}

	t.Log(v)

	if v.Id != 1 || *v.Bool != true || asString(*v.Float) != "3.14" || *v.Int != 123 || *v.String != "string" {
		t.Errorf("Read row struct tag value error; id=[%v]; bool=[%v]; int=[%v]; float=[%v]; string=[%v]; ", v.Id, *v.Bool, *v.Int, *v.Float, *v.String)
	}

	v = TypeInfoTag{}

	query = fmt.Sprintf("select * from ttypes where id = 2 ")
	if ok := queryAndRead(t, query, &v); !ok {
		t.Errorf("Read row struct null error; want=[%v]; acutal=[%v] ", true, ok)
		return
	}

	t.Log(v)

	if v.Id != 2 || v.Bool != nil || v.Float != nil || v.Int != nil || v.String != nil {
		t.Errorf("Read row struct tag null value error; id=[%v]; bool=[%v]; int=[%v]; float=[%v]; string=[%v]; ", v.Id, v.Bool, v.Int, v.Float, v.String)
	}

}

func BenchmarkParseStruct(b *testing.B) {
	//go test --bench .*
	b.StopTimer()

	db := NewDB("demo")
	defer db.Close()

	query := "select * from ttypes limit 1,1000 "
	rows, err := db.Query(query)
	if err != nil {
		b.Errorf("Query error: %s", query)
		return
	}
	defer rows.Close()

	b.StartTimer()

	count := 0
	for rows.Next() {
		count++
		var v TypeInfo
		err = ReadRow(rows, &v)
		if err != nil {
			b.Errorf("ReadRow error: %v", err)
			break
		}
	}
	b.Log("count", count)
	if rows.Err() != nil {
		b.Error(rows.Err())
	}

}

func TestRowsStruct(t *testing.T) {
	var dest []TypeInfo

	query := fmt.Sprintf("select * from ttypes where id in (1,2) order by id ")
	rows, err := queryRows(t, query)
	if err != nil {
		return
	}

	err = Read(rows, &dest)
	if err != nil {
		t.Error("Read error", err)
		return
	}

	t.Log(dest)

	want := TypeInfo{
		CBool:   true,
		CFloat:  3.14,
		CInt:    123,
		CString: "string",
		Id:      1,
	}
	if dest[0] != want {
		t.Errorf("Read rows struct value error; want=[%v]; acutal=[%v] ", want, dest[0])
	}

	want = TypeInfo{
		CBool:   false,
		CFloat:  0,
		CInt:    0,
		CString: "",
		Id:      2,
	}
	if dest[1] != want {
		t.Errorf("Read rows struct null value error; want=[%v]; acutal=[%v] ", want, dest[1])
	}
}

func TestRowsStructPtr(t *testing.T) {
	var dest []*TypeInfo

	query := fmt.Sprintf("select * from ttypes where id in (1,2) order by id ")
	rows, err := queryRows(t, query)
	if err != nil {
		return
	}

	err = Read(rows, &dest)
	if err != nil {
		t.Error("Read error", err)
		return
	}

	t.Log(dest)

	want := TypeInfo{
		CBool:   true,
		CFloat:  3.14,
		CInt:    123,
		CString: "string",
		Id:      1,
	}
	if *(dest[0]) != want {
		t.Errorf("Read rows struct value error; want=[%v]; acutal=[%v] ", want, *(dest[0]))
	}

	want = TypeInfo{
		CBool:   false,
		CFloat:  0,
		CInt:    0,
		CString: "",
		Id:      2,
	}
	if *(dest[1]) != want {
		t.Errorf("Read rows struct null value error; want=[%v]; acutal=[%v] ", want, *(dest[1]))
	}
}

func BenchmarkReadStruct(b *testing.B) {
	b.StopTimer()

	var dest []TypeInfo

	db := NewDB("demo")
	defer db.Close()

	query := "select * from ttypes limit 1,1000 "
	rows, err := db.Query(query)
	if err != nil {
		b.Errorf("Query error: %s", query)
		return
	}
	defer rows.Close()

	b.StartTimer()

	err = Read(rows, &dest)
	if err != nil {
		b.Error("Read error", err)
		return
	}

	b.Log("len(dest)", len(dest))
}
