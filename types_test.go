package kdb

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseTag(t *testing.T) {
	s := "xx kdb:{nAme =test   ; pk; key=value} xx"
	tag := parseTag(s)

	t.Log(tag)

	if name, _ := tag.Option("name"); name != "test" {
		t.Errorf("tag Name() error, want=[%v]; actual=[%v]", "test", name)
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

type aEntity struct {
	Id     string "kdb:{pk}"
	FieldA string "kdb:{name=FA}"
	FieldB string "kdb:{name=FB;ingore}"
	FieldC string "kdb:{update;insert;}"
	FieldD string
}

func TestEntity(t *testing.T) {

	a := aEntity{
		Id:     "id",
		FieldA: "a",
		FieldB: "b",
		FieldC: "c",
		FieldD: "d",
	}

	e1 := Entity(a)

	want := []string{"Id", "FA", "FB", "FieldC", "FieldD"}
	if fields := e1.Fields(); strings.Join(fields, "") != strings.Join(want, "") {
		t.Errorf("Fields error; want=[%v]; actual=[%v]", want, fields)
	}

	if x, ok := e1.Get("id"); x != "id" || ok != true {
		t.Error("Get(id) error;", x, ok)
	}
	if x, ok := e1.Get("FieldA"); x != nil || ok != false {
		t.Error("Get(FieldA) error;", x, ok)
	}
	if x, ok := e1.Get("FA"); x != "a" || ok != true {
		t.Error("Get(FA) error;", x, ok)
	}
	if x, ok := e1.Get("fb"); x != "b" || ok != true {
		t.Error("Get(fb) error;", x, ok)
	}
	if x, ok := e1.Get("FieldC"); x != "c" || ok != true {
		t.Error("Get(FieldC) error;", x, ok)
	}
	if x, ok := e1.Get("FieldD"); x != "d" || ok != true {
		t.Error("Get(FieldD) error;", x, ok)
	}

	e2 := Entity(a, "ingore", "update", "insert")
	want = []string{"Id", "FA", "FieldD"}
	if fields := e2.Fields(); strings.Join(fields, "") != strings.Join(want, "") {
		t.Errorf("Fields error; want=[%v]; actual=[%v]", want, fields)
	}

	if x, ok := e2.Get("FB"); x != nil || ok != false {
		t.Error("Get(FB) error;", x, ok)
	}
	if x, ok := e2.Get("FieldC"); x != nil || ok != false {
		t.Error("Get(FieldC) error;", x, ok)
	}
	if x, ok := e2.Get("FieldD"); x != "d" || ok != true {
		t.Error("Get(FieldD) error;", x, ok)
	}

}
