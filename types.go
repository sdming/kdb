package kdb

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// tagStart is prefix of kdb tag
var tagStart []byte = []byte("kdb:{")

// tagEnd is suffix of kdb tag
var tagEnd byte = byte('}')

// tagOptions is wrap of kdb tag information
type tagOptions struct {
	tag     string
	options map[string]string
}

// Option return option value by name
func (tag *tagOptions) Option(name string) (string, bool) {
	name, ok := tag.options[name]
	return name, ok
}

// Name return option value of name
func (tag *tagOptions) Name() string {
	name, ok := tag.options["name"]
	if !ok {
		return ""
	}
	return name
}

// Contains return true if tagOptions contains option name
func (tag *tagOptions) Contains(name string) bool {
	_, ok := tag.options[name]
	return ok
}

// parseOptions parse a string like k1:v1;k2:v2; ... to map[string]string
func parseOptions(s string) map[string]string {
	options := make(map[string]string)

	items := strings.Split(s, ";")
	for i := 0; i < len(items); i++ {
		kv := strings.Split(items[i], "=")
		if len(kv) == 2 {
			k := strings.TrimSpace(kv[0])
			if k != "" {
				options[strings.ToLower(k)] = strings.TrimSpace(kv[1])
			}
		} else if len(kv) == 1 {
			k := strings.TrimSpace(kv[0])
			if k != "" {
				options[strings.ToLower(k)] = ""
			}
		}
	}
	return options
}

// parseTag parse tagOptions from a string like kdb:{name:x;pk;...}
func parseTag(tag string) *tagOptions {
	b := []byte(tag)
	if start := bytes.Index(b, tagStart); start != -1 {
		start = start + len(tagStart)
		b := b[start:]
		if end := bytes.IndexByte(b, tagEnd); end != -1 {
			t := tag[start : start+end]
			return &tagOptions{
				tag:     t,
				options: parseOptions(t),
			}
		}
	}

	return &tagOptions{
		tag:     "",
		options: make(map[string]string),
	}
}

// structInfo is a wrap of kdb struct information
type structInfo struct {
	sType  reflect.Type
	fields []*fieldInfo
}

// FieldByColName return field which colName equal name
func (si *structInfo) FieldByColName(name string) (*fieldInfo, bool) {
	l := len(si.fields)
	for i := 0; i < l; i++ {
		f := si.fields[i]
		if strings.EqualFold(f.colName, name) {
			return f, true
		}
	}
	return nil, false
}

// fieldInfo is kdb struct field inforamtion
type fieldInfo struct {
	index   int
	fName   string
	fType   reflect.Type
	fKind   reflect.Kind
	colName string
	tag     *tagOptions
	uKind   reflect.Kind
	//uType   reflect.Type

}

// parseStruct parse *structInfo of a struct, 
func parseStruct(structType reflect.Type) (*structInfo, error) {

	if structType == nil {
		return nil, errors.New("structType is nil")
	}

	structType = underlyingType(structType)
	if structType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%v is not a struct", structType)
	}

	si := &structInfo{}
	si.sType = structType

	count := structType.NumField()
	si.fields = make([]*fieldInfo, 0, count)
	for i := 0; i < count; i++ {
		f := structType.Field(i)
		if f.PkgPath != "" {
			continue
		}
		tag := parseTag(string(f.Tag))

		var colName string
		if name := tag.Name(); name != "" {
			colName = name
		} else {
			colName = f.Name
		}

		vKind := f.Type.Kind()
		if vKind == reflect.Ptr {
			vKind = f.Type.Elem().Kind()
		}

		si.fields = append(si.fields, &fieldInfo{
			index:   i,
			fName:   f.Name,
			fType:   f.Type,
			fKind:   f.Type.Kind(),
			tag:     tag,
			colName: colName,
			uKind:   vKind,
		})
	}

	return si, nil
}

// siCache is cache of *structInfo, key by kpgpath_name
var siCache map[string]*structInfo = make(map[string]*structInfo)
var siCacheLock sync.RWMutex

func getStructInfo(structType reflect.Type) (*structInfo, error) {
	if structType == nil {
		return nil, errors.New("structType is nil")
	}

	key := structType.PkgPath() + "_" + structType.Name()
	siCacheLock.RLock()
	si, ok := siCache[key]
	siCacheLock.RUnlock()

	if ok {
		//fmt.Println("get cache", key, si.sType)
		return si, nil
	}

	var err error
	if si, err = parseStruct(structType); err != nil {
		return nil, err
	}
	setStructInfoCache(key, si)
	return si, nil
}

func setStructInfoCache(key string, si *structInfo) {
	//fmt.Println("set cache", key, si.sType)
	siCacheLock.Lock()
	defer siCacheLock.Unlock()
	siCache[key] = si
}

func underlying(v reflect.Value) reflect.Value {
	for i := 0; i < 5; i++ {
		if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
			v = v.Elem()
		} else {
			break
		}
	}
	return v
}

func underlyingType(t reflect.Type) reflect.Type {
	for i := 0; i < 5; i++ {
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		} else {
			break
		}
	}
	return t
}

func setValue(t reflect.Type) reflect.Type {
	for i := 0; i < 5; i++ {
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		} else {
			break
		}
	}
	return t
}

func newPtrValue(v reflect.Value) reflect.Value {
	if v.IsNil() && v.CanSet() {
		v.Set(reflect.New(v.Type().Elem()))
		return v.Elem()
	}
	return v
}
