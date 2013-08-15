package kdb

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/sdming/kdb/ansi"
	"reflect"
	"strconv"
	"strings"
)

// CompileTemplate parse template, return formated template, parameter names
func CompileTemplate(template string) (string, []string, error) {
	b := []byte(template)
	buffer := &bytes.Buffer{}
	state := 0
	var args []string

	for {

		if state == 0 {
			index := bytes.IndexByte(b, '{')
			if index >= 0 {
				buffer.Write(b[:index])
				buffer.WriteByte('{')
				b = b[index+1:]
				state = 1
			} else {
				break
			}
		} else {
			index := bytes.IndexByte(b, '}')
			if index > 0 {
				if args == nil {
					args = make([]string, 0, 5)
				}
				name := string(bytes.TrimSpace((b[:index])))
				args = append(args, name)
				buffer.WriteString(name)
				buffer.WriteByte('}')
				b = b[index+1:]
				state = 0
			} else {
				return "", nil, errors.New("Invalid template format")
			}
		}
	}

	buffer.Write(b)
	return buffer.String(), args, nil
}

// SafeSql return a sql that without inject
func SafeSql(v string) string {
	panic("SafeSql")
}

// FormatSqlValue format v to native sql according dbType
func FormatSqlValue(dbType ansi.DbType, v interface{}) string {
	panic("FormatSqlValue")
}

func nativeType(p ansi.DbParameter) string {
	if p.DbType.IsBoolean() || p.DbType.IsInteger() || p.DbType.IsDateTime() {
		return p.NativeType
	}
	if p.DbType.HasPrecisionAndScale() {
		return fmt.Sprintf("%s(%d,%d", p.NativeType, p.Precision, p.Scale)
	}
	if p.DbType.HasLength() {
		if p.Size > 0 {
			return fmt.Sprintf("%s(%d)", p.NativeType, p.Size)
		} else {
			return fmt.Sprintf("%s(max)", p.NativeType)
		}
	}
	return p.NativeType
}

func scanScalar(rows *sql.Rows, v interface{}) (err error) {
	if rows == nil {
		return errors.New("rows is nil")
	}

	i := 0
	for rows.Next() {
		err = rows.Scan(v)
		i++
	}

	if err != nil {
		return
	}
	if err = rows.Err(); err != nil {
		return
	}
	if i == 0 {
		return ErrNoResult
	}

	return nil

}

func lastInsertIdErr(result sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	x, _ := lastInsertId(result)
	return x, nil
}

func lastInsertId(result sql.Result) (int64, error) {
	if result == nil {
		return -1, errors.New("result is nil")
	}
	if x, err := result.LastInsertId(); err != nil {
		return -1, err
	} else {
		return x, nil
	}
}

func rowsAffectedErr(result sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	x, _ := rowsAffected(result)
	return x, nil
}

func rowsAffected(result sql.Result) (int64, error) {
	if result == nil {
		return -1, errors.New("result is nil")
	}
	if x, err := result.RowsAffected(); err != nil {
		return -1, err
	} else {
		return x, nil
	}
}

// DumpResult dump sql.Result
func DumpResult(result sql.Result) string {
	buf := &bytes.Buffer{}

	if result == nil {
		buf.WriteString("result is <nil>")
		return buf.String()
	}

	buf.WriteString("LastInsertId:")
	if x, err := result.LastInsertId(); err != nil {
		buf.WriteString(err.Error())
	} else {
		buf.WriteString(fmt.Sprint(x))
	}
	buf.WriteString("; ")

	buf.WriteString("RowsAffected:")
	if x, err := result.RowsAffected(); err != nil {
		buf.WriteString(err.Error())
	} else {
		buf.WriteString(fmt.Sprint(x))
	}

	return buf.String()
}

// DumpRows dump *sql.Rows
func DumpRows(rows *sql.Rows) string {
	buf := &bytes.Buffer{}

	if rows == nil {
		buf.WriteString("rows is <nil>")
		return buf.String()
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		buf.WriteString(fmt.Sprintln("rows columns error:", err))
		return buf.String()
	}
	fmt.Println(columns)
	buf.WriteString(fmt.Sprintln("rows", columns))
	count := 0

	for rows.Next() {
		var values []interface{}
		for i := 0; i < len(columns); i++ {
			var v interface{}
			values = append(values, &v)
		}

		if err := rows.Scan(values...); err != nil {
			buf.WriteString(fmt.Sprintln("scan error:", count, err))
			return buf.String()
		}

		countStr := strconv.Itoa(count)
		buf.WriteString(fmt.Sprint(countStr, strings.Repeat(" ", 5-len(countStr)), "["))
		for i := 0; i < len(columns); i++ {
			if i > 0 {
				buf.WriteString(", ")
			}

			rv := reflect.Indirect(reflect.ValueOf(values[i])).Interface()
			if rv == nil {
				buf.WriteString("null")
			} else {
				switch x := rv.(type) {
				case []byte:
					buf.WriteString(string(x))
				default:
					buf.WriteString(fmt.Sprint(x))
				}
			}
		}

		buf.WriteString("]\n")
		count++
	}
	buf.WriteString(fmt.Sprint("rows count:", count))

	if err := rows.Err(); err != nil {
		buf.WriteString(fmt.Sprintln("rows.Err()", err))
	}
	return buf.String()
}

func asString(s interface{}) string {
	switch v := s.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	return fmt.Sprintf("%v", s)
}
