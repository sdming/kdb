package kdb

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

// Read iterate rows and scan value to dest. dest can be *[]T, *[]map, *[]sliece, *[]struct.
func Read(rows *sql.Rows, dest interface{}) error {
	if dest == nil {
		return errors.New("dest is nil")
	}

	dvptr := reflect.ValueOf(dest)
	dk := dvptr.Kind()
	if dk != reflect.Ptr {
		return fmt.Errorf("Read does not support dest type %v", dk)
	}

	//dv := reflect.Indirect(dvptr)
	dv := underlying(dvptr)
	dk = dv.Kind()
	if dk != reflect.Slice {
		return fmt.Errorf("Read does not support type %v", dk)
	}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	et := dv.Type().Elem()
	ek := et.Kind()

	switch ek {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		if len(cols) != 1 {
			return fmt.Errorf("Elem kind is %v, but rows has %v columns", ek, len(cols))
		}

		switch d := dest.(type) {
		case *[]bool:
			for rows.Next() {
				var b bool
				if err = ReadRow(rows, &b); err != nil {
					return err
				}
				*d = append(*d, b)
			}
		case *[]string:
			for rows.Next() {
				var s string
				if err = ReadRow(rows, &s); err != nil {
					return err
				}
				*d = append(*d, s)
			}
		case *[]int:
			for rows.Next() {
				var i int
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]int8:
			for rows.Next() {
				var i int8
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]int16:
			for rows.Next() {
				var i int16
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]int32:
			for rows.Next() {
				var i int32
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]int64:
			for rows.Next() {
				var i int64
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]uint:
			for rows.Next() {
				var i uint
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]uint8:
			for rows.Next() {
				var i uint8
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]uint16:
			for rows.Next() {
				var i uint16
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]uint32:
			for rows.Next() {
				var i uint32
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]uint64:
			for rows.Next() {
				var i uint64
				if err = ReadRow(rows, &i); err != nil {
					return err
				}
				*d = append(*d, i)
			}
		case *[]float32:
			for rows.Next() {
				var f float32
				if err = ReadRow(rows, &f); err != nil {
					return err
				}
				*d = append(*d, f)
			}
		case *[]float64:
			for rows.Next() {
				var f float64
				if err = ReadRow(rows, &f); err != nil {
					return err
				}
				*d = append(*d, f)
			}
		}

		return nil
	case reflect.Map:
		for rows.Next() {
			m := reflect.MakeMap(et)
			if err = ReadRow(rows, m.Interface()); err != nil {
				return err
			}
			dv.Set(reflect.Append(dv, m))
		}
		return nil
	case reflect.Slice:
		for rows.Next() {
			m := reflect.MakeSlice(et, len(cols), len(cols))
			if err = ReadRow(rows, m.Interface()); err != nil {
				return err
			}
			dv.Set(reflect.Append(dv, m))
		}
		return nil
	case reflect.Struct:
		var si *structInfo
		si, err = getStructInfo(et)
		if err != nil {
			return err
		}
		fields := colsFields(cols, si)

		for rows.Next() {
			v := reflect.New(et)
			if err = setStructValue(rows, v, fields); err != nil {
				return err
			}
			dv.Set(reflect.Append(dv, v.Elem()))
		}
		return nil

	case reflect.Ptr:
		elem := et.Elem()
		if elem.Kind() != reflect.Struct {
			return fmt.Errorf("Read does not support elem type %v", et)
		}

		var si *structInfo
		si, err = getStructInfo(elem)
		if err != nil {
			return err
		}
		fields := colsFields(cols, si)

		for rows.Next() {
			v := reflect.New(elem)
			if err = setStructValue(rows, v, fields); err != nil {
				return err
			}
			dv.Set(reflect.Append(dv, v))
		}
		return nil

	default:
		return fmt.Errorf("Read does not support elem type %v", et)
	}

	return fmt.Errorf("Read does not support dest %v", dest)
}

// readInt64 copy value from rows to dest.
func readInt64(rows *sql.Rows, dest interface{}) (err error) {
	var v sql.NullInt64
	if err = rows.Scan(&v); err != nil {
		return
	}

	if v.Valid {
		switch d := dest.(type) {
		case *int:
			*d = int(v.Int64)
		case *int8:
			*d = int8(v.Int64)
		case *int16:
			*d = int16(v.Int64)
		case *int32:
			*d = int32(v.Int64)
		case *int64:
			*d = int64(v.Int64)
		case *uint:
			*d = uint(v.Int64)
		case *uint8:
			*d = uint8(v.Int64)
		case *uint16:
			*d = uint16(v.Int64)
		case *uint32:
			*d = uint32(v.Int64)
		case *uint64:
			*d = uint64(v.Int64)
		default:
			err = fmt.Errorf("% is not int", dest)
		}
	}

	return
}

// readString copy value from rows to dest.
func readString(rows *sql.Rows, dest *string) (err error) {
	var v sql.NullString
	if err = rows.Scan(&v); err != nil {
		return
	}

	if v.Valid {
		*dest = v.String
	}
	return
}

// readFloat64 copy value from rows to dest.
func readFloat64(rows *sql.Rows, dest interface{}) (err error) {
	var v sql.NullFloat64
	if err = rows.Scan(&v); err != nil {
		return
	}

	if v.Valid {
		switch d := dest.(type) {
		case *float32:
			*d = float32(v.Float64)
		case *float64:
			*d = float64(v.Float64)
		default:
			err = fmt.Errorf("% is not float", dest)
		}
	}

	return
}

// readBool copy value from rows to dest.
func readBool(rows *sql.Rows, dest *bool) (err error) {
	var v sql.NullBool
	if err = rows.Scan(&v); err != nil {
		return
	}

	if v.Valid {
		*dest = v.Bool
	}
	return
}

// readSlice copy value from rows to dest. dest must be a slice.
func readSlice(rows *sql.Rows, dest interface{}) (err error) {
	var cols []string
	if cols, err = rows.Columns(); err != nil {
		return
	}

	l := len(cols)
	v := make([]interface{}, l, l)

	switch d := dest.(type) {
	case []string, []*string:
		for i := 0; i < l; i++ {
			v[i] = &sql.NullString{}
		}
	case []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64, []*int, []*int8, []*int16, []*int32, []*int64, []*uint, []*uint8, []*uint16, []*uint32, []*uint64:
		for i := 0; i < l; i++ {
			v[i] = &sql.NullInt64{}
		}
	case []float32, []float64, []*float32, []*float64:
		for i := 0; i < l; i++ {
			v[i] = &sql.NullFloat64{}
		}
	case []bool, []*bool:
		for i := 0; i < l; i++ {
			v[i] = &sql.NullBool{}
		}
	case []interface{}:
		for i := 0; i < l; i++ {
			switch d[i].(type) {
			case string, *string:
				v[i] = &sql.NullString{}
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
				v[i] = &sql.NullInt64{}
			case float32, float64, *float32, *float64:
				v[i] = &sql.NullFloat64{}
			case bool, *bool:
				v[i] = &sql.NullBool{}
			default:
				var tv interface{}
				v[i] = &tv
			}
		}
	default:
		return fmt.Errorf("% does not support scanSlice", dest)
	}

	if err = rows.Scan(v...); err != nil {
		return
	}

	switch d := dest.(type) {
	case []string:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullString); x.Valid {
				d[i] = x.String
			}
		}
	case []*string:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullString); x.Valid {
				*(d[i]) = x.String
			}
		}
	case []int:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = int(x.Int64)
			}
		}
	case []*int:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*(d[i]) = int(x.Int64)
			}
		}
	case []int8:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = int8(x.Int64)
			}
		}
	case []*int8:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*(d[i]) = int8(x.Int64)
			}
		}
	case []int16:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = int16(x.Int64)
			}
		}
	case []*int16:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[i] = int16(x.Int64)
			}
		}
	case []int32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = int32(x.Int64)
			}
		}
	case []*int32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[i] = int32(x.Int64)
			}
		}
	case []int64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = int64(x.Int64)
			}
		}
	case []*int64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[i] = int64(x.Int64)
			}
		}
	case []uint:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = uint(x.Int64)
			}
		}
	case []*uint:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[i] = uint(x.Int64)
			}
		}
	case []uint8:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = uint8(x.Int64)
			}
		}
	case []*uint8:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[i] = uint8(x.Int64)
			}
		}
	case []uint16:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = uint16(x.Int64)
			}
		}
	case []*uint16:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[i] = uint16(x.Int64)
			}
		}
	case []uint32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = uint32(x.Int64)
			}
		}
	case []*uint32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[i] = uint32(x.Int64)
			}
		}
	case []uint64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[i] = uint64(x.Int64)
			}
		}
	case []*uint64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[i] = uint64(x.Int64)
			}
		}
	case []float32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullFloat64); x.Valid {
				d[i] = float32(x.Float64)
			}
		}
	case []*float32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullFloat64); x.Valid {
				*d[i] = float32(x.Float64)
			}
		}
	case []float64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullFloat64); x.Valid {
				d[i] = x.Float64
			}
		}
	case []*float64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullFloat64); x.Valid {
				*d[i] = x.Float64
			}
		}
	case []bool:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullBool); x.Valid {
				d[i] = x.Bool
			}
		}
	case []*bool:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullBool); x.Valid {
				*d[i] = x.Bool
			}
		}
	case []interface{}:
		for i := 0; i < l; i++ {
			switch di := d[i].(type) {
			case string:
				if x, _ := v[i].(*sql.NullString); x.Valid {
					d[i] = x.String
				}
			case *string:
				if x, _ := v[i].(*sql.NullString); x.Valid {
					*di = x.String
				}
			case int:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = int(x.Int64)
				}
			case *int:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = int(x.Int64)
				}
			case int8:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = int8(x.Int64)
				}
			case *int8:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = int8(x.Int64)
				}
			case int16:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = int16(x.Int64)
				}
			case *int16:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = int16(x.Int64)
				}
			case int32:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = int32(x.Int64)
				}
			case *int32:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = int32(x.Int64)
				}
			case int64:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = int64(x.Int64)
				}
			case *int64:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = int64(x.Int64)
				}
			case uint:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = uint(x.Int64)
				}
			case *uint:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = uint(x.Int64)
				}
			case uint8:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = uint8(x.Int64)
				}
			case *uint8:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = uint8(x.Int64)
				}
			case uint16:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = uint16(x.Int64)
				}
			case *uint16:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = uint16(x.Int64)
				}
			case uint32:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = uint32(x.Int64)
				}
			case *uint32:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = uint32(x.Int64)
				}
			case uint64:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					d[i] = uint64(x.Int64)
				}
			case *uint64:
				if x, _ := v[i].(*sql.NullInt64); x.Valid {
					*di = uint64(x.Int64)
				}
			case float32:
				if x, _ := v[i].(*sql.NullFloat64); x.Valid {
					d[i] = float32(x.Float64)
				}
			case *float32:
				if x, _ := v[i].(*sql.NullFloat64); x.Valid {
					*di = float32(x.Float64)
				}
			case float64:
				if x, _ := v[i].(*sql.NullFloat64); x.Valid {
					d[i] = float64(x.Float64)
				}
			case *float64:
				if x, _ := v[i].(*sql.NullFloat64); x.Valid {
					*di = float64(x.Float64)
				}
			case bool:
				if x, _ := v[i].(*sql.NullBool); x.Valid {
					d[i] = x.Bool
				}
			case *bool:
				if x, _ := v[i].(*sql.NullBool); x.Valid {
					*di = x.Bool
				}
			default:
				d[i] = inDirect(v[i])
			}
		}
	}

	return
}

// readMap copy value from rows to dest. dest must be a map[string]T.
func readMap(rows *sql.Rows, dest interface{}) (err error) {

	var cols []string
	if cols, err = rows.Columns(); err != nil {
		return
	}

	l := len(cols)
	v := make([]interface{}, l, l)

	switch d := dest.(type) {
	case map[string]string, map[string]*string:
		for i := 0; i < l; i++ {
			v[i] = &sql.NullString{}
		}
	case map[string]int, map[string]int8, map[string]int16, map[string]int32, map[string]int64, map[string]uint, map[string]uint8, map[string]uint16, map[string]uint32, map[string]uint64,
		map[string]*int, map[string]*int8, map[string]*int16, map[string]*int32, map[string]*int64, map[string]*uint, map[string]*uint8, map[string]*uint16, map[string]*uint32, map[string]*uint64:
		for i := 0; i < l; i++ {
			v[i] = &sql.NullInt64{}
		}
	case map[string]float32, map[string]float64, map[string]*float32, map[string]*float64:
		for i := 0; i < l; i++ {
			v[i] = &sql.NullFloat64{}
		}
	case map[string]bool, map[string]*bool:
		for i := 0; i < l; i++ {
			v[i] = &sql.NullBool{}
		}
	case map[string]interface{}:
		if len(d) > 0 {
			for i := 0; i < l; i++ {
				col := cols[i]
				if mv, ok := d[col]; ok {
					switch mv.(type) {
					case string, *string:
						v[i] = &sql.NullString{}
					case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
						v[i] = &sql.NullInt64{}
					case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
						v[i] = &sql.NullInt64{}
					case float32, float64, *float32, *float64:
						v[i] = &sql.NullFloat64{}
					case bool, *bool:
						v[i] = &sql.NullBool{}
					default:
						v[i] = mv
					}
				} else {
					var tv interface{}
					v[i] = &tv
				}
			}
		} else {
			for i := 0; i < l; i++ {
				var tv interface{}
				v[i] = &tv
			}
		}
	default:
		return fmt.Errorf("% does support readMap", dest)
	}

	if err = rows.Scan(v...); err != nil {
		return
	}

	switch d := dest.(type) {
	case map[string]string:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullString); x.Valid {
				d[cols[i]] = x.String
			}
		}
	case map[string]*string:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullString); x.Valid {
				*d[cols[i]] = x.String
			}
		}
	case map[string]int:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = int(x.Int64)
			}
		}
	case map[string]*int:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = int(x.Int64)
			}
		}
	case map[string]int8:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = int8(x.Int64)
			}
		}
	case map[string]*int8:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = int8(x.Int64)
			}
		}
	case map[string]int16:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = int16(x.Int64)
			}
		}
	case map[string]*int16:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = int16(x.Int64)
			}
		}
	case map[string]int32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = int32(x.Int64)
			}
		}
	case map[string]*int32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = int32(x.Int64)
			}
		}
	case map[string]int64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = int64(x.Int64)
			}
		}
	case map[string]*int64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = int64(x.Int64)
			}
		}
	case map[string]uint:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = uint(x.Int64)
			}
		}
	case map[string]*uint:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = uint(x.Int64)
			}
		}
	case map[string]uint8:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = uint8(x.Int64)
			}
		}
	case map[string]*uint8:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = uint8(x.Int64)
			}
		}
	case map[string]uint16:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = uint16(x.Int64)
			}
		}
	case map[string]*uint16:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = uint16(x.Int64)
			}
		}
	case map[string]uint32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = uint32(x.Int64)
			}
		}
	case map[string]*uint32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = uint32(x.Int64)
			}
		}
	case map[string]uint64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				d[cols[i]] = uint64(x.Int64)
			}
		}
	case map[string]*uint64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				*d[cols[i]] = uint64(x.Int64)
			}
		}
	case map[string]float32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullFloat64); x.Valid {
				d[cols[i]] = float32(x.Float64)
			}
		}
	case map[string]*float32:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullFloat64); x.Valid {
				*d[cols[i]] = float32(x.Float64)
			}
		}
	case map[string]float64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullFloat64); x.Valid {
				d[cols[i]] = x.Float64
			}
		}
	case map[string]*float64:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullFloat64); x.Valid {
				*d[cols[i]] = x.Float64
			}
		}
	case map[string]bool:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullBool); x.Valid {
				d[cols[i]] = x.Bool
			}
		}
	case map[string]*bool:
		for i := 0; i < l; i++ {
			if x, _ := v[i].(*sql.NullBool); x.Valid {
				*d[cols[i]] = x.Bool
			}
		}
	case map[string]interface{}:
		ml := len(d)
		for i := 0; i < l; i++ {
			col := cols[i]
			if ml > 0 {
				if mv, ok := d[col]; ok {
					switch di := mv.(type) {
					case string:
						if x, _ := v[i].(*sql.NullString); x.Valid {
							d[cols[i]] = x.String
						}
					case *string:
						if x, _ := v[i].(*sql.NullString); x.Valid {
							*di = x.String
						}
					case int:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = int(x.Int64)
						}
					case *int:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = int(x.Int64)
						}
					case int8:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = int8(x.Int64)
						}
					case *int8:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = int8(x.Int64)
						}
					case int16:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = int16(x.Int64)
						}
					case *int16:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = int16(x.Int64)
						}
					case int32:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = int32(x.Int64)
						}
					case *int32:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = int32(x.Int64)
						}
					case int64:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = int64(x.Int64)
						}
					case *int64:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = int64(x.Int64)
						}
					case uint:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = uint(x.Int64)
						}
					case *uint:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = uint(x.Int64)
						}
					case uint8:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = uint8(x.Int64)
						}
					case *uint8:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = uint8(x.Int64)
						}
					case uint16:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = uint16(x.Int64)
						}
					case *uint16:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = uint16(x.Int64)
						}
					case uint32:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = uint32(x.Int64)
						}
					case *uint32:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = uint32(x.Int64)
						}
					case uint64:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							d[cols[i]] = uint64(x.Int64)
						}
					case *uint64:
						if x, _ := v[i].(*sql.NullInt64); x.Valid {
							*di = uint64(x.Int64)
						}
					case float32:
						if x, _ := v[i].(*sql.NullFloat64); x.Valid {
							d[cols[i]] = float32(x.Float64)
						}
					case *float32:
						if x, _ := v[i].(*sql.NullFloat64); x.Valid {
							*di = float32(x.Float64)
						}
					case float64:
						if x, _ := v[i].(*sql.NullFloat64); x.Valid {
							d[cols[i]] = float64(x.Float64)
						}
					case *float64:
						if x, _ := v[i].(*sql.NullFloat64); x.Valid {
							*di = float64(x.Float64)
						}
					case bool:
						if x, _ := v[i].(*sql.NullBool); x.Valid {
							d[cols[i]] = x.Bool
						}
					case *bool:
						if x, _ := v[i].(*sql.NullBool); x.Valid {
							*di = x.Bool
						}
					default:
						d[cols[i]] = inDirect(v[i])
					}
				} else {
					d[cols[i]] = inDirect(v[i])
				}
			} else {
				d[cols[i]] = inDirect(v[i])
			}
		}
	}

	return
}

func inDirect(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch v := v.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return v
	case *string:
		return *v
	case *bool:
		return *v
	case *int:
		return *v
	case *int8:
		return *v
	case *int16:
		return *v
	case *int32:
		return *v
	case *int64:
		return *v
	case *uint:
		return *v
	case *uint8:
		return *v
	case *uint16:
		return *v
	case *uint32:
		return *v
	case *uint64:
		return *v
	case *float32:
		return *v
	case *float64:
		return *v
	case *sql.NullBool:
		if v.Valid {
			return v.Bool
		}
		return nil
	case *sql.NullInt64:
		if v.Valid {
			return v.Int64
		}
		return nil
	case *sql.NullFloat64:
		if v.Valid {
			return v.Float64
		}
		return nil
	case *sql.NullString:
		if v.Valid {
			return v.String
		}
		return nil
	}

	rv := reflect.Indirect(reflect.ValueOf(v)).Interface()
	switch x := rv.(type) {
	case []byte:
		return (string(x))
	default:
		return rv
	}
}

// readStruct copy value from rows to dest, dest should be potiner to a struct
func readStruct(rows *sql.Rows, dest interface{}) error {
	if dest == nil {
		return errors.New("dest is nil")
	}

	dv := reflect.ValueOf(dest)
	dv = underlying(dv)
	dt := dv.Type()
	si, err := getStructInfo(dt)
	if err != nil {
		return err
	}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	fields := colsFields(cols, si)
	return setStructValue(rows, dv, fields)
}

func setStructValue(rows *sql.Rows, dv reflect.Value, fields []*fieldInfo) error {
	dv = underlying(dv)

	l := len(fields)
	v := make([]interface{}, l)

	for i := 0; i < l; i++ {
		fi := fields[i]
		if fi == nil {
			var tv interface{}
			v[i] = &tv
			continue
		}

		switch fi.uKind {
		case reflect.Bool:
			v[i] = &sql.NullBool{}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v[i] = &sql.NullInt64{}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			v[i] = &sql.NullInt64{}
		case reflect.Float32, reflect.Float64:
			v[i] = &sql.NullFloat64{}
		case reflect.String:
			v[i] = &sql.NullString{}
		case reflect.Interface:
			var tv interface{}
			v[i] = &tv
		case reflect.Invalid, reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Struct, reflect.UnsafePointer:
			// ingore
			var tv interface{}
			v[i] = &tv
		case reflect.Slice:
			var tv interface{} = reflect.MakeSlice(fi.fType.Elem(), 0, 0).Interface()
			v[i] = &tv
		default:
			v[i] = reflect.New(fi.fType).Interface()
		}
	}

	if err := rows.Scan(v...); err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		fi := fields[i]
		if fi == nil {
			continue
		}
		fv := dv.Field(fi.index)

		switch fi.uKind {
		case reflect.Bool:
			if x, _ := v[i].(*sql.NullBool); x.Valid {
				if fv.Kind() == reflect.Ptr {
					fv = newPtrValue(fv)
				}
				if fv.CanSet() {
					fv.SetBool(x.Bool)
				}
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				if fv.Kind() == reflect.Ptr {
					fv = newPtrValue(fv)
				}
				if fv.CanSet() {
					fv.SetInt(x.Int64)
				}
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if x, _ := v[i].(*sql.NullInt64); x.Valid {
				if fv.Kind() == reflect.Ptr {
					fv = newPtrValue(fv)
				}
				if fv.CanSet() {
					fv.SetUint(uint64(x.Int64))
				}
			}
		case reflect.Float32, reflect.Float64:
			if x, _ := v[i].(*sql.NullFloat64); x.Valid {
				if fv.Kind() == reflect.Ptr {
					fv = newPtrValue(fv)
				}
				if fv.CanSet() {
					fv.SetFloat(x.Float64)
				}
			}
		case reflect.String:
			if x, _ := v[i].(*sql.NullString); x.Valid {
				if fv.Kind() == reflect.Ptr {
					fv = newPtrValue(fv)
				}
				if fv.CanSet() {
					fv.SetString(x.String)
				}
			}
		case reflect.Interface:
			if fv.CanSet() {
				fv.Set(reflect.ValueOf(v[i]))
			}
		case reflect.Invalid, reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Struct, reflect.UnsafePointer:
			// ingore			
		case reflect.Slice:
			if fv.CanSet() {
				fv.Set(reflect.ValueOf(v[i]))
			}
		default:
			if fv.CanSet() {
				fv.Set(reflect.ValueOf(v[i]))
			}
		}
	}

	return nil
}

// ReadRow scan current row value to dest. dest can be *T, []T, map[string]T
func ReadRow(rows *sql.Rows, dest interface{}) error {
	if rows == nil {
		return errors.New("rows is nil.")
	}
	if dest == nil {
		return errors.New("dest is nil.")
	}

	switch d := dest.(type) {
	case *string:
		return readString(rows, d)
	case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
		return readInt64(rows, d)
	case *float32, *float64:
		return readFloat64(rows, d)
	case *bool:
		return readBool(rows, d)
	case []interface{}, []string, []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64, []float32, []float64, []bool,
		[]*string, []*int, []*int8, []*int16, []*int32, []*int64, []*uint, []*uint8, []*uint16, []*uint32, []*uint64, []*float32, []*float64, []*bool:
		return readSlice(rows, d)
	case map[string]interface{}, map[string]string, map[string]int, map[string]int8, map[string]int16, map[string]int32, map[string]int64, map[string]uint, map[string]uint8, map[string]uint16, map[string]uint32, map[string]uint64, map[string]float32, map[string]float64, map[string]bool,
		map[string]*string, map[string]*int, map[string]*int8, map[string]*int16, map[string]*int32, map[string]*int64, map[string]*uint, map[string]*uint8, map[string]*uint16, map[string]*uint32, map[string]*uint64, map[string]*float32, map[string]*float64, map[string]*bool:
		return readMap(rows, d)
	}

	rv := reflect.ValueOf(dest)
	rv = underlying(rv)

	if rv.Kind() == reflect.Struct {
		return readStruct(rows, dest)
	}

	//struct

	return rows.Scan(dest)
}

func ReadRowScan(rows *sql.Rows, dest ...interface{}) error {
	return ReadRow(rows, dest)
}

func colsFields(cols []string, si *structInfo) []*fieldInfo {
	l := len(cols)
	fields := make([]*fieldInfo, l)
	for i := 0; i < l; i++ {
		f, ok := si.FieldByColName(cols[i])
		if ok {
			fields[i] = f
		} else {
			fields[i] = nil
		}
	}
	return fields
}
