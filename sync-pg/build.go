package pg

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func BuildToSaveWithArray(table string, model interface{}, driver string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}, error) {
	buildParam := BuildDollarParam
	return BuildToSaveWithSchema(table, model, driver, buildParam, toArray, options...)
}
func BuildToSaveWithSchema(table string, model interface{}, driver string, buildParam func(i int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (string, []interface{}, error) {
	// driver := GetDriver(db)
	if buildParam == nil {
		buildParam = BuildDollarParam
	}
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	mv := reflect.ValueOf(model)
	if mv.Kind() == reflect.Ptr {
		mv = mv.Elem()
	}
	var cols, keys []FieldDB
	// var schema map[string]FieldDB
	if len(options) > 0 && options[0] != nil {
		m := options[0]
		cols = m.Columns
		keys = m.Keys
		// schema = m.Fields
	} else {
		// cols, keys, schema = MakeSchema(modelType)
		m := CreateSchema(modelType)
		cols = m.Columns
		keys = m.Keys
		// schema = m.Fields
	}
	iCols := make([]string, 0)
	values := make([]string, 0)
	setColumns := make([]string, 0)
	args := make([]interface{}, 0)
	boolSupport := true
	i := 1
	for _, fdb := range cols {
		f := mv.Field(fdb.Index)
		fieldValue := f.Interface()
		isNil := false
		if f.Kind() == reflect.Ptr {
			if reflect.ValueOf(fieldValue).IsNil() {
				isNil = true
			} else {
				fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
			}
		}
		if !isNil {
			iCols = append(iCols, fdb.Column)
			v, ok := GetDBValue(fieldValue, boolSupport)
			if ok {
				values = append(values, v)
			} else {
				if boolValue, ok := fieldValue.(bool); ok {
					if boolValue {
						if fdb.True != nil {
							values = append(values, buildParam(i))
							i = i + 1
							args = append(args, *fdb.True)
						} else {
							values = append(values, "'1'")
						}
					} else {
						if fdb.False != nil {
							values = append(values, buildParam(i))
							i = i + 1
							args = append(args, *fdb.False)
						} else {
							values = append(values, "'0'")
						}
					}
				} else {
					values = append(values, buildParam(i))
					i = i + 1
					if toArray != nil && reflect.TypeOf(fieldValue).Kind() == reflect.Slice {
						args = append(args, toArray(fieldValue))
					} else {
						args = append(args, fieldValue)
					}
				}
			}
		}
	}
	for _, fdb := range cols {
		if !fdb.Key && fdb.Update {
			f := mv.Field(fdb.Index)
			fieldValue := f.Interface()
			isNil := false
			if f.Kind() == reflect.Ptr {
				if reflect.ValueOf(fieldValue).IsNil() {
					isNil = true
				} else {
					fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
				}
			}
			if isNil {
				setColumns = append(setColumns, fdb.Column+"=null")
			} else {
				v, ok := GetDBValue(fieldValue, boolSupport)
				if ok {
					setColumns = append(setColumns, fdb.Column+"="+v)
				} else {
					if boolValue, ok := fieldValue.(bool); ok {
						if boolValue {
							if fdb.True != nil {
								setColumns = append(setColumns, fdb.Column+"="+buildParam(i))
								i = i + 1
								args = append(args, *fdb.True)
							} else {
								values = append(values, "'1'")
							}
						} else {
							if fdb.False != nil {
								setColumns = append(setColumns, fdb.Column+"="+buildParam(i))
								i = i + 1
								args = append(args, *fdb.False)
							} else {
								values = append(values, "'0'")
							}
						}
					} else {
						setColumns = append(setColumns, fdb.Column+"="+buildParam(i))
						i = i + 1
						if toArray != nil && reflect.TypeOf(fieldValue).Kind() == reflect.Slice {
							args = append(args, toArray(fieldValue))
						} else {
							args = append(args, fieldValue)
						}
					}
				}
			}
		}
	}
	var query string
	iKeys := make([]string, 0)
	for _, fdb := range keys {
		iKeys = append(iKeys, fdb.Column)
	}
	if len(setColumns) > 0 {
		query = fmt.Sprintf("insert into %s(%s) values (%s) on conflict (%s) do update set %s",
			table,
			strings.Join(iCols, ","),
			strings.Join(values, ","),
			strings.Join(iKeys, ","),
			strings.Join(setColumns, ","),
		)
	} else {
		query = fmt.Sprintf("insert into %s(%s) values (%s) on conflict (%s) do nothing",
			table,
			strings.Join(iCols, ","),
			strings.Join(values, ","),
			strings.Join(iKeys, ","),
		)
	}
	return query, args, nil
}
func BuildToSaveBatchWithArray(table string, models interface{}, drive string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models must be a slice")
	}
	slen := s.Len()
	if slen <= 0 {
		return nil, nil
	}
	buildParam := BuildDollarParam
	var strt *Schema
	if len(options) > 0 {
		strt = options[0]
	} else {
		first := s.Index(0).Interface()
		modelType := reflect.TypeOf(first)
		strt = CreateSchema(modelType)
	}
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args, err := BuildToSaveWithSchema(table, model, drive, buildParam, toArray, strt)
		if err != nil {
			return stmts, err
		}
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func GetDBValue(v interface{}, boolSupport bool) (string, bool) {
	switch v.(type) {
	case string:
		s0 := v.(string)
		if len(s0) == 0 {
			return "''", true
		}
		return "", false
	case int:
		return strconv.Itoa(v.(int)), true
	case int64:
		return strconv.FormatInt(v.(int64), 10), true
	case int32:
		return strconv.FormatInt(int64(v.(int32)), 10), true
	case bool:
		if !boolSupport {
			return "", false
		}
		b0 := v.(bool)
		if b0 {
			return "true", true
		} else {
			return "false", true
		}
	default:
		return "", false
	}
}
func BuildDollarParam(i int) string {
	return "$" + strconv.Itoa(i)
}
