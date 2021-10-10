package cassandra

import (
	"fmt"
	"reflect"
	"strings"
)

func BuildParam(i int) string {
	return "?"
}
func BuildToSave(table string, model interface{}, options ...*Schema) (string, []interface{}) {
	return BuildToInsertWithVersion(table, model, -1, true, options...)
}
func BuildToInsertWithVersion(table string, model interface{}, versionIndex int, orUpdate bool, options ...*Schema) (string, []interface{}) {
	buildParam := BuildParam
	modelType := reflect.TypeOf(model)
	var cols []FieldDB
	if len(options) > 0 && options[0] != nil {
		cols = options[0].Columns
	} else {
		m := CreateSchema(modelType)
		cols = m.Columns
	}
	mv := reflect.ValueOf(model)
	if mv.Kind() == reflect.Ptr {
		mv = mv.Elem()
	}
	values := make([]string, 0)
	args := make([]interface{}, 0)
	icols := make([]string, 0)
	i := 1
	for _, fdb := range cols {
		if fdb.Index == versionIndex {
			icols = append(icols, fdb.Column)
			values = append(values, "1")
		} else {
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
			if fdb.Insert {
				if isNil {
					if orUpdate {
						icols = append(icols, fdb.Column)
						values = append(values, "null")
					}
				} else {
					icols = append(icols, fdb.Column)
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						values = append(values, buildParam(i))
						i = i + 1
						args = append(args, fieldValue)
					}
				}
			}
		}
	}
	return fmt.Sprintf("insert into %v(%v) values (%v)", table, strings.Join(icols, ","), strings.Join(values, ",")), args
}
func BuildToInsertOrUpdateBatch(table string, models interface{}, orUpdate bool, options...*Schema) ([]Statement, error) {
	return BuildToInsertBatchWithVersion(table, models, -1, orUpdate, options...)
}
func BuildToInsertBatchWithVersion(table string, models interface{}, versionIndex int, orUpdate bool, options...*Schema) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() <= 0 {
		return nil, nil
	}
	var strt *Schema
	if len(options) > 0 {
		strt = options[0]
	} else {
		first := s.Index(0).Interface()
		modelType := reflect.TypeOf(first)
		strt = CreateSchema(modelType)
	}
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args := BuildToInsertWithVersion(table, model, versionIndex, orUpdate, strt)
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
