package pg

import (
	"reflect"
	"strings"
)

const (
	IgnoreReadWrite = "-"
	DriverPostgres  = "postgres"
)

type FieldDB struct {
	JSON   string
	Column string
	Field  string
	Index  int
	Key    bool
	Update bool
	Insert bool
	True   *string
	False  *string
}
type Schema struct {
	SKeys    []string
	SColumns []string
	Keys     []FieldDB
	Columns  []FieldDB
	Fields   map[string]FieldDB
}
type Statement struct {
	Query  string        `mapstructure:"query" json:"query,omitempty" gorm:"column:query" bson:"query,omitempty" dynamodbav:"query,omitempty" firestore:"query,omitempty"`
	Params []interface{} `mapstructure:"params" json:"params,omitempty" gorm:"column:params" bson:"params,omitempty" dynamodbav:"params,omitempty" firestore:"params,omitempty"`
}

func CreateSchema(modelType reflect.Type) *Schema {
	m := modelType
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}
	numField := m.NumField()
	scolumns := make([]string, 0)
	skeys := make([]string, 0)
	columns := make([]FieldDB, 0)
	keys := make([]FieldDB, 0)
	schema := make(map[string]FieldDB, 0)
	for idx := 0; idx < numField; idx++ {
		field := m.Field(idx)
		tag, _ := field.Tag.Lookup("gorm")
		if !strings.Contains(tag, IgnoreReadWrite) {
			update := !strings.Contains(tag, "update:false")
			insert := !strings.Contains(tag, "insert:false")
			if has := strings.Contains(tag, "column"); has {
				json := field.Name
				col := json
				str1 := strings.Split(tag, ";")
				num := len(str1)
				for i := 0; i < num; i++ {
					str2 := strings.Split(str1[i], ":")
					for j := 0; j < len(str2); j++ {
						if str2[j] == "column" {
							isKey := strings.Contains(tag, "primary_key")
							col = str2[j+1]
							scolumns = append(scolumns, col)
							jTag, jOk := field.Tag.Lookup("json")
							if jOk {
								tagJsons := strings.Split(jTag, ",")
								json = tagJsons[0]
							}
							f := FieldDB{
								JSON:   json,
								Column: col,
								Index:  idx,
								Key:    isKey,
								Update: update,
								Insert: insert,
							}
							if isKey {
								skeys = append(skeys, col)
								keys = append(keys, f)
							}
							columns = append(columns, f)
							tTag, tOk := field.Tag.Lookup("true")
							if tOk {
								f.True = &tTag
								fTag, fOk := field.Tag.Lookup("false")
								if fOk {
									f.False = &fTag
								}
							}
							schema[col] = f
						}
					}
				}
			}
		}
	}
	s := &Schema{SColumns: scolumns, SKeys: skeys, Columns: columns, Keys: keys, Fields: schema}
	return s
}
