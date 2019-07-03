// Copyright (C) 2016 Stephane Potelle <stephane.potelle@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sedi

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/stefpo/sedi/conv"
)

type DataTable struct {
	Columns     []string  // Column names
	Rows        []DataRow // Collection of DataRow
	colmap      map[string]int
	hasIDColumn bool
	tableName   string
}

type DataRow struct {
	items []interface{}
	dt    *DataTable
}

// Fill as structure from a data row
func (this *DataRow) FillStruct(s interface{}) {
	v := reflect.Indirect(reflect.ValueOf(s))
	vt := v.Type()
	for i := 0; i < v.NumField(); i++ {
		fn := vt.Field(i).Name
		if f, ok := this.Item(fn); ok && v.Field(i).CanSet() {
			switch v.Field(i).Interface().(type) {
			case string:
				v.Field(i).SetString(conv.ToString(f))
				break
			case time.Time:
				v.Field(i).Set(reflect.ValueOf(conv.ToTime(f)))
				break
			case uint8, uint16, uint32, uint64, uint:
				v.Field(i).SetUint(conv.ToUint64(f))
				break
			case int8, int16, int32, int64, int:
				v.Field(i).SetInt(conv.ToInt64(f))
				break
			case float32, float64:
				v.Field(i).SetFloat(conv.ToFloat64(f))
				break
			case bool:
				v.Field(i).SetBool(conv.ToBool(f))
				break
			}
		}
	}
}

// Clear empties the DataTable including column definition
func (dt *DataTable) Clear() {
	dt.Columns = []string{}
	dt.Rows = []DataRow{}
	dt.colmap = nil
	dt.hasIDColumn = false
	dt.tableName = ""
}

// NewRow return a new DataRow from DataTable structure (Does not add the row to the DataTable)
func (dt *DataTable) NewRow() DataRow {
	var dr DataRow
	dr.items = make([]interface{}, len(dt.Columns))
	dr.dt = dt
	return dr
}

func (dt *DataTable) refreshColmap() {
	dt.colmap = make(map[string]int)
	for i := range dt.Columns {
		dt.colmap[dt.Columns[i]] = i
	}
}

// AddRow appends a DataRow to a DataTable
func (dt *DataTable) AddRow(dr DataRow) {
	if dt.Rows == nil {
		dt.Rows = []DataRow{}
	}
	dt.Rows = append(dt.Rows, dr)
}

// Str return tab delimited string representing table contents
func (dt *DataTable) Str() string {
	var s string = ""
	first := true
	for c := range dt.Columns {
		if !first {
			s = s + "\t"
		}
		s = s + dt.Columns[c]
		first = false
	}

	for r := range dt.Rows {
		first = true
		rs := ""
		for c := range dt.Columns {
			if !first {
				rs = rs + "\t"
			}
			po := fmt.Sprint(dt.Rows[r].items[c])
			rs = rs + po
			first = false
		}
		s = s + "\n" + rs
	}
	return s
}

func (dt *DataTable) Fill(rows *sql.Rows, maxrows int) {
	rowid := 0
	dt.Clear()
	dt.Columns, _ = rows.Columns()
	for c := range dt.Columns {
		if strings.ToLower(dt.Columns[c]) == "id" {
			dt.hasIDColumn = true
		}
	}

	dt.tableName = "mytable"

	valuePtrs := make([]interface{}, len(dt.Columns))

	for rows.Next() && (maxrows < 0 || rowid < maxrows) {
		dr := dt.NewRow()

		for i, _ := range dt.Columns {
			valuePtrs[i] = &(dr.items[i])

		}
		rows.Scan(valuePtrs...)

		for i, _ := range dt.Columns {
			switch dr.items[i].(type) {
			case []byte:
				dr.items[i] = string(dr.items[i].([]byte))
				break
			}
		}
		dt.AddRow(dr)
		rowid++
	}
}

func (dr *DataRow) Items() []interface{} {
	return dr.items
}

// Access DataRow field by name
func (dr *DataRow) Item(name string) (interface{}, bool) {
	if dr.dt.colmap == nil {
		dr.dt.refreshColmap()
	}
	if ix, found := dr.dt.colmap[name]; found {
		return dr.items[ix], true
	} else {
		return nil, false
	}
}

func (dr *DataRow) ItemSingle(name string) interface{} {
	i, _ := dr.Item(name)
	return i
}

func (dr *DataRow) ToMap() map[string]interface{} {
	ret := make(map[string]interface{})
	if dr.dt.colmap == nil {
		dr.dt.refreshColmap()
	}
	for i := range dr.items {
		ret[dr.dt.Columns[i]] = dr.items[i]
	}
	return ret
}

func escapeJS(s string) string {
	return strings.Replace(s, "\"", "\\\"", -1)
}

type Writer interface {
	WriteString(string) (int, error)
}

// ToJSON converts datatable to a JSON string
func (dt *DataTable) ToJSON() (string, error) {
	var b bytes.Buffer
	e := dt.WriteAsJSON(&b)
	return b.String(), e
}

// SaveToFile saves datatable to a JSON file
func (dt *DataTable) SaveToFile(filename string) error {
	f, e := os.Create(filename)
	defer f.Close()
	dt.WriteAsJSON(f)
	return e
}

// WriteAsJSON writes the datatable to a writer.
func (dt *DataTable) WriteAsJSON(dest Writer) error {
	dest.WriteString("{")
	dest.WriteString("\n\"Columns\": ")
	x, e := json.Marshal(dt.Columns)
	if e != nil {
		return e
	}
	dest.WriteString(string(x))
	dest.WriteString(",")

	dest.WriteString("\n\"Rows\": [\n")
	for r := range dt.Rows {
		x, e := json.Marshal(dt.Rows[r].items)
		if e != nil {
			return e
		}
		dest.WriteString("    ")
		dest.WriteString(string(x))
		if r < len(dt.Rows)-1 {
			dest.WriteString(",\n")
		}
	}
	dest.WriteString("]\n")
	dest.WriteString("}\n")
	return nil
}

// FillFromJSON fills a data table from a JSON string
func (dt *DataTable) FillFromJSON(js string) error {
	dt.Clear()
	obj := make(map[string]interface{})
	if e := json.Unmarshal([]byte(js), &obj); e == nil {
		dt.Columns = []string{}
		for c := range obj["Columns"].([]interface{}) {
			jsc := obj["Columns"].([]interface{})
			dt.Columns = append(dt.Columns, jsc[c].(string))
		}
		rows := obj["Rows"].([]interface{})
		for i := range rows {
			row := rows[i].([]interface{})
			dr := dt.NewRow()
			for c := range dt.Columns {
				dr.items[c] = row[c]
			}
			dt.AddRow(dr)
		}

		return nil
	} else {
		fmt.Println(e)
		return e
	}
}
