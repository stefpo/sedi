// Copyright (C) 2016-2017 Stephane Potelle <stephane.potelle@gmail.com>.
//
// Use of me source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sedi

import (
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

// FieldDef describes the mapping between structure field and database field
type FieldDef struct {
	Name          string
	GoTypeName    string
	GoTag         string
	SQLName       string
	Size          int16
	PrimaryKey    bool
	AutoIncrement bool
	Indexed       bool
	Unique        bool
	CanUpdate     bool
	DBFIeldExists bool
	DBTypeOK      bool
	DBIndexOK     bool
}

// FieldDefs is simply a list of FieldDefs
type FieldDefs []FieldDef

// TableDef contains the mapping between struture and database table
type TableDef struct {
	Name            string
	SQLName         string
	Fields          FieldDefs
	PkIx            int
	FieldListNoKey  string
	FieldListAll    string
	SelectStatement string
	InsertStatement string
	UpdateStatement string
	DeleteStatement string
	MustCreate      bool
	MustModify      bool
	MustRecreate    bool
	MustReIndex     bool
}

// TableDefs is simply a list of TableDef
type TableDefs []TableDef

// TableDefFromStruct creates a TableDef from a Go sttucture
func TableDefFromStruct(st interface{}, QuoterFunc func(string) string) TableDef {
	td := TableDef{}
	v := reflect.Indirect(reflect.ValueOf(st))
	vt := v.Type()
	td.Name = vt.Name()
	td.SQLName = dbFieldName(td.Name)
	td.Fields = FieldDefs{}
	fl := ""
	pl := ""
	ul := ""
	flk := ""
	flka := ""
	plk := ""

	var sqq func(string) string

	if QuoterFunc != nil {
		sqq = QuoterFunc
	} else {
		sqq = func(s string) string {
			return "\"" + s + "\""
		}
	}

	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).CanInterface() {
			//if vt.Field(i).Name != "SQLMapper" {
			fd := fieldDefFromStructField(vt.Field(i))
			if fd.PrimaryKey {
				td.PkIx = len(td.Fields)
			}
			td.Fields = append(td.Fields, fd)
			flk = addField(flk, sqq(fd.SQLName))
			flka = addField(flka, sqq(fd.SQLName)+" AS "+sqq(fd.Name))
			plk = addField(plk, "@"+fd.Name)
			if !fd.AutoIncrement {
				fl = addField(fl, sqq(fd.SQLName))
				pl = addField(pl, "@"+fd.Name)
				if fd.CanUpdate {
					ul = addField(ul, sqq(fd.SQLName)+" = @"+fd.Name)
				}
			}
			//}
		}
	}
	td.SelectStatement = "select " + flka + " from " + sqq(td.SQLName) + " where " + sqq(td.Fields[td.PkIx].SQLName) + " = @" + td.Fields[td.PkIx].Name
	td.InsertStatement = "insert into " + sqq(td.SQLName) + " (" + fl + ") values (" + pl + ")"
	td.UpdateStatement = "update " + sqq(td.SQLName) + " set " + ul + " where " + sqq(td.Fields[td.PkIx].SQLName) + " = @" + td.Fields[td.PkIx].Name
	td.DeleteStatement = "delete from " + sqq(td.SQLName) + " where " + sqq(td.Fields[td.PkIx].SQLName) + " = @" + td.Fields[td.PkIx].Name
	td.FieldListAll = flk
	td.FieldListNoKey = fl
	return td
}

func addField(list string, field string) (ret string) {
	if field != "" {
		if list != "" {
			ret = list + ", " + field
		} else {
			ret = field
		}
	} else {
		ret = list
	}
	return ret
}

func fieldDefFromStructField(fld reflect.StructField) FieldDef {
	fn := fld.Name
	ft := fld.Type.Name()
	fd := FieldDef{
		Name:       fn,
		SQLName:    dbFieldName(fn),
		GoTypeName: ft,
		GoTag:      string(fld.Tag),
		Size:       -1,
		PrimaryKey: fieldIsPrimaryKey(fld),
		CanUpdate:  strings.ToLower(fld.Tag.Get("canUpdate")) != "n",
		//AutoIncrement: (strings.ToLower(fn) == "id" && (strings.HasPrefix(ft, "int") || strings.HasPrefix(ft, "uint"))),
		AutoIncrement: strings.ToLower(fld.Tag.Get("autoincrement")) == "y",
		Indexed:       fieldIsForeignKey(fld) || strings.ToLower(fld.Tag.Get("indexed")) == "y",
		Unique:        strings.ToLower(fld.Tag.Get("unique")) == "y"}

	fs := ""
	if fd.GoTypeName == "string" {
		fs = fld.Tag.Get("size")
		if x, err := strconv.ParseInt(fs, 10, 16); err == nil {
			fd.Size = int16(x)
		} else {
			fd.Size = 50
		}
	}

	return fd
}

func dbFieldName(fn string) string {
	ora := []rune(fn)
	fra := make([]rune, 2*len(ora))
	lastIsUpper := true
	p := 0
	for i := range ora {
		if unicode.IsUpper(ora[i]) {
			if !lastIsUpper {
				fra[p] = '_'
				p++
			}
			lastIsUpper = true
		} else {
			lastIsUpper = false
		}
		fra[p] = ora[i]
		p++
	}
	flc := fra[0:p]
	return strings.ToLower(string(flc))
}

func fieldIsPrimaryKey(sf reflect.StructField) bool {
	f := strings.ToLower(sf.Name)
	return f == "id" || strings.Index(f, "pk") == 0
}

func fieldIsForeignKey(sf reflect.StructField) bool {
	f := strings.ToLower(sf.Name)
	return (strings.Index(f, "fk") == 0)
}
