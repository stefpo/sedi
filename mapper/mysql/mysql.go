// Copyright (C) 2016-2017 Stephane Potelle <stephane.potelle@gmail.com>.
//
// Use of me source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// TODO: This file is currently under work.
// It is NOT functional

package mysql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stefpo/sedi" // Make sure we load the driver !
	"github.com/stefpo/sedi/conv"
)

func GetSQLMapper() *SQLMapper {
	return &SQLMapper{}
}

// *SQLMapper provides persistence framework for structures
// in a MySQL database
type SQLMapper struct {
	conn *sedi.Conn
	sedi.TableDefs
	modelDiffDone bool
}

func (me *SQLMapper) Connection() *sedi.Conn {
	return me.conn
}

func quoteFieldName(s string) string {
	return "`" + s + "`"
}

func (me *SQLMapper) OpenConnection(url string) *SQLMapper {
	if cn, e := sedi.OpenConnection("mysql", url); e == nil {
		me.conn = &cn
	} else {
		me.conn = nil
	}
	return me
}

func (me *SQLMapper) CloseConnection() {
	me.conn.Close()
	me.conn = nil
}

func (me *SQLMapper) Add(st interface{}) *SQLMapper {
	if me.TableDefs == nil {
		me.TableDefs = make(sedi.TableDefs, 0)
	}
	td := sedi.TableDefFromStruct(st, quoteFieldName)
	me.TableDefs = append(me.TableDefs, td)
	return me
}

func (me *SQLMapper) SQLType(fd sedi.FieldDef) string {
	var ts string
	switch fd.GoTypeName {
	case "string":
		ts = "varchar(" + strconv.FormatInt(int64(fd.Size), 10) + ") charset utf8"
		break
	case "Time":
		ts = "datetime"
		break
	case "bool":
		ts = "tinyint unsigned"
		break
	case "uint8", "byte":
		ts = "tinyint unsigned"
		break
	case "uint16":
		ts = "smallint unsigned"
		break
	case "uint32":
		ts = "int unsigned"
		break
	case "uint64":
		ts = "bigint unsigned"
		break
	case "uint":
		ts = "unsigned bigint"
		break
	case "int8":
		ts = "tinyint"
		break
	case "int16":
		ts = "smallint"
		break
	case "int32":
		ts = "int"
		break
	case "int64":
		ts = "bigint"
		break
	case "int":
		ts = "bigint"
		break
	case "float32":
		ts = "float"
		break
	case "float64":
		ts = "double"
		break
	}

	return ts
}
func (me *SQLMapper) sameFieldType(dbField string, stField string) bool {
	df := strings.ToLower(dbField)
	sf := strings.ToLower(stField)
	ret := false
	dbparts := strings.Split(df, " ")
	stparts := strings.Split(sf, " ")

	if strings.HasPrefix(sf, "varchar") {
		if strings.HasPrefix(sf, df) {
			ret = true
		}
	} else if strings.HasPrefix(dbparts[0], stparts[0]) {
		p1 := strings.Index(df, "unsigned") >= 0
		p2 := strings.Index(sf, "unsigned") >= 0
		if p1 == p2 {
			ret = true
		}
	}
	//fmt.Println(dbField, stField, ret)
	return ret
}

func (me *SQLMapper) ModelIsUpToDate() (ok bool, err error) {
	ok = true
	err = nil
	for i := range me.TableDefs {
		td := &(me.TableDefs[i])
		if dt, err := me.conn.GetDataTable("describe `"+td.SQLName+"`", nil); err == nil {
			if len(dt.Rows) == 0 {
				td.MustCreate = true
			} else {
				for fi := range td.Fields {
					fld := &(td.Fields[fi])
					dbfieldid := -1
					dbtype := ""
					for i, r := range dt.Rows {
						if fn, _ := r.Item("Field"); fn == fld.SQLName {
							dbfieldid = i
							dbtype = r.ItemSingle("Type").(string)
						}
					}
					if dbfieldid != -1 {
						fld.DBFIeldExists = true
						if me.sameFieldType(dbtype, me.SQLType(*fld)) {
							fld.DBTypeOK = true
						} else {
							td.MustModify = true
							td.MustRecreate = true
						}
						dt, _ := me.conn.GetDataTable("show indexes from `"+td.SQLName+"` where key_name=@key_name;", sedi.SQLParms{"@key_name": fld.SQLName})
						if fld.Indexed {
							if len(dt.Rows) == 1 {
								isDBUnique := conv.ToInt64(dt.Rows[0].ItemSingle("Non_unique")) == 0
								if fld.Unique == isDBUnique {
									fld.DBIndexOK = true
								} else {
									td.MustReIndex = true
								}
							} else {
								td.MustReIndex = true
							}
						} else if len(dt.Rows) >= 1 {
							td.MustReIndex = true
						}

					} else {
						fld.DBFIeldExists = false
						fld.DBIndexOK = false
						fld.DBTypeOK = false
						td.MustModify = true
					}
				}
			}
		}
		if td.MustCreate || td.MustModify || td.MustReIndex {
			ok = false
		}
	}
	if err == nil {
		me.modelDiffDone = true
	}
	return ok, err
}

func (me *SQLMapper) UpdateModel() (err error) {
	if !me.modelDiffDone {
		me.ModelIsUpToDate()
	}
	for _, v := range me.TableDefs {
		if v.MustCreate {
			me.createTableStructure(v)
		} else if v.MustModify {
			err = me.modifyTableStructure(v)
		}
		err = me.createTableIndexes(v)
	}
	return nil
}

func (me *SQLMapper) createTableStructure(td sedi.TableDef) (err error) {
	sql := "create table `" + td.SQLName + "` ("
	for i := 0; i < len(td.Fields); i++ {
		fld := td.Fields[i]
		if i != 0 {
			sql += ","
		}
		sql += "\n   `" + fld.SQLName + "` " + me.SQLType(fld)
		if fld.PrimaryKey {
			sql += " primary key"
		}
		if fld.AutoIncrement {
			sql += " auto_increment"
		}

	}
	sql += ");"
	_, err = me.conn.Exec(sql, nil)
	return err
}

func (me *SQLMapper) modifyTableStructure(td sedi.TableDef) (err error) {
	after := ""
	for _, fld := range td.Fields {
		sql := ""

		if !fld.DBFIeldExists {
			sql = "alter table `" + td.SQLName + "` add `" + fld.SQLName + "` " + me.SQLType(fld)
			if fld.PrimaryKey {
				sql += " primary key"
			}
			if fld.AutoIncrement {
				sql += " auto_increment"
			}
		} else if !fld.DBTypeOK {
			if !fld.PrimaryKey {
				sql = "alter table `" + td.SQLName + "` modify column `" + fld.SQLName + "` " + me.SQLType(fld)
			}
		}
		if after != "" && sql != "" {
			sql += " after `" + after + "`"
		}
		if sql != "" {
			_, err = me.conn.Exec(sql, nil)
		}
		after = fld.SQLName
	}
	return err

}

func (me *SQLMapper) createTableIndexes(td sedi.TableDef) (err error) {
	for _, fld := range td.Fields {
		mustRecreate := false

		if !fld.DBIndexOK {
			dt, _ := me.conn.GetDataTable("show indexes from `"+td.SQLName+"` where key_name=@key_name;", sedi.SQLParms{"@key_name": fld.SQLName})
			var isDBUnique bool
			if len(dt.Rows) > 0 {
				isDBUnique = conv.ToInt64(dt.Rows[0].ItemSingle("Non_unique")) == 0
				mustRecreate = (fld.Unique != isDBUnique)
			}
			if fld.Indexed {
				// TODO: MakeFldName lowercase
				if len(dt.Rows) > 1 || mustRecreate {
					me.conn.Exec("drop index `"+fld.SQLName+"` on `"+td.SQLName+"`", nil)
				}
				if len(dt.Rows) != 1 || mustRecreate {
					ui := ""
					if fld.Unique {
						ui = "unique "
					}
					me.conn.Exec("create "+ui+"index `"+fld.SQLName+"` on `"+td.SQLName+"`(`"+fld.SQLName+"`)", nil)
				}
			} else if len(dt.Rows) >= 1 {
				me.conn.Exec("drop index `"+fld.SQLName+"` on `"+td.SQLName+"`", nil)
			}

		}
	}
	return err
}

func (me *SQLMapper) Insert(st interface{}) error {
	td := sedi.TableDefFromStruct(st, quoteFieldName)
	sql := td.InsertStatement
	v := reflect.Indirect(reflect.ValueOf(st))

	result, e := me.conn.Exec(sql, structToSQLParms(st))
	if e == nil {
		fx := td.Fields[td.PkIx]
		if fx.AutoIncrement {
			idv := v.FieldByName(td.Fields[td.PkIx].Name)
			if id, e := result.LastInsertId(); e == nil {
				switch idv.Interface().(type) {
				case int, int8, int16, int32, int64:
					idv.SetInt(id)
				case uint, uint8, uint16, uint32, uint64:
					idv.SetUint(uint64(id))
				}

			}
		}
	}
	return e
}

func (me *SQLMapper) Read(st interface{}) error {
	td := sedi.TableDefFromStruct(st, quoteFieldName)
	sql := td.SelectStatement
	dr, e := me.conn.GetSingleRow(sql, structToSQLParms(st))
	if e == nil {
		dr.FillStruct(st)
	}
	return e
}

func (me *SQLMapper) Update(st interface{}) error {
	td := sedi.TableDefFromStruct(st, quoteFieldName)
	sql := td.UpdateStatement
	_, e := me.conn.Exec(sql, structToSQLParms(st))
	return e
}

func (me *SQLMapper) Delete(st interface{}) error {
	td := sedi.TableDefFromStruct(st, quoteFieldName)
	sql := td.DeleteStatement
	_, e := me.conn.Exec(sql, structToSQLParms(st))
	return e
}

func escapeParameter(parm string) string {
	return strings.Replace(parm, "'", "''", -1)
}

func structToSQLParms(o interface{}) sedi.SQLParms {
	p := make(map[string]interface{})
	v := reflect.Indirect(reflect.ValueOf(o))
	vt := v.Type()
	for i := 0; i < v.NumField(); i++ {
		pname := "@" + vt.Field(i).Name
		switch v.Field(i).Interface().(type) {
		case string:
			p[pname] = sedi.SqlParm("'" + escapeParameter(v.Field(i).String()) + "'")
		case bool:
			xv := v.Field(i).Bool()
			if xv {
				p[pname] = sedi.SqlParm("1")
			} else {
				p[pname] = sedi.SqlParm("0")
			}
		case uint8:
			p[pname] = sedi.SqlParm(fmt.Sprint(uint8(v.Field(i).Uint())))
		case uint16:
			p[pname] = sedi.SqlParm(fmt.Sprint(uint16(v.Field(i).Uint())))
		case uint32:
			p[pname] = sedi.SqlParm(fmt.Sprint(uint32(v.Field(i).Uint())))
		case uint64:
			p[pname] = sedi.SqlParm(fmt.Sprint(uint64(v.Field(i).Uint())))
		case uint:
			p[pname] = sedi.SqlParm(fmt.Sprint(uint(v.Field(i).Uint())))
		case int8:
			p[pname] = sedi.SqlParm(fmt.Sprint(int8(v.Field(i).Int())))
		case int16:
			p[pname] = sedi.SqlParm(fmt.Sprint(int16(v.Field(i).Int())))
		case int32:
			p[pname] = sedi.SqlParm(fmt.Sprint(int32(v.Field(i).Int())))
		case int64:
			p[pname] = sedi.SqlParm(fmt.Sprint(int64(v.Field(i).Int())))
		case int:
			p[pname] = sedi.SqlParm(fmt.Sprint(int(v.Field(i).Int())))
		case float32:
			p[pname] = sedi.SqlParm(fmt.Sprint(float32(v.Field(i).Float())))
		case float64:
			p[pname] = sedi.SqlParm(fmt.Sprint(float64(v.Field(i).Float())))
		case time.Time:
			p[pname] = sedi.SqlParm("date('" + v.Field(i).Interface().(time.Time).UTC().Format("2006-01-02 15:04:05") + "')")
		default:
			p[pname] = v.Field(i).Type().Name()
		}
	}
	return p
}
