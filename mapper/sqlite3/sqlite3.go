// Copyright (C) 2016-2017 Stephane Potelle <stephane.potelle@gmail.com>.
//
// Use of me source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sqlite3

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/stefpo/sedi"

	_ "github.com/mattn/go-sqlite3" // me package works only with SQLite3
	//_ "github.com/mxk/go-sqlite/sqlite3"
)

// GetSQLMapper create a new SQLmapper.
func GetSQLMapper() *SQLMapper {
	return &SQLMapper{}
}

// SQLMapper provides persistence framework for structures
// in a MySQL database
type SQLMapper struct {
	conn *sedi.Conn
	sedi.TableDefs
	modelDiffDone bool
}

func (me *SQLMapper) Connection() *sedi.Conn {
	return me.conn
}

func (me *SQLMapper) quoteFieldName(s string) string {
	return "`" + s + "`"
}

func (me *SQLMapper) OpenConnection(url string) *SQLMapper {
	if cn, e := sedi.OpenConnection("sqlite3", url); e == nil {
		cn.DisallowConcurency = true
		me.conn = &cn
		cn.SleepTime = time.Duration(10) * time.Millisecond
		me.conn.ExecNoResult("pragma busy_timeout=10000", nil)
		me.conn.ExecNoResult("pragma locking_mode = NORMAL", nil)
		me.conn.ExecNoResult("pragma encoding = \"UTF-8\"", nil)
	} else {
		me.conn = nil
	}
	return me
}

func (me *SQLMapper) CloseConnection() {
	me.conn.Close()
	me.conn = nil
}

func (me *SQLMapper) AddPersistence(st interface{}) *SQLMapper {
	if me.TableDefs == nil {
		me.TableDefs = make(sedi.TableDefs, 0)
	}
	td := sedi.TableDefFromStruct(st, me.quoteFieldName)
	me.TableDefs = append(me.TableDefs, td)
	return me
}

func (me *SQLMapper) SQLType(fd sedi.FieldDef) string {
	var ts string
	switch fd.GoTypeName {
	case "byte", "int", "short", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "char", "bool":
		ts = "integer"
	case "string":
		ts = "text"
	case "float32", "float64":
		ts = "real"
	case "Time":
		ts = "text"
	}
	return ts
}

func (me *SQLMapper) ModelIsUpToDate() (ok bool, err error) {
	ok = true
	err = nil
	for i := range me.TableDefs {
		td := &(me.TableDefs[i])
		if dt, err := me.conn.GetDataTable("pragma table_info ('"+td.SQLName+"')", nil); err == nil {
			if len(dt.Rows) == 0 {
				td.MustCreate = true
			} else {
				for fi := range td.Fields {
					fld := &(td.Fields[fi])
					dbfieldid := -1
					dbtype := ""
					for i, r := range dt.Rows {
						if fn, _ := r.Item("name"); fn == fld.SQLName {
							dbfieldid = i
							dbtype = r.ItemSingle("type").(string)
						}
					}
					if dbfieldid != -1 {
						fld.DBFIeldExists = true
						if strings.ToLower(me.SQLType(*fld)) == strings.ToLower(dbtype) {
							fld.DBTypeOK = true
						} else {
							td.MustModify = true
							td.MustRecreate = true
						}
						ui := ""
						if fld.Unique {
							ui = "unique "
						}
						createIndexSQL := "create " + ui + "index `" + td.SQLName + "." + fld.SQLName + "` on `" + td.SQLName + "`(`" + fld.SQLName + "`)"
						dt, _ := me.conn.GetDataTable("select * from sqlite_master where type='index' and tbl_name=@tbl_name and name=@key_name;",
							sedi.SQLParms{"@tbl_name": td.SQLName, "@key_name": td.SQLName + "." + fld.SQLName})
						if len(dt.Rows) > 0 {
							if strings.ToLower(dt.Rows[0].ItemSingle("sql").(string)) != strings.ToLower(createIndexSQL) || !fld.Indexed {
								td.MustReIndex = true
							} else {
								fld.DBIndexOK = true
							}
						} else {
							if fld.Indexed {
								td.MustReIndex = true
							}
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
	}
	sql += ");"
	_, err = me.conn.Exec(sql, nil)
	return err
}

func (me *SQLMapper) modifyTableStructure(td sedi.TableDef) (err error) {
	if !td.MustRecreate {
		for _, fld := range td.Fields {
			sql := ""

			if !fld.DBFIeldExists {
				sql = "alter table `" + td.SQLName + "` add `" + fld.SQLName + "` " + me.SQLType(fld)
				if fld.PrimaryKey {
					sql += " primary key"
				}

			} else {
				if !fld.PrimaryKey {
					//sql = sql + "alter table `" + td.SQLName + "` modify column `" + fld.SQLName + "` " + me.SqlType(fld)
				}
			}
			if sql != "" {
				_, err = me.conn.Exec(sql, nil)
			}
		}
	} else {
		err = me.conn.ExecNoResult("begin exclusive transaction", nil)
		if err == nil {
			err = me.conn.ExecNoResult("alter table `"+td.SQLName+"` rename to `tmp_rename_"+td.SQLName+"`", nil)
		}
		if err == nil {
			err = me.createTableStructure(td)
		}
		if err == nil {
			err = me.conn.ExecNoResult("insert into `"+td.SQLName+"` select * from `tmp_rename_"+td.SQLName+"`", nil)
		}
		if err == nil {
			err = me.createTableIndexes(td)
		}
		if err == nil {
			err = me.conn.ExecNoResult("drop table `tmp_rename_"+td.SQLName+"`", nil)
		}
		if err == nil {
			err = me.conn.ExecNoResult("commit", nil)
		} else {
			me.conn.ExecNoResult("rollback transaction", nil)
		}
	}
	return err

}

func (me *SQLMapper) createTableIndexes(td sedi.TableDef) (err error) {
	for _, fld := range td.Fields {
		ui := ""
		if fld.Unique {
			ui = "unique "
		}
		createIndexSQL := "create " + ui + "index `" + td.SQLName + "." + fld.SQLName + "` on `" + td.SQLName + "`(`" + fld.SQLName + "`)"
		dropIndexSQL := "drop index `" + td.SQLName + "." + fld.SQLName + "`"

		dt, _ := me.conn.GetDataTable("select * from sqlite_master where type='index' and tbl_name=@tbl_name and name=@key_name;",
			sedi.SQLParms{"@tbl_name": td.SQLName, "@key_name": td.SQLName + "." + fld.SQLName})
		if len(dt.Rows) > 0 {
			if strings.ToLower(dt.Rows[0].ItemSingle("sql").(string)) != strings.ToLower(createIndexSQL) {
				me.conn.Exec(dropIndexSQL, nil)
				me.conn.Exec(createIndexSQL, nil)
			} else if !fld.Indexed {
				me.conn.Exec(dropIndexSQL, nil)
			}
		} else {
			if fld.Indexed {
				me.conn.Exec(createIndexSQL, nil)
			}
		}
	}
	return err
}

func (me *SQLMapper) Insert(st interface{}) error {
	td := sedi.TableDefFromStruct(st, me.quoteFieldName)
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
	td := sedi.TableDefFromStruct(st, me.quoteFieldName)
	sql := td.SelectStatement
	dr, e := me.conn.GetSingleRow(sql, structToSQLParms(st))
	if e == nil {
		dr.FillStruct(st)
	}
	return e
}

func (me *SQLMapper) Update(st interface{}) error {
	td := sedi.TableDefFromStruct(st, me.quoteFieldName)
	sql := td.UpdateStatement
	_, e := me.conn.Exec(sql, structToSQLParms(st))
	return e
}

func (me *SQLMapper) Delete(st interface{}) error {
	td := sedi.TableDefFromStruct(st, me.quoteFieldName)
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
			//p[pname] = sedi.SqlParm("datetime('" + time.Time(v.Field(i).Interface().(time.Time)).Format("2006-01-02 15:04:05") + "')")
			if v.Field(i).Interface().(time.Time).IsZero() {
				p[pname] = "null"
			} else {
				p[pname] = sedi.SqlParm("datetime('" + v.Field(i).Interface().(time.Time).UTC().Format("2006-01-02 15:04:05") + "')")
			}
		default:
			p[pname] = v.Field(i).Type().Name()
		}
	}
	return p
}
