// Copyright (C) 2016 Stephane Potelle <stephane.potelle@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sedi

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/stefpo/sedi/conv"
)

var lockRead sync.Mutex
var lockWrite sync.Mutex

// LogErrors causes sedi function to output errors
var LogErrors = true

// LogAll causes sedi function to output SQL statements
var LogAll = false

// Conn Wraps the sql.DB object
type Conn struct {
	DB                 *sql.DB
	rowsAffected       int64
	driver             string
	SleepTime          time.Duration
	DisallowConcurency bool
}

type SqlParm string

// SQLParms is a map of SQL parameters
type SQLParms map[string]interface{}

// ToSqlParms converts a structure to a SqlParms
func ToSqlParms(o interface{}) SQLParms {
	return conv.StructToMap(o)
}

// OpenConnection opens a SQL connection and returns a Conn object
func OpenConnection(driver string, connString string) (Conn, error) {
	var conn Conn
	var err error
	conn.DisallowConcurency = true // As a general rule, do not allow multiple go routines
	conn.DB, err = sql.Open(driver, connString)
	conn.driver = driver
	if err != nil && LogErrors {
		log.Print(err.Error())
	}
	return conn, err
}

// Close closes the underlying SQL connection
func (cn *Conn) Close() {
	cn.DB.Close()
}

func (cn *Conn) sleep() {
	if cn.SleepTime > 0 {
		time.Sleep(cn.SleepTime)
	}
}

// GetDataTable executes a SELECT statement and returns the result in a datatable
func (cn *Conn) GetDataTable(query string, parms SQLParms) (DataTable, error) {
	var dt DataTable
	var err error
	SQL := query
	if parms != nil {
		SQL = insertParameters(SQL, parms)
	}
	if LogAll {
		log.Print(SQL)
	}
	if cn.DisallowConcurency {
		defer lockWrite.Unlock()
		lockWrite.Lock()
	}
	if rows, err := cn.DB.Query(SQL); err == nil {
		dt.Fill(rows, -1)
		cn.sleep()
		err = nil
	} else {

		if LogErrors {
			log.Print(err.Error())
		}
		err = errors.New("GetDataTable:" + err.Error())
	}
	return dt, err
}

func (cn *Conn) GetSingleRow(query string, parms SQLParms) (DataRow, error) {
	var dt DataTable
	var err error
	var dr DataRow
	dt.Clear()

	SQL := query
	if parms != nil {
		SQL = insertParameters(SQL, parms)
	}
	if LogAll {
		log.Print(SQL)
	}
	if cn.DisallowConcurency {
		defer lockWrite.Unlock()
		lockWrite.Lock()
	}
	if rows, err := cn.DB.Query(SQL); err == nil {
		dt.Fill(rows, 1)
		if len(dt.Rows) > 0 {
			dr = dt.Rows[0]
			err = nil
		} else {
			err = errors.New("No data")
		}
		cn.sleep()
	} else {
		if LogErrors {
			log.Print(err.Error())
		}
		err = errors.New("GetSingleRow:" + err.Error())
	}
	return dr, err
}

func (cn *Conn) ReadStruct(table string, id int64, output interface{}) error {
	var err error
	qry := "select * from " + table + " where id = @id"
	if dr, e := cn.GetSingleRow(qry, SQLParms{"@id": id}); e == nil {
		dr.FillStruct(output)
		err = nil
	} else {
		err = e
	}
	return err
}

func (cn *Conn) GetScalar(query string, parms SQLParms) (interface{}, error) {
	var dt DataTable
	var ret interface{}
	var err error

	dt.Clear()

	SQL := query
	if parms != nil {
		SQL = insertParameters(SQL, parms)
	}
	if LogAll {
		log.Print(SQL)
	}
	if cn.DisallowConcurency {
		defer lockWrite.Unlock()
		lockWrite.Lock()
	}
	if rows, err := cn.DB.Query(SQL); err == nil {
		dt.Fill(rows, 1)
		if len(dt.Rows) > 0 {
			ret = dt.Rows[0].Items()[0]
		} else {
			ret = nil
		}
	} else {
		if LogErrors {
			log.Print(err.Error())
		}
		err = errors.New("GetScalar:" + err.Error())
	}
	return ret, err
}

func (cn *Conn) Exists(query string, parms SQLParms) (bool, error) {
	var dt DataTable
	var ret bool
	var err error
	dt.Clear()

	SQL := query
	if parms != nil {
		SQL = insertParameters(SQL, parms)
	}
	if LogAll {
		log.Print(SQL)
	}
	if cn.DisallowConcurency {
		defer lockWrite.Unlock()
		lockWrite.Lock()
	}
	if rows, err := cn.DB.Query(SQL); err == nil {
		dt.Fill(rows, 1)
		if len(dt.Rows) > 0 {
			ret = true
		} else {
			ret = false
		}

	} else {
		if LogErrors {
			log.Print(err.Error())
		}
		err = errors.New("Exists:" + err.Error())
	}
	return ret, err
}

func (cn *Conn) ExecNoResult(query string, parms SQLParms) (err error) {
	_, err = cn.Exec(query, parms)
	return err
}

func (cn *Conn) Exec(query string, parms SQLParms) (sql.Result, error) {
	var err error
	var ret sql.Result
	SQL := query

	if parms != nil {
		SQL = insertParameters(SQL, parms)
	}
	if LogAll {
		log.Print(SQL)
	}
	if cn.DisallowConcurency {
		defer lockWrite.Unlock()
		defer lockRead.Unlock()
		lockRead.Lock()
		lockWrite.Lock()
	}
	if result, err := cn.DB.Exec(SQL); err == nil {
		if x, e := result.RowsAffected(); e == nil {
			cn.rowsAffected = x
		} else {
			cn.rowsAffected = 0
		}
		cn.sleep()
		ret = result
	} else {
		cn.rowsAffected = 0
		if LogErrors {
			log.Print(SQL)
			log.Print(err.Error())
		}
		err = errors.New("Exec:" + err.Error())
	}
	return ret, err
}

func (cn *Conn) RowsAffected() int64 { return cn.rowsAffected }

func insertParameters(query string, parms SQLParms) string {
	res := query
	var po string
	for p := range parms {
		parm := parms[p]

		switch parm.(type) {
		case nil:
			po = "null"
		case time.Time:
			po = "date('" + parm.(time.Time).Format("2006-01-02 15:04:05") + "')"
			break
		case SqlParm:
			po = string(parm.(SqlParm))
		case string:
			po = "'" + escapeParameter(parm.(string)) + "'"
			break
		default:
			po = fmt.Sprint(parm)
		}

		res = strings.Replace(res, p, po, -1)
	}
	return res
}

func escapeParameter(parm string) string {
	return strings.Replace(parm, "'", "''", -1)
}

func IsNull(x interface{}, defaultvalue interface{}) interface{} {
	switch x.(type) {
	case nil:
		return defaultvalue
	default:
		return defaultvalue
	}
}
