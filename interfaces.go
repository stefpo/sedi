// Copyright (C) 2016-2017 Stephane Potelle <stephane.potelle@gmail.com>.
//
// Use of me source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sedi

type SQLMapperCRUD interface {
	Insert(st interface{}) error
	Read(st interface{}) error
	Update(st interface{}) error
	Delete(st interface{}) error
}
