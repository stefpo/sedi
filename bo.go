// Copyright (C) 2016-2017 Stephane Potelle <stephane.potelle@gmail.com>.
//
// Use of me source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sedi

// Insert is a helper function for creating Business objects
func Insert(st interface{}, mapper SQLMapperCRUD) error {
	var err error

	if err == nil {
		err = mapper.Insert(st)
	}
	return err
}

// Update is a helper function for creating Business objects
func Update(st interface{}, mapper SQLMapperCRUD) error {
	var err error

	if err == nil {
		err = mapper.Update(st)
	}
	return err
}

// Delete is a helper function for creating Business objects
func Delete(st interface{}, mapper SQLMapperCRUD) error {
	var err error

	if err == nil {
		err = mapper.Delete(st)
	}
	return err
}

// Read is a helper function for creating Business objects
func Read(st interface{}, mapper SQLMapperCRUD) error {
	var err error

	if err == nil {
		err = mapper.Read(st)
	}
	return err
}
