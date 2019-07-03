package main

import (
	"fmt"
	"time"

	"github.com/stefpo/sedi"
	"github.com/stefpo/sedi/conv"
	//_ "github.com/stefpo/sedi/mapper/mysql"
	sq3orm "github.com/stefpo/sedi/mapper/sqlite3"
)

type ContactInfo struct {
	Id         uint32 `autoincrement:"y"`
	FirstName  string `size:"23"`
	LastName   string
	IsMale     bool
	Email      string
	AddedField string
	FkGroup    string
	BirthDate  time.Time `indexed:"y"`
	Tint16     int16
	Tint32     int32
	Tunit64    uint64
}

type GroupInfo struct {
	Id   int    `autoincrement:"y"`
	Name string `indexed:"y" unique:"n"`
}

func main() {
	fmt.Println("Test Automodel")

	sedi.LogAll = true
	sedi.LogErrors = true

	mm := sq3orm.GetSQLMapper().
		AddPersistence(ContactInfo{}).
		AddPersistence(GroupInfo{}).
		OpenConnection("file:gotest.sqlite3?cache=shared&mode=rwc&")
		//OpenConnection("root:xenon21@/gotest")

	if mm.Connection() != nil {
		fmt.Println("Connect successful")
	} else {
		return
	}

	if ok, e := mm.ModelIsUpToDate(); e == nil && !ok {
		fmt.Println("Model needs changes")
		mm.UpdateModel()
	} else if e != nil {
		fmt.Println(e)
		return
	} else {
		fmt.Println("Model is up to date. No change required")
	}
	//fmt.Println(sedi.Stringify(mm.TableDefs))

	contact := &ContactInfo{FirstName: "John", LastName: "Doe", Email: "john.doe@gmail.com", Tint16: 16, Tint32: 32, Tunit64: 64}

	//mm.Connection().Exec("truncate table contact_info;", nil)
	mm.Connection().Exec("begin transaction;", nil)
	mm.Connection().Exec("delete from contact_info;", nil)
	mm.Connection().Exec("insert into contact_info (first_name, Last_name) values('Jean','Dupont') ", nil)

	nw := 50

	complete := make(chan int, nw)

	for i := 0; i < nw; i++ {
		go func() {
			sedi.Insert(contact, mm)
			complete <- 1
		}()
	}

	for i := 0; i < nw; i++ {
		<-complete
	}

	mm.Connection().Exec("commit;", nil)
	//mm.Insert(contact)

	//contact.AddedField = "Test update"
	contact.IsMale = true
	contact.BirthDate = conv.ToTime("1988-02-24 18:03:17 (UTC)")
	sedi.Update(contact, mm)
	//mm.Update(contact)
	fmt.Println(sedi.Stringify(contact))

	Contact2 := &ContactInfo{Id: contact.Id}
	sedi.Read(Contact2, mm)
	//mm.Read(Contact2)
	fmt.Println(sedi.Stringify(Contact2))

	Contact3 := &ContactInfo{Id: 1}
	sedi.Read(Contact3, mm)
	//mm.Read(Contact2)
	fmt.Println(sedi.Stringify(Contact3))

	Contact2.Id = 51
	mm.Read(Contact2)
	//mm.Read(Contact2)
	fmt.Println("BirthDate", conv.ToString(Contact2.BirthDate.Local()))

	dt, _ := mm.Connection().GetDataTable("select * from contact_info", nil)

	dtjs, _ := dt.ToJSON()
	fmt.Println(dtjs)

	dt.SaveToFile("datatable.json")

	dt2 := sedi.DataTable{}
	dt2.FillFromJSON(dtjs)
	fmt.Println(dt2.ToJSON())

}
