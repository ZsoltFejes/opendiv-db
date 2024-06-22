package main

import (
	"testing"
)

type TestObject struct {
	String string  `yaml:"string,omitempty"`
	Number float64 `yaml:"number,omitempty"`
}

func ClearTestDatabase(DB *Driver, t *testing.T) {
	err := DB.Collection("Test").Delete("")
	if err != nil {
		t.Fatal("Unable to clear test database " + err.Error())
	}
}

func TestWriteAndRead(t *testing.T) {
	config, err := LoadConfig()
	if err != nil {
		t.Fatal(err.Error())
	}

	// Create database driver
	DB, err = NewDB(config.DB_path, config)
	if err != nil {
		t.Fatal("Unable to create DB! " + err.Error())
	}

	ClearTestDatabase(DB, t)

	test1 := TestObject{String: "test1", Number: 1}
	doc, err := DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}

	doc, err = DB.Collection("Test").Document(doc.Id)
	if err != nil {
		t.Fatal(err.Error())
	}
	test1_got := TestObject{}
	err = doc.DataTo(&test1_got)
	if err != nil {
		t.Fatal("Unable to marshall test document to object!")
	}

	if test1_got.String != test1.String || test1_got.Number != test1.Number {
		t.Fatal("Returned object return does not match the test object!")
	}

	ClearTestDatabase(DB, t)
}

func TestFilter(t *testing.T) {
	config, err := LoadConfig()
	if err != nil {
		l(err.Error(), true, true)
	}

	// Create database driver
	DB, err = NewDB(config.DB_path, config)
	if err != nil {
		t.Fatal("Unable to create DB! " + err.Error())
	}
	// Cache not needed right now for this test
	// go DB.Cache.RunCachePurge()

	ClearTestDatabase(DB, t)

	test1 := TestObject{String: "test1", Number: 1}
	_, err = DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}

	test2 := TestObject{String: "test2", Number: 2}
	_, err = DB.Collection("Test").Add(test2)
	if err != nil {
		t.Fatal(err.Error())
	}

	test3 := TestObject{String: "test3", Number: 3}
	_, err = DB.Collection("Test").Add(test3)
	if err != nil {
		t.Fatal(err.Error())
	}

	test4 := TestObject{String: "test4", Number: 4}
	_, err = DB.Collection("Test").Add(test4)
	if err != nil {
		t.Fatal(err.Error())
	}

	/////////////////
	// Test Number //
	/////////////////
	var test_number float64 = 2
	col, err := DB.Collection("Test").Where("Number", ">", test_number).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(col) != 2 {
		t.Fatal("Returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("Unable to un-marshall test document to object")
		}
		if got_doc.Number <= test_number {
			t.Fatal("Object found filtered in incorrectly")
		}
	}

	/////////////////
	// Test String //
	/////////////////
	var test_string string = "test1"
	col, err = DB.Collection("Test").Where("String", "==", test_string).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(col) != 1 {
		t.Fatal("Returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("Unable to un-marshall test document to object")
		}
		if got_doc.String != test_string {
			t.Fatal("Object found filtered in incorrectly")
		}
	}
	ClearTestDatabase(DB, t)
}
