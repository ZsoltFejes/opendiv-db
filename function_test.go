package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type TestObject struct {
	String string
	Number float64
	Bool   bool
	Time   time.Time
}

func ClearTestDatabase(DB *Driver) error {
	test_dir := filepath.Join(DB.dir, "Test")
	return os.RemoveAll(test_dir)
}

func TestWriteAndRead(t *testing.T) {
	config, err := LoadConfig()
	if err != nil {
		t.Fatal(err.Error())
	}

	// Create database driver
	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	DB, err = NewDB(config.Path, config)
	if err != nil {
		t.Fatal("Unable to create DB! " + err.Error())
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("Testing read and write operation")
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

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestEncryption(t *testing.T) {
	config, err := LoadConfig()
	if err != nil {
		l(err.Error(), true, true)
	}
	config.Encryption_key = ""
	config.Salt = ""
	// Create database driver
	DB, err = NewDB(config.Path, config)
	if err != nil {
		t.Fatal("Unable to create DB! " + err.Error())
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("Running non encrypted test")
	test1 := TestObject{String: "test1", Number: 1, Bool: true, Time: time.Now()}
	doc_created, err := DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Check to see if file exists
	record := filepath.Join(config.Path, "test", doc_created.Id)
	if _, err := stat(record); err != nil {
		t.Fatal("Document '" + doc_created.Id + "' doesn't exist in 'test'!")
	}

	// read record from database
	b, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err.Error())
	}

	doc := Document{}
	err = json.Unmarshal(b, &doc)
	if err != nil {
		t.Fatal("Unable to unmarshall document: " + err.Error())
	}
	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("Running encrypted test")
	config, err = LoadConfig()
	if err != nil {
		l(err.Error(), true, true)
	}
	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	// Create database driver
	DB, err = NewDB(config.Path, config)
	if err != nil {
		t.Fatal("Unable to create DB! " + err.Error())
	}

	doc_created, err = DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Check to see if file exists
	record = filepath.Join(config.Path, "test", doc_created.Id)
	if _, err := stat(record); err != nil {
		t.Fatal("Document '" + doc_created.Id + "' doesn't exist in 'test'!")
	}

	// read record from database
	b, err = os.ReadFile(record)
	if err != nil {
		t.Fatal(err.Error())
	}

	b, err = DecryptAES(DB.encryption_key, b[:])
	if err != nil {
		t.Fatal(err.Error())
	}

	doc = Document{}
	err = json.Unmarshal(b, &doc)
	if err != nil {
		t.Fatal("Unable to unmarshall document: " + err.Error())
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestFilter(t *testing.T) {
	config, err := LoadConfig()
	if err != nil {
		l(err.Error(), true, true)
	}

	// Create database driver
	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	DB, err = NewDB(config.Path, config)
	if err != nil {
		t.Fatal("Unable to create DB! " + err.Error())
	}
	// Cache not needed right now for this test
	// go DB.Cache.RunCachePurge()

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	test1 := TestObject{String: "test1", Number: 1, Bool: true, Time: time.Now()}
	_, err = DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}

	test2 := TestObject{String: "test2", Number: 2, Bool: true, Time: time.Now().Add(time.Second * 10)}
	_, err = DB.Collection("Test").Add(test2)
	if err != nil {
		t.Fatal(err.Error())
	}

	test3 := TestObject{String: "test3", Number: 3, Bool: true, Time: time.Now().Add(time.Second * 10)}
	_, err = DB.Collection("Test").Add(test3)
	if err != nil {
		t.Fatal(err.Error())
	}

	test4 := TestObject{String: "test4", Number: 4, Bool: false, Time: time.Now().Add(time.Second * 1)}
	_, err = DB.Collection("Test").Add(test4)
	if err != nil {
		t.Fatal(err.Error())
	}

	/////////////////
	// Test Number //
	/////////////////
	t.Log("Testing string number")
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
	t.Log("Testing string filter")
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

	///////////////
	// Test Bool //
	///////////////
	t.Log("Testing bool filter")
	test_bool := true
	col, err = DB.Collection("Test").Where("Bool", "==", test_bool).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(col) != 3 {
		t.Fatal("Returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("Unable to un-marshall test document to object")
		}
		if got_doc.Bool != test_bool {
			t.Fatal("Object found filtered in incorrectly")
		}
	}

	test_bool = false
	col, err = DB.Collection("Test").Where("Bool", "==", test_bool).Documents()
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
		if got_doc.Bool != test_bool {
			t.Fatal("Object found filtered in incorrectly")
		}
	}
	///////////////
	// Test Time //
	///////////////
	t.Log("Testing time filter")
	test_time := time.Now()
	col, err = DB.Collection("Test").Where("Time", "<", test_time.Format(time.RFC3339Nano)).Documents()
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
		if !got_doc.Time.Before(test_time) {
			t.Fatal("Object found filtered in incorrectly")
		}
	}

	col, err = DB.Collection("Test").Where("Time", ">", test_time.Format(time.RFC3339Nano)).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(col) != 3 {
		t.Fatal("Returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("Unable to un-marshall test document to object")
		}
		if !got_doc.Time.After(test_time) {
			t.Fatal("Object found filtered in incorrectly")
		}
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestCache(t *testing.T) {
	config, err := LoadConfig()
	//Set cache timeout for short for testing
	config.Cache_timeout = 5
	config.Cache_limit = 2
	if err != nil {
		l(err.Error(), true, true)
	}

	// Create database driver
	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	DB, err = NewDB(config.Path, config)
	if err != nil {
		t.Fatal("Unable to create DB! " + err.Error())
	}
	// Cache not needed right now for this test
	go DB.RunCachePurge()

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("Testing document timeout purge")
	test1 := TestObject{String: "test1", Number: 1}
	test1_doc, err := DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Test to see document is coming from cache
	doc_got_cache, err := DB.Collection("Test").Document(test1_doc.Id)
	if err != nil {
		t.Fatal(err.Error())
	}

	if doc_got_cache.FromCache != true {
		t.Fatal("Document wasn't returned from Cache")
	}

	t.Log("Sleep to wait to make sure document stays in cache, 2 seconds")
	time.Sleep(time.Second * 2)

	doc_got_cache, err = DB.Collection("Test").Document(test1_doc.Id)
	if err != nil {
		t.Fatal(err.Error())
	}

	if doc_got_cache.FromCache != true {
		t.Fatal("Document wasn't returned from Cache")
	}

	t.Log("Sleep to wait for cache to clear document, 6 seconds")
	time.Sleep(time.Second * 6)

	doc_got_noncache, err := DB.Collection("Test").Document(test1_doc.Id)
	if err != nil {
		t.Fatal(err.Error())
	}

	if doc_got_noncache.FromCache != false {
		t.Fatal("Document was returned from Cache")
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log("Timeout purge test completed")
	t.Log("Testing cache overflow")

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

	col, err := DB.Collection("Test").Documents()
	if err != nil {
		t.Fatal(err.Error())
	}

	expected_cached := config.Cache_limit
	var cached_docs int
	for _, doc := range col {
		if doc.FromCache == true {
			cached_docs = cached_docs + 1
		}
	}
	if cached_docs != int(expected_cached) {
		t.Fatal("Returned number of cached documents was unexpected")
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
}
