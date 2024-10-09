package opendivdb

import (
	"encoding/json"
	"fmt"
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

func Test_CRUD(t *testing.T) {
	var DB *Driver
	config, err := LoadConfig("db_config.yml")
	if err != nil {
		t.Fatal(err.Error())
	}

	// Create database driver
	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	DB, err = NewDB(config)
	if err != nil {
		t.Fatal("unable to create DB! " + err.Error())
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("testing write operation")
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
		t.Fatal("unable to marshall test document to object")
	}

	if test1_got != test1 {
		t.Fatal("returned object return does not match the test object")
	}

	t.Log("testing update operation")
	test1_got.String = "test1_updated"
	test1_got.Number = 10
	doc, err = DB.Collection("Test").Write(doc.Id, test1_got)
	if err != nil {
		t.Fatal(err.Error())
	}

	doc, err = DB.Collection("Test").Document(doc.Id)
	if err != nil {
		t.Fatal(err.Error())
	}
	test1_updated_got := TestObject{}
	err = doc.DataTo(&test1_updated_got)
	if err != nil {
		t.Fatal("unable to marshall test document to object")
	}

	if test1_updated_got != test1_got {
		t.Fatal("returned object return does not match the test object")
	}

	t.Log("testing delete operation")
	err = DB.Collection("Test").Delete(doc.Id)
	if err != nil {
		t.Fatal(err.Error())
	}

	doc, err = DB.Collection("Test").Document(doc.Id)
	if err == nil {
		t.Fatal("document '" + doc.Id + "' still exists")
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func Test_Encryption(t *testing.T) {
	var DB *Driver
	config, err := LoadConfig("db_config.yml")
	if err != nil {
		t.Fatal(err.Error())
	}
	config.Encryption_key = ""
	config.Salt = ""
	// Create database driver
	DB, err = NewDB(config)
	if err != nil {
		t.Fatal("unable to create DB " + err.Error())
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("testing non encrypted database")
	test1 := TestObject{String: "test1", Number: 1, Bool: true, Time: time.Now()}
	doc_created, err := DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Check to see if file exists
	record := filepath.Join(config.Path, "test", doc_created.Id)
	if _, err := stat(record); err != nil {
		t.Fatal("document '" + doc_created.Id + "' doesn't exist in 'test'")
	}

	// read record from database
	b, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err.Error())
	}

	doc := Document{}
	err = json.Unmarshal(b, &doc)
	if err != nil {
		t.Fatal("unable to unmarshall document: " + err.Error())
	}
	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("testing encrypted database")
	config, err = LoadConfig("db_config.yml")
	if err != nil {
		t.Fatal(err.Error())
	}
	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	// Create database driver
	DB, err = NewDB(config)
	if err != nil {
		t.Fatal("unable to create DB! " + err.Error())
	}

	doc_created, err = DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Check to see if file exists
	record = filepath.Join(config.Path, "test", doc_created.Id)
	if _, err := stat(record); err != nil {
		t.Fatal("document '" + doc_created.Id + "' doesn't exist in 'test'")
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
		t.Fatal("unable to un-marshall document: " + err.Error())
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func Test_Filter(t *testing.T) {
	var DB *Driver
	config, err := LoadConfig("db_config.yml")
	if err != nil {
		t.Fatal(err.Error())
	}

	// Create database driver
	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	DB, err = NewDB(config)
	if err != nil {
		t.Fatal("unable to create DB " + err.Error())
	}
	// Cache not needed right now for this test
	go DB.RunCachePurge()

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	test1 := TestObject{String: "test1", Number: 1, Bool: true, Time: time.Now()}
	test1_doc, err := DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}

	test2 := TestObject{String: "test2", Number: 2, Bool: true, Time: time.Now().Add(time.Second * 10)}
	test2_doc, err := DB.Collection("Test").Add(test2)
	if err != nil {
		t.Fatal(err.Error())
	}

	test3 := TestObject{String: "test3", Number: 3, Bool: true, Time: time.Now().Add(time.Second * 10)}
	test3_doc, err := DB.Collection("Test").Add(test3)
	if err != nil {
		t.Fatal(err.Error())
	}

	test4 := TestObject{String: "test4", Number: 4, Bool: false, Time: time.Now().Add(time.Second * 1)}
	test4_doc, err := DB.Collection("Test").Add(test4)
	if err != nil {
		t.Fatal(err.Error())
	}

	/////////////////
	// Test Number //
	/////////////////
	t.Log("testing string number")
	var test_number float64 = 2
	col, err := DB.Collection("Test").Where("Number", ">", test_number).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(col) != 2 {
		t.Fatal("returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("unable to un-marshall test document to object")
		}
		if got_doc.Number <= test_number {
			t.Fatal("object found filtered in incorrectly")
		}
	}

	/////////////////
	// Test String //
	/////////////////
	t.Log("testing string filter")
	var test_string string = "test1"
	col, err = DB.Collection("Test").Where("String", "==", test_string).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(col) != 1 {
		t.Fatal("returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("unable to un-marshall test document to object")
		}
		if got_doc.String != test_string {
			t.Fatal("object found filtered in incorrectly")
		}
	}

	///////////////
	// Test Bool //
	///////////////
	t.Log("testing bool filter")
	test_bool := true
	col, err = DB.Collection("Test").Where("Bool", "==", test_bool).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(col) != 3 {
		t.Fatal("returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("unable to un-marshall test document to object")
		}
		if got_doc.Bool != test_bool {
			t.Fatal("object found filtered in incorrectly")
		}
	}

	test_bool = false
	col, err = DB.Collection("Test").Where("Bool", "==", test_bool).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(col) != 1 {
		t.Fatal("returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("unable to un-marshall test document to object")
		}
		if got_doc.Bool != test_bool {
			t.Fatal("object found filtered in incorrectly")
		}
	}
	///////////////
	// Test Time //
	///////////////
	t.Log("testing time filter")
	test_time := time.Now()
	col, err = DB.Collection("Test").Where("Time", "<", test_time).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(col) != 1 {
		t.Fatal("returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("unable to un-marshall test document to object")
		}
		if !got_doc.Time.Before(test_time) {
			t.Fatal("object found filtered in incorrectly")
		}
	}

	col, err = DB.Collection("Test").Where("Time", ">", test_time).Documents()
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(col) != 3 {
		t.Fatal("returned number of documents are not what is expected")
	}
	for _, doc := range col {
		got_doc := TestObject{}
		err := doc.DataTo(&got_doc)
		if err != nil {
			t.Fatal("unable to un-marshall test document to object")
		}
		if !got_doc.Time.After(test_time) {
			t.Fatal("object found filtered in incorrectly")
		}
	}

	/////////////////////
	// Test Doc States //
	/////////////////////
	t.Log("testing doc states")
	if DB.doc_state["Test/"+test1_doc.Id] != test1_doc.Hash {
		t.Fatal("doc state isn't correct for doc 1")
	}

	if DB.doc_state["Test/"+test2_doc.Id] != test2_doc.Hash {
		t.Fatal("doc state isn't correct for doc 2")
	}

	if DB.doc_state["Test/"+test3_doc.Id] != test3_doc.Hash {
		t.Fatal("doc state isn't correct for doc 3")
	}

	if DB.doc_state["Test/"+test4_doc.Id] != test4_doc.Hash {
		t.Fatal("doc state isn't correct for doc 4")
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func Test_Cache(t *testing.T) {
	var DB *Driver
	config, err := LoadConfig("db_config.yml")
	//Set cache timeout for short for testing
	config.Cache_timeout = 5
	config.Cache_limit = 2
	if err != nil {
		t.Fatal(err.Error())
	}

	// Create database driver
	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	DB, err = NewDB(config)
	if err != nil {
		t.Fatal("unable to create DB " + err.Error())
	}
	// Cache not needed right now for this test
	go DB.RunCachePurge()

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("testing document timeout purge")
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
		t.Fatal("document wasn't returned from Cache")
	}

	t.Log("sleep to wait to make sure document stays in cache, 2 seconds")
	time.Sleep(time.Second * 2)

	doc_got_cache, err = DB.Collection("Test").Document(test1_doc.Id)
	if err != nil {
		t.Fatal(err.Error())
	}

	if doc_got_cache.FromCache != true {
		t.Fatal("document wasn't returned from Cache")
	}

	t.Log("sleep to wait for cache to clear document, 6 seconds")
	time.Sleep(time.Second * 6)

	doc_got_noncache, err := DB.Collection("Test").Document(test1_doc.Id)
	if err != nil {
		t.Fatal(err.Error())
	}

	if doc_got_noncache.FromCache != false {
		t.Fatal("document was returned from Cache")
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log("testing cache overflow")

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
		t.Fatal("returned number of cached documents was unexpected")
	}

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func Test_Subscriptions(t *testing.T) {
	var DB *Driver
	config, err := LoadConfig("db_config.yml")
	//Set cache timeout for short for testing
	config.Cache_timeout = 5
	config.Cache_limit = 2
	if err != nil {
		t.Fatal(err.Error())
	}

	// Create database driver
	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	DB, err = NewDB(config)
	if err != nil {
		t.Fatal("unable to create DB " + err.Error())
	}
	// Cache not needed right now for this test
	go DB.RunCachePurge()

	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}

	/////////////////////////////////////
	// Testing Collection Subscription //
	/////////////////////////////////////
	t.Log("testing entire collection subscriptions")
	// Create subscription for the entire "Test" collection
	collection_test_sub, err := DB.Collection("Test").Subscribe()
	if err != nil {
		t.Fatal(err.Error())
	}
	// Returned slice should have a length of 0 as we haven't created any documents yet
	docs := <-collection_test_sub.Channel
	if len(docs) != 0 {
		t.Fatal("incorrect number of documents were sent in channel")
	}

	// Create first document
	test1 := TestObject{String: "test1", Number: 1}
	_, err = DB.Collection("Test").Add(test1)
	if err != nil {
		t.Fatal(err.Error())
	}
	// Read channel, there should de one document in this update
	docs = <-collection_test_sub.Channel
	if len(docs) != 1 {
		t.Fatal("incorrect number of documents were sent in channel")
	}

	// Create second document
	test2 := TestObject{String: "test2", Number: 2}
	_, err = DB.Collection("Test").Add(test2)
	if err != nil {
		t.Fatal(err.Error())
	}
	// There should be two documents in this update
	docs = <-collection_test_sub.Channel
	if len(docs) != 2 {
		t.Fatal("incorrect number of documents were sent in channel")
	}
	// Unsubscribe from updates
	collection_test_sub.Unsubscribe()

	//////////////////////////////////////////////
	// Testing Filtered Collection Subscription //
	//////////////////////////////////////////////
	t.Log("testing filtered collection subscriptions")
	// Create a document that is out of our filter
	test3 := TestObject{String: "test3", Number: 3}
	_, err = DB.Collection("Test").Add(test3)
	if err != nil {
		t.Fatal(err.Error())
	}
	// Subscribe to documents in "Test" collection where "Number" filed has a number that is less or equal to 2
	filtered_collection_test_sub, err := DB.Collection("Test").Where("Number", "<=", 2).Subscribe()
	if err != nil {
		t.Fatal(err.Error())
	}

	// There should be 2 documents that matches this criteria
	docs = <-filtered_collection_test_sub.Channel
	if len(docs) != 2 {
		fmt.Println(docs)
		t.Fatal("incorrect number of documents were sent in channel")
	}
	// Add a third document that matches the filter
	test4 := TestObject{String: "test4", Number: 2}
	_, err = DB.Collection("Test").Add(test4)
	if err != nil {
		t.Fatal(err.Error())
	}
	// There should be three documents now matching the filter
	docs = <-filtered_collection_test_sub.Channel
	if len(docs) != 3 {
		t.Fatal("incorrect number of documents were sent in channel")
	}

	// Clean up the database
	err = ClearTestDatabase(DB)
	if err != nil {
		t.Fatal(err.Error())
	}
}
