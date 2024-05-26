package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type (
	// Driver is what is used to interact with the scribble database. It runs
	// transactions, and provides log output
	Driver struct {
		encryption_key string
		mutex          sync.Mutex
		mutexes        map[string]*sync.Mutex
		Cache          Cache
		dir            string // the directory where scribble will create the database
	}

	Cache struct {
		Documents     map[string]Cached_Doc
		Cache_timeout float64 // Cache timeout in seconds
	}

	Cached_Doc struct {
		Cached_at time.Time
		Document  Document
	}

	Collection_ref struct {
		collection_name string
		driver          *Driver
	}
	Document struct {
		Id         string
		Updated_at time.Time
		Hash       string // Hash of "Data" bytes
		Data       json.RawMessage
	}

	Filter struct {
		collection *Collection_ref
		driver     *Driver
		field      string // Filed to filter by
		operator   string // Accepted conditions ==, <=, >=, !=, >, <. Comparison is done in the following format: [field] [operator] [value]
		value      any    // Value of condition
	}
)

func ValidateID(id string) error {
	if id == "" {
		return fmt.Errorf("empty value")
	}

	if strings.Contains(id, "/") || strings.Contains(id, `\`) {
		return fmt.Errorf(`unsupported character, can't contain '/' or '\'`)
	}
	return nil
}

func (d Document) DataTo(v interface{}) error {
	doc_b, err := json.Marshal(d.Data)
	if err != nil {
		return fmt.Errorf("Unable to marshal document data! " + err.Error())
	}

	return json.Unmarshal(doc_b, &v)
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

// New creates a new scribble database at the desired directory location, and
// returns a *Driver to then use for interacting with the database
func NewDB(dir string, config Config) (*Driver, error) {

	//
	dir = filepath.Clean(dir)

	//
	driver := Driver{
		encryption_key: config.Encryption_key,
		dir:            dir,
		mutexes:        make(map[string]*sync.Mutex),
		Cache:          Cache{Cache_timeout: config.Cache_timeout, Documents: make(map[string]Cached_Doc)},
	}

	// if the database already exists, just use it
	if _, err := os.Stat(dir); err == nil {
		l("Using '"+dir+"' (database already exists)", false, true)
		return &driver, nil
	}

	// if the database doesn't exist create it
	l("Creating scribble database at '"+dir+"'...", false, true)
	return &driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Collection(name string) *Collection_ref {
	return &Collection_ref{collection_name: name, driver: d}
}

// Write locks the database and attempts to write the record to the database under
// the [collection] specified with the random document name (UUID). Name is added to document under [Id]
func (c *Collection_ref) Add(v interface{}) (Document, error) {
	new_id := uuid.NewString()
	return c.Write(new_id, v)
}

// Write locks the database and attempts to write the record to the database under
// the [collection] specified with the [document] name given
func (c *Collection_ref) Write(document string, v interface{}) (Document, error) {
	err := ValidateID(c.collection_name)
	if err != nil {
		return Document{}, fmt.Errorf(`collection name validation error - ` + err.Error())
	}

	// ensure there is a document (name) to save record as
	err = ValidateID(document)
	if err != nil {
		return Document{}, fmt.Errorf(`document ID validation error - ` + err.Error())
	}

	mutex := c.driver.getOrCreateMutex(c.collection_name)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(c.driver.dir, c.collection_name)
	fnlPath := filepath.Join(dir, document)
	tmpPath := fnlPath + ".tmp"

	// create collection directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return Document{}, err
	}
	v_b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return Document{}, err
	}
	doc := Document{Id: document, Data: v_b, Updated_at: time.Now(), Hash: GetMD5Hash(string(v_b[:]))}
	b, err := json.MarshalIndent(doc, "", "\t")
	if err != nil {
		return Document{}, err
	}

	err = c.driver.Cache.Add(*c, doc)
	if err != nil {
		return Document{}, err
	}
	if c.driver.encryption_key != "" {
		ct := EncryptAES(c.driver.encryption_key, b)
		// write marshaled data to the temp file
		if err := os.WriteFile(tmpPath, ct, 0644); err != nil {
			return Document{}, err
		}
	} else {
		// write marshaled data to the temp file
		if err := os.WriteFile(tmpPath, b, 0644); err != nil {
			return Document{}, err
		}
	}

	// move final file into place
	err = os.Rename(tmpPath, fnlPath)
	if err != nil {
		return Document{}, err
	}
	return doc, nil
}

// Read a document from the database
func (c *Collection_ref) Document(id string) (Document, error) {
	// ensure there is a place to save record
	err := ValidateID(c.collection_name)
	if err != nil {
		return Document{}, fmt.Errorf(`collection name validation error - ` + err.Error())
	}

	// ensure there is a document (name) to save record as
	err = ValidateID(id)
	if err != nil {
		return Document{}, fmt.Errorf(`document ID validation error - ` + err.Error())
	}

	if doc, in_cache := c.driver.Cache.GetDoc(c.collection_name, id); in_cache {
		return doc, nil
	}

	// check to see if collection (directory) exists
	dir := filepath.Join(c.driver.dir, c.collection_name)
	if _, err := stat(dir); err != nil {
		return Document{}, fmt.Errorf("Collection '" + c.collection_name + "' doesn't exist!")
	}

	// Check to see if file exists
	record := filepath.Join(c.driver.dir, c.collection_name, id)
	if _, err := stat(record); err != nil {
		return Document{}, fmt.Errorf("Document '" + id + "' doesn't exist in '" + c.collection_name + "!")
	}

	// read record from database
	b, err := os.ReadFile(record)
	if err != nil {
		return Document{}, err
	}

	if c.driver.encryption_key != "" {
		b = DecryptAES(c.driver.encryption_key, b[:])
	}
	doc := Document{}
	err = json.Unmarshal(b, &doc)
	if err != nil {
		return Document{}, err
	}

	return doc, nil
}

// ReadAll documents from a collection; this is returned as a Collection
// there is no way of knowing what type the record is.
func (c *Collection_ref) Documents() ([]Document, error) {
	var col []Document
	// ensure there is a collection to read
	if c.collection_name == "" {
		return col, fmt.Errorf("missing collection - unable to record location")
	}

	if strings.Contains(c.collection_name, "/") || strings.Contains(c.collection_name, `\`) {
		return col, fmt.Errorf(`unsupported character in collection name, collection name can't contain '/' or '\'`)
	}

	// check to see if collection (directory) exists
	dir := filepath.Join(c.driver.dir, c.collection_name)
	if _, err := stat(dir); err != nil {
		return col, fmt.Errorf("Collection '" + c.collection_name + "' doesn't exist!")
	}

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, _ := os.ReadDir(dir)

	// iterate over each of the files, attempting to read the file. If successful
	// append the files to the collection of read files
	for _, file := range files {
		doc, err := c.Document(file.Name())
		if err != nil {
			return col, fmt.Errorf("unable to read file "+file.Name(), false, true)
		}

		// append read file
		col = append(col, doc)
	}

	// unmarshal the read files as a comma delimited byte array
	return col, nil
}

// Delete locks that database and then attempts to remove the collection/document
// specified by [path]
func (c *Collection_ref) Delete(id string) error {
	err := ValidateID(c.collection_name)
	if err != nil {
		return fmt.Errorf(`collection name validation error - ` + err.Error())
	}

	// ensure there is a document (name) to save record as
	if id != "" {
		err = ValidateID(id)
		if err != nil {
			return fmt.Errorf(`document ID validation error - ` + err.Error())
		}
	}

	path := filepath.Join(c.collection_name, id)
	mutex := c.driver.getOrCreateMutex(c.collection_name)
	mutex.Lock()
	defer mutex.Unlock()

	//
	dir := filepath.Join(c.driver.dir, path)

	switch fi, err := stat(dir); {

	// if fi is nil or error is not nil return
	case fi == nil, err != nil:
		return fmt.Errorf("unable to find file or directory named %v", path)

	// remove directory and all contents
	case fi.Mode().IsDir():
		// read all the files in the transaction.Collection;
		files, _ := os.ReadDir(dir)
		// Loop through each file to delete it from cache
		for _, file := range files {
			c.driver.Cache.Delete(c.collection_name, file.Name())
		}
		return os.RemoveAll(dir)

	// remove file
	case fi.Mode().IsRegular():
		c.driver.Cache.Delete(c.collection_name, id)
		return os.RemoveAll(dir)
	}

	return nil
}

// Creates Filter object so do simple queries
func (c *Collection_ref) Where(field string, operator string, value string) *Filter {
	return &Filter{collection: c, driver: c.driver, field: field, operator: operator, value: value}
}

func (f *Filter) Documents() ([]Document, error) {
	var col []Document
	// ensure there is a collection to read
	if f.collection.collection_name == "" {
		return col, fmt.Errorf("missing collection - unable to record location")
	}

	dir := filepath.Join(f.driver.dir, f.collection.collection_name)

	// check to see if collection (directory) exists
	if _, err := stat(dir); err != nil {
		return col, err
	}

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, _ := os.ReadDir(dir)

	// iterate over each of the files, attempting to read the file. If successful
	// append the files to the collection of read files
	for _, file := range files {
		doc, err := f.collection.Document(file.Name())
		if err != nil {
			return col, fmt.Errorf("Unable to read file "+file.Name(), false, true)
		}

		// Accepted operators
		operators := map[string]bool{
			"==": true,
			"<=": true,
			">=": true,
			"!=": true,
			"<":  true,
			">":  true}

		// Check to make sure correct condition is provided
		if _, ok := operators[f.operator]; !ok {
			return col, fmt.Errorf("Filter '" + f.operator + "' is not supported. Accepted conditions ==, <=, >=, !=, <, > ")
		}

		// Marshal document data into generic map for comparison
		var d map[string]interface{}
		if err := json.Unmarshal(doc.Data, &d); err != nil {
			panic(err)
		}

		// Find field
		field := d[f.field]
		// Check for provided field
		if field == nil {
			return col, fmt.Errorf("field not provided")
		}

		switch real := field.(type) {
		case string:
			switch filter_t := f.value.(type) {
			case string:
				switch f.operator {
				case "==":
					if real == filter_t {
						col = append(col, doc)
					}
				case "!=":
					if real != filter_t {
						col = append(col, doc)
					}
				}
			}
		case float64:
			switch filter_t := f.value.(type) {
			case float64:
				switch f.operator {
				case "==":
					if real == f.value {
						col = append(col, doc)
					}
				case "<=":
					if real <= filter_t {
						col = append(col, doc)
					}
				case ">=":
					if real >= filter_t {
						col = append(col, doc)
					}
				case "!=":
					if real != filter_t {
						col = append(col, doc)
					}
				case "<":
					if real < filter_t {
						col = append(col, doc)
					}
				case ">":
					if real > filter_t {
						col = append(col, doc)
					}
				}
			default:
				return col, fmt.Errorf("Filter Value is not float64. For more details: https://pkg.go.dev/encoding/json#Unmarshal")
			}
		case bool:
			switch filter_t := f.value.(type) {
			case bool:
				switch f.operator {
				case "==":
					if real == filter_t {
						col = append(col, doc)
					}
				case "!=":
					if real != filter_t {
						col = append(col, doc)
					}
				}
			}
		}
	}
	return col, nil
}

func stat(path string) (fi os.FileInfo, err error) {

	// check for dir, if path isn't a directory check to see if it's a file
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path)
	}

	return
}

// getOrCreateMutex creates a new collection specific mutex any time a collection
// is being modified to avoid unsafe operations
func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]

	// if the mutex doesn't exist make it
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func (c *Cache) Add(coll_ref Collection_ref, doc Document) error {
	cached_doc := Cached_Doc{Cached_at: time.Now(), Document: doc}
	c.Documents[coll_ref.collection_name+"/"+doc.Id] = cached_doc
	return nil
}

func (c *Cache) GetDoc(collection_name string, document_id string) (Document, bool) {
	if val, ok := c.Documents[collection_name+"/"+document_id]; ok {
		return val.Document, true
	}
	return Document{}, false
}

func (c *Cache) Delete(collection_name string, document_id string) {
	delete(c.Documents, collection_name+"/"+document_id)
}

func (c *Cache) check() {
	timeout := time.Second * time.Duration(c.Cache_timeout)
	if timeout == 0 {
		timeout = time.Duration(time.Minute * 5)
	}
	for id, value := range c.Documents {
		if value.Cached_at.Add(timeout).Before(time.Now()) {
			delete(c.Documents, id)
		}
	}
}

// Must be run as a go routine. Runs an infinite loop to check the cache every 5 seconds to deletes expired cache
func (c *Cache) RunCachePurge() {
	for {
		c.check()
		time.Sleep(time.Second * 5)
	}
}
