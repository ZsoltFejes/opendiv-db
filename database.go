package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
		dir            string // the directory where scribble will create the database
	}
	Document struct {
		Id         string
		Updated_at time.Time
		Hash       string // Hash of "Data" bytes
		Data       json.RawMessage
	}
	Collection struct {
		Documents []Document
	}

	Filter struct {
		Collection string
		Field      string // Filed to filter by
		Condition  string // Accepted conditions ==, <=, >=, !=, >, <. Comparison is done in the following format: [Field] [Confition] [Value]
		Value      any    // Value of condition
	}
)

func (d *Document) DataTo(v interface{}) error {
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

func l(message string, fatal bool, public bool) {
	if (public || *debug) && !fatal {
		log.Println(message)
	} else if fatal {
		log.Fatalln(message)
	}
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

// Write locks the database and attempts to write the record to the database under
// the [collection] specified with the random document name (UUID). Name is added to document under [Id]
func (d *Driver) Add(collection string, v interface{}) (Document, error) {
	new_id := uuid.NewString()
	return d.Write(collection, new_id, v)
}

// Write locks the database and attempts to write the record to the database under
// the [collection] specified with the [document] name given
func (d *Driver) Write(collection, document string, v interface{}) (Document, error) {

	// ensure there is a place to save record
	if collection == "" {
		return Document{}, fmt.Errorf("Missing collection - no place to save record!")
	}

	// ensure there is a document (name) to save record as
	if document == "" {
		document = uuid.NewString()
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
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

	if d.encryption_key != "" {
		ct := EncryptAES(d.encryption_key, b)
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

// Read a record from the database
func (d *Driver) Read(collection, document string) (Document, error) {

	// ensure there is a place to save record
	if collection == "" {
		return Document{}, fmt.Errorf("Missing collection - no place to save record!")
	}

	// ensure there is a document (name) to save record as
	if document == "" {
		return Document{}, fmt.Errorf("Missing document - unable to save record (no name)!")
	}

	//
	record := filepath.Join(d.dir, collection, document)

	// check to see if file exists
	if _, err := stat(record); err != nil {
		return Document{}, err
	}

	// read record from database
	b, err := os.ReadFile(record)
	if err != nil {
		return Document{}, err
	}

	if d.encryption_key != "" {
		b = DecryptAES(d.encryption_key, b[:])
	}
	doc := Document{}
	err = json.Unmarshal(b, &doc)
	if err != nil {
		return Document{}, err
	}

	return doc, nil
}

// ReadAll records from a collection; this is returned as a slice of strings because
// there is no way of knowing what type the record is.
func (d *Driver) ReadAll(filter Filter) (Collection, error) {
	var c Collection
	// ensure there is a collection to read
	if filter.Collection == "" {
		return c, fmt.Errorf("Missing collection - unable to record location!")
	}

	dir := filepath.Join(d.dir, filter.Collection)

	// check to see if collection (directory) exists
	if _, err := stat(dir); err != nil {
		return c, err
	}

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, _ := os.ReadDir(dir)

	// iterate over each of the files, attempting to read the file. If successful
	// append the files to the collection of read files
	for _, file := range files {
		doc, err := d.Read(filter.Collection, file.Name())
		if err != nil {
			return c, fmt.Errorf("Unable to read file "+file.Name(), false, true)
		}

		// If conditional specified
		if filter.Field != "" && filter.Condition != "" && filter.Value != "" {
			// Check to make sure correct condition is provided
			if filter.Condition == "==" || filter.Condition == "<=" || filter.Condition == ">=" || filter.Condition == "!=" {
				var d map[string]interface{}
				if err := json.Unmarshal(doc.Data, &d); err != nil {
					panic(err)
				}
				// Find field
				field := d[filter.Field]
				// If field is found do comparison
				if field != nil {
					switch real := field.(type) {
					case string:
						switch filter_t := filter.Value.(type) {
						case string:
							switch filter.Condition {
							case "==":
								if real == filter_t {
									c.Documents = append(c.Documents, doc)
								}
							case "!=":
								if real != filter_t {
									c.Documents = append(c.Documents, doc)
								}
							}
						}
					case float64:
						switch filter_t := filter.Value.(type) {
						case float64:
							switch filter.Condition {
							case "==":
								if real == filter.Value {
									c.Documents = append(c.Documents, doc)
								}
							case "<=":
								if real <= filter_t {
									c.Documents = append(c.Documents, doc)
								}
							case ">=":
								if real >= filter_t {
									c.Documents = append(c.Documents, doc)
								}
							case "!=":
								if real != filter_t {
									c.Documents = append(c.Documents, doc)
								}
							case "<":
								if real < filter_t {
									c.Documents = append(c.Documents, doc)
								}
							case ">":
								if real > filter_t {
									c.Documents = append(c.Documents, doc)
								}
							}
						default:
							return c, fmt.Errorf("Filter Value is not float64. For more details: https://pkg.go.dev/encoding/json#Unmarshal")
						}
					case bool:
						switch filter_t := filter.Value.(type) {
						case bool:
							switch filter.Condition {
							case "==":
								if real == filter_t {
									c.Documents = append(c.Documents, doc)
								}
							case "!=":
								if real != filter_t {
									c.Documents = append(c.Documents, doc)
								}
							}
						}
					}
				}
			} else {
				return c, fmt.Errorf("Filter '" + filter.Condition + "' is not supported. Accepted conditions ==, <=, >=, != ")
			}
		} else {
			// append read file
			c.Documents = append(c.Documents, doc)
		}
	}

	// unmarhsal the read files as a comma delimeted byte array
	return c, nil
}

// Delete locks that database and then attempts to remove the collection/document
// specified by [path]
func (d *Driver) Delete(collection, document string) error {
	path := filepath.Join(collection, document)
	//
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	//
	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {

	// if fi is nil or error is not nil return
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find file or directory named %v\n", path)

	// remove directory and all contents
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	// remove file
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir)
	}

	return nil
}

func stat(path string) (fi os.FileInfo, err error) {

	// check for dir, if path isn't a directory check to see if it's a file
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path)
	}

	return
}

// getOrCreateMutex creates a new collection specific mutex any time a collection
// is being modfied to avoid unsafe operations
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
