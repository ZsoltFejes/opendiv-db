package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type (

	// Driver is what is used to interact with the scribble database. It runs
	// transactions, and provides log output
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string // the directory where scribble will create the database
	}
)

func l(message string, fatal bool, public bool) {
	if (public || *debug) && !fatal {
		log.Println(message)
	} else if fatal {
		log.Fatalln(message)
	}
}

// New creates a new scribble database at the desired directory location, and
// returns a *Driver to then use for interacting with the database
func NewDB(dir string) (*Driver, error) {

	//
	dir = filepath.Clean(dir)

	//
	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
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
// the [collection] specified with the [resource] name given
func (d *Driver) Write(collection, resource string, v interface{}, encryption_key string) error {

	// ensure there is a place to save record
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	// ensure there is a resource (name) to save record as
	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record (no name)!")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	//
	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource)
	tmpPath := fnlPath + ".tmp"

	// create collection directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	//
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	if encryption_key != "" {
		ct := EncryptAES(encryption_key, b)
		// write marshaled data to the temp file
		if err := os.WriteFile(tmpPath, ct, 0644); err != nil {
			return err
		}
	} else {
		// write marshaled data to the temp file
		if err := os.WriteFile(tmpPath, b, 0644); err != nil {
			return err
		}
	}

	// move final file into place
	return os.Rename(tmpPath, fnlPath)
}

// Read a record from the database
func (d *Driver) Read(collection, resource string, v interface{}, encryption_key string) error {

	// ensure there is a place to save record
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	// ensure there is a resource (name) to save record as
	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record (no name)!")
	}

	//
	record := filepath.Join(d.dir, collection, resource)

	// check to see if file exists
	if _, err := stat(record); err != nil {
		return err
	}

	// read record from database
	b, err := os.ReadFile(record)
	if err != nil {
		return err
	}

	if encryption_key != "" {
		b = DecryptAES(encryption_key, b[:])
	}

	return json.Unmarshal(b, &v)
}

// ReadAll records from a collection; this is returned as a slice of strings because
// there is no way of knowing what type the record is.
func (d *Driver) ReadAll(collection string, encryption_key string) ([]string, error) {

	// ensure there is a collection to read
	if collection == "" {
		return nil, fmt.Errorf("Missing collection - unable to record location!")
	}

	//
	dir := filepath.Join(d.dir, collection)

	// check to see if collection (directory) exists
	if _, err := stat(dir); err != nil {
		return nil, err
	}

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, _ := os.ReadDir(dir)

	// the files read from the database
	var records []string

	// iterate over each of the files, attempting to read the file. If successful
	// append the files to the collection of read files
	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		if encryption_key != "" {
			ds := DecryptAES(encryption_key, b[:])
			b = []byte(ds)
		}

		// append read file
		records = append(records, string(b))
	}

	// unmarhsal the read files as a comma delimeted byte array
	return records, nil
}

// Delete locks that database and then attempts to remove the collection/resource
// specified by [path]
func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(collection, resource)
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
