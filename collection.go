package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Collection_ref struct {
	collection_name string
	driver          *Driver
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

	mutex := c.driver.getOrCreateMutex(c.collection_name + "/" + document)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(c.driver.dir, c.collection_name)
	fnlPath := filepath.Join(dir, document)
	tmpPath := fnlPath + ".tmp"

	// create collection directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return Document{}, err
	}
	// marshal document to JSON with tab indents
	v_b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return Document{}, err
	}
	// create document wrapping the data bytes
	doc := Document{Id: document, Data: v_b, Updated_at: time.Now(), Hash: GetMD5Hash(string(v_b[:])), FromCache: false}
	b, err := json.MarshalIndent(doc, "", "\t")
	if err != nil {
		return Document{}, err
	}

	// add the new document to cache
	err = c.driver.cache.Add(*c, doc)
	if err != nil {
		return Document{}, err
	}
	// check if encryption is enabled and encrypt entire document before writing it to disk
	if len(c.driver.encryption_key) != 0 {
		b = EncryptAES(c.driver.encryption_key, b)
	}
	// write document bytes to the disk
	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return Document{}, err
	}

	// move final file into place
	err = os.Rename(tmpPath, fnlPath)
	if err != nil {
		return Document{}, err
	}
	return doc, nil
}

// Read a document from the database or Cache
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

	// check if document exist in cache, if yes return the document from cache
	if doc, in_cache := c.driver.cache.GetDoc(c.collection_name, id); in_cache {
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

	// if encryption is enabled decrypt document bytes
	if len(c.driver.encryption_key) != 0 {
		b, err = DecryptAES(c.driver.encryption_key, b[:])
		if err != nil {
			return Document{}, fmt.Errorf(err.Error())
		}
	}
	// unmarshall bytes into Document
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
	case fi == nil:
		return nil

	case err != nil:
		return fmt.Errorf(err.Error())

	// remove directory and all contents
	case fi.Mode().IsDir():
		// read all the files in the transaction.Collection;
		files, _ := os.ReadDir(dir)
		// Loop through each file to delete it from cache
		for _, file := range files {
			c.driver.cache.Delete(c.collection_name, file.Name())
		}
		return os.RemoveAll(dir)

	// remove file
	case fi.Mode().IsRegular():
		c.driver.cache.Delete(c.collection_name, id)
		return os.RemoveAll(dir)
	}

	return nil
}

// Creates Filter object so do simple queries
func (c *Collection_ref) Where(field string, operator string, value any) *Filter {
	return &Filter{collection: c, driver: c.driver, field: field, operator: operator, value: value}
}
