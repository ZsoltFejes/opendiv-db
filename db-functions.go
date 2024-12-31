package opendivdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Collection struct {
	collection_name string
	driver          *Driver
	filter          Filter
}

// Write locks the database and attempts to write the record to the database under
// the [collection] specified with the random document name (UUID). Name is added to document under [ID]
func (c *Collection) Add(v interface{}) (Document, error) {
	new_id := uuid.NewString()
	return c.Write(new_id, v)
}

// Write locks the database and attempts to write the record to the database under
// the [collection] specified with the [document] name given
func (c *Collection) Write(document string, v interface{}) (Document, error) {
	err := ValidateID(c.collection_name)
	if err != nil {
		return Document{}, fmt.Errorf(`collection name validation error - ` + err.Error())
	}

	// ensure there is a document (name) to save record as
	err = ValidateID(document)
	if err != nil {
		return Document{}, fmt.Errorf(`document ID validation error - ` + err.Error())
	}

	// marshal document to JSON with tab indents
	v_b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return Document{}, err
	}
	// create document wrapping the data bytes
	doc := Document{ID: document, Data: v_b, Updated_at: time.Now(), Hash: GetMD5Hash(v_b), From_cache: false}
	// Write document to disk
	err = c.write(document, doc)
	if err != nil {
		return doc, err
	}
	go c.driver.sendDocToAllNodes(c.collection_name, doc)

	return doc, nil
}

// Internal function to write a document into collection
func (c *Collection) write(document_id string, doc Document) error {
	mutex := c.driver.getOrCreateMutex(c.collection_name + "/" + document_id)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(c.driver.dir, c.collection_name)
	fnlPath := filepath.Join(dir, document_id)
	tmpPath := fnlPath + ".tmp"

	// create collection directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(doc, "", "\t")
	if err != nil {
		return err
	}

	// check if encryption is enabled and encrypt entire document before writing it to disk
	if len(c.driver.encryption_key) != 0 {
		b, err = EncryptAES(c.driver.encryption_key, b)
		if err != nil {
			return err
		}
	}
	// write document bytes to the disk
	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	// move final file into place
	err = os.Rename(tmpPath, fnlPath)
	if err != nil {
		return err
	}

	// add the new document to cache
	c.driver.cache.add(*c, doc)
	// Update in memory document state
	c.driver.setDocState(c.collection_name, doc)
	// Push change to subscribers
	go c.driver.checkSubscriptionPush(c.collection_name, doc)

	return nil
}

// Read a document from the database or Cache
func (c *Collection) Document(id string) (Document, error) {
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

	return c.read(id)
}

// Internal function to read document from collection
func (c *Collection) read(id string) (Document, error) {
	// check if document exist in cache, if yes return the document from cache
	if doc, in_cache := c.driver.cache.getDoc(c.collection_name, id); in_cache {
		return doc, nil
	}

	// check to see if collection (directory) exists
	dir := filepath.Join(c.driver.dir, c.collection_name)
	if _, err := stat(dir); err != nil {
		return Document{}, nil
	}

	// Check to see if file exists
	record := filepath.Join(c.driver.dir, c.collection_name, id)
	if _, err := stat(record); err != nil {
		return Document{}, fmt.Errorf("document '" + id + "' doesn't exist in '" + c.collection_name + "'")
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
			return Document{}, err
		}
	}
	// unmarshall bytes into Document
	doc := Document{}
	err = json.Unmarshal(b, &doc)
	if err != nil {
		return Document{}, err
	}

	// Add document to cache
	c.driver.cache.add(*c, doc)

	return doc, nil
}

// ReadAll documents from a collection; this is returned as a Collection.
func (c *Collection) Documents() ([]Document, error) {
	// Check if filter is specified, use filtered function
	if c.filter.field != "" {
		return c.filteredDocuments()
	} else {
		return c.allDocuments()
	}
}

func (c *Collection) allDocuments() ([]Document, error) {
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
		return col, nil
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
	return col, nil
}

// Delete locks that database and then attempts to remove the collection/document
// specified by [path]
func (c *Collection) Delete(id string) error {
	err := ValidateID(c.collection_name)
	if err != nil {
		return fmt.Errorf(`collection name validation error - ` + err.Error())
	}

	// ensure there is a document (name) to save record as
	err = ValidateID(id)
	if err != nil {
		return fmt.Errorf(`document ID validation error - ` + err.Error())
	}

	path := filepath.Join(c.collection_name, id)
	mutex := c.driver.getOrCreateMutex(c.collection_name + "/" + id)
	mutex.Lock()
	defer mutex.Unlock()

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
		return fmt.Errorf("deletion of entire collection is not allowed")

	// remove file
	case fi.Mode().IsRegular():
		doc, err := c.Document(id)
		if err != nil {
			return fmt.Errorf("unable to retrieve document for subscription push check " + err.Error())
		}
		err = os.RemoveAll(dir)
		if err != nil {
			return fmt.Errorf("unable to delete document from OS " + err.Error())
		}
		c.driver.cache.delete(c.collection_name, id)
		c.driver.removeDocState(c.collection_name, id)
		go c.driver.checkSubscriptionPush(c.collection_name, doc)
		return nil
	}

	return nil
}

// Creates Filter object so do simple queries
func (c *Collection) Where(field string, operator string, value any) *Collection {
	c.filter = Filter{field: field, operator: operator, value: value}
	return c
}
