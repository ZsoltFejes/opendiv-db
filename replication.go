package opendivdb

import (
	"os"
)

// Set Document stat into memory
func (d *Driver) setDocState(collection string, doc Document) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.doc_state[collection+"/"+doc.Id] = doc.Hash
}

// Remove a document state from memory
func (d *Driver) removeDocState(collection string, id string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.doc_state, collection+"/"+id)
}

// Load each document's current state into the memory
func (d *Driver) loadDocState() error {
	// Get all collection names
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return nil
	}
	// For each collection
	for _, dir := range entries {
		if dir.IsDir() {
			col, err := d.Collection(dir.Name()).Documents()
			if err != nil {
				return err
			}
			for _, doc := range col {
				d.setDocState(dir.Name(), doc)
			}
		}
	}
	return nil
}

// Sync function TODOs
//	Struct for database logs; set, delete
//	Struct for a wrapper for sync
//	Function to create log
//		Make sure "_logs" directory is blocked for collection names
//
//	Function to compile the collection of documents that other node needs to sync (do 10 at a time; max 100 MB per sync)
//	Go Function to keep checking if a replica is listening
