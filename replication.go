package opendivdb

/*
Sync function TODOs
	Function to send Doc to specific node
	Function to broadcast changes to all nodes
	Go Function to keep checking if a replica is listening (PING-PONG)
		Keep the peer's state in memory
		Keep checking doc state across peers
	Function to handle requests coming from remote to sync

Sync Idea (peer 1, 2 and 3)
	1. Peer 1 comes online and reaches out to peer 1 and 3 that it is online and should receive now live updates. Current state SYNCING
	2. Peer 1 reaches out to peer 2 and 3 to get their latest doc state
	3. Peers 2 and 3 reply with the Doc State
	4. Peer 1 will request one by one the documents where the hash matches on both peers from peer 2
		Request doc with the hash, if the hash since changed on peer 2, the assumption is that the change was pushed already by the sync process that started when peer 1 came online
	5. After peer 1 has gone through the sync process, it should have all changes it didn't have before coming online and all live changes since coming online.

	* Also a go routine is running in the background to make sure peers are online and a state sync is done regularly (every 5-10 minutes) to make sure states are indeed in sync
*/

import (
	"os"
)

// Set Document stat into memory
func (d *Driver) setDocState(collection string, doc Document) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.doc_state[collection+"/"+doc.ID] = doc.Hash
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
