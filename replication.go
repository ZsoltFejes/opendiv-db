package opendivdb

/*

Sync Idea (peer 1, 2 and 3)
	1. Peer 1 comes online and reaches out to peer 1 and 3 that it is online. Current state SYNCING
	2. Peer 1 reaches out to peer 2 and 3 to get their latest doc state
	3. Peers 2 and 3 reply with the Doc State
	4. Peer 1 will request one by one the documents where the hash matches on both peers from peer 2
		Request doc with the hash, if the hash since changed on peer 2, the assumption is that the change was pushed already by the sync process that started when peer 1 came online
	5. After peer 1 has gone through the sync process, it should have all changes it didn't have before coming online and all live changes since coming online.

	* Also a go routine is running in the background to make sure peers are online and a state sync is done regularly (every 5-10 minutes) to make sure states are indeed in sync
*/

import (
	"os"
	"time"
)

type (
	replication_host struct {
		host_address string
		state        string // OFFLINE, ONLINE, SYNCING
		last_ping    time.Time
		last_synced  time.Time
	}
	doc_state struct {
		Hash      string
		Timestamp time.Time
	}
)

// Set Document stat into memory
func (d *Driver) setDocState(collection string, doc Document) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	doc_state := doc_state{Hash: doc.Hash, Timestamp: doc.Updated_at}
	d.doc_state[collection+"/"+doc.ID] = doc_state
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

// Function to send Doc to specific node

// Function to broadcast changes to all nodes (wrap sending a doc to single node into a loop)

/*
Main Go Function to keep checking if a replica is listening (PING-PONG)
	Handle SYNC
	Do ping pong to keep peer's state up to date
	Keep checking doc state across peers
*/

// Main Sync Go Routine
func (d *Driver) runSync() {
	// Define replication endpoints (Gin)
	// Start goroutine to listen to replication requests
	// Broadcast that you are SYNCING. Peers, will reply with the doc state		<- Endpoint to create /api/sync?replication_state=SYNCING
	// Request all out of sync docs from one peer
	// Once all docs are synced, Start goroutine to get regular sync checks (every 5 minute)
	// Broadcast to all peers that you have finished syncing and are online		<- Endpoint to create /api/sync?replication_state=ONLINE
}
