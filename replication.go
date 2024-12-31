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
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
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

	error_response struct {
		Error string `json:"error,omitempty"`
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

func (d *Driver) getDocStateAfter(timestamp time.Time) map[string]doc_state {
	doc_state_temp := make(map[string]doc_state)
	for id, doc := range d.doc_state {
		if doc.Timestamp.After(timestamp) {
			doc_state_temp[id] = doc
		}
	}
	return doc_state_temp
}

// Create Authentication middleware to check for replication pass
func (d *Driver) checkReplicationPass(c *gin.Context) {
	pass := c.GetHeader("Authorization")
	if pass != d.replication_pass {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	c.Next()
}

// URL ARGS: state=SYNCING or replication_state=ONLINE
func (d *Driver) syncEndpoint(c *gin.Context) {
	// check url args for new state
	new_state := c.Query("state")
	if new_state == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'state' was not provided"})
		return
	}
	replication_id := c.Query("id")
	if replication_id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'id' was not provided"})
		return
	}
	d.mutex.Lock()

	// Prep new state
	host_state := d.replication_hosts[replication_id]
	host_state.state = new_state
	host_state.last_synced = time.Now() // Time is set before replying to the node

	var (
		response_status int
		response        any
	)
	// reply with latest state depending on state type
	switch new_state {
	case "SYNC":
		response_status = http.StatusOK
		response = d.doc_state
	case "ONLINE":
		response_status = http.StatusOK
		response = d.getDocStateAfter(d.replication_hosts[replication_id].last_synced)
	default:
		response_status = http.StatusBadRequest
		response = gin.H{"error": "state '" + new_state + "' not supported"}
	}

	// Save new state
	d.replication_hosts[replication_id] = host_state
	// Relese mutex before sending response
	d.mutex.Unlock()

	c.JSON(response_status, response)
}

// URL ARGS: collection=test,document_id=docID,hash=docHash
func (d *Driver) docEndpoint(c *gin.Context) {
	collection := c.Query("collection")
	if collection == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'collection' was not provided"})
		return
	}
	document_id := c.Query("document_id")
	if document_id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'document_id' was not provided"})
		return
	}
	hash := c.Query("hash")

	if hash != d.doc_state[collection+"/"+document_id].Hash {
		doc, err := d.Collection(collection).Document(document_id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, doc)
	}
}

// URL ARGS collection, and document_id
func (d *Driver) docUpdateEndpoint(c *gin.Context) {
	// Check url arguments
	collection := c.Query("collection")
	if collection == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'collection' was not provided"})
		return
	}
	document_id := c.Query("document_id")
	if document_id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'document_id' was not provided"})
		return
	}
	// Unmarshal doc from request
	doc := Document{}
	err := c.ShouldBindJSON(&doc)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}

	// Save replicated file to local file system
	if err = d.Collection(collection).write(document_id, doc); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

// Function to send Doc to specific node
func (d *Driver) sendDocToNode(node string, collection string, document Document) error {
	client := http.Client{}
	// Marshal document to json and add it to bellow request
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/sync/doc?collection=%s,document_id=%s", d.replication_hosts[node].host_address, collection, document.ID), nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", d.replication_pass)
	res, err := client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		//response := error_response{}
		// Unmarshal response to error

		// Update bellow error message
		return fmt.Errorf(fmt.Sprintf("following error occurred while pushing change to node '%s' - ", node))
	}

	return nil
}

// Function to broadcast changes to all nodes (wrap sending a doc to single node into a loop)
func (d *Driver) sendDocToAllNodes(collection string, doc Document) {
	for id, node := range d.replication_hosts {
		if node.state == "ONLINE" {
			if err := d.sendDocToNode(id, collection, doc); err != nil {
				fmt.Println("[error] " + err.Error())
			}
		}
	}
}

// Main Sync Go Routine
func (d *Driver) runReplication() {
	// Define replication endpoints (Gin)
	gin.ForceConsoleColor()
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	sync := r.Group("/api/sync")
	sync.Use(d.checkReplicationPass)
	{
		r.GET("", d.syncEndpoint)
		r.GET("/doc", d.docEndpoint)
		r.POST("/doc", d.docUpdateEndpoint)
	}

	// Start goroutine to listen to replication requests
	r.Run(":" + strconv.Itoa(d.replication_port))

	// Broadcast that you are SYNCING. Peers, will reply with the doc state		<- Endpoint to create /api/sync?state=SYNCING
	// Request all out of sync docs from one peer
	// Once all docs are synced, Start goroutine to get regular sync checks (every 5 minute)
	// Broadcast to all peers that you have finished syncing and are online		<- Endpoint to create /api/sync?state=ONLINE
}
