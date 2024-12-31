package opendivdb

import (
	"sync"
	"time"
)

type (
	cache struct {
		documents map[string]cached_doc //Cached documents
		timeout   time.Duration         // Cache timeout in seconds
		limit     float64               // Maximum number of cached documents
		mutex     sync.Mutex
	}

	cached_doc struct {
		cached_at time.Time
		document  Document
	}
)

// Must be run as a go routine. Runs an infinite loop to check the cache every 5 seconds to deletes expired cache
func (c *cache) runCachePurge() {
	for {
		c.mutex.Lock()
		for id, value := range c.documents {
			if value.cached_at.Add(c.timeout).Before(time.Now()) {
				delete(c.documents, id)
			}
		}
		c.mutex.Unlock()
		time.Sleep(time.Second * 1)
	}
}

func (c *cache) add(coll_ref Collection, doc Document) {
	// Obtain Mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check how many documents are in cache
	num_of_cached_docs := len(c.documents)

	// If there are more or equal to the cache limit
	if num_of_cached_docs >= int(c.limit) {
		oldest_doc := cached_doc{}

		// Loop through all cached documents and find the one that was cached the longest
		for _, doc := range c.documents {
			if oldest_doc.cached_at.IsZero() {
				oldest_doc = doc
			} else {
				if doc.cached_at.Before(oldest_doc.cached_at) {
					oldest_doc = doc
				}
			}
		}
		c.delete(coll_ref.collection_name, oldest_doc.document.ID)
	}

	doc.From_cache = true
	cached_doc := cached_doc{cached_at: time.Now(), document: doc}
	c.documents[coll_ref.collection_name+"/"+doc.ID] = cached_doc
}

func (c *cache) getDoc(collection_name string, document_id string) (Document, bool) {
	if val, ok := c.documents[collection_name+"/"+document_id]; ok {
		val.cached_at = time.Now()
		c.documents[collection_name+"/"+document_id] = val
		return val.document, true
	}
	return Document{}, false
}

// Delete Document from Cache
func (c *cache) delete(collection_name string, document_id string) {
	delete(c.documents, collection_name+"/"+document_id)
}
