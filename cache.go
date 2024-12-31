package opendivdb

import (
	"sync"
	"time"
)

type (
	Cache struct {
		documents map[string]Cached_Doc //Cached documents
		Timeout   time.Duration         // Cache timeout in seconds
		Limit     float64               // Maximum number of cached documents
		mutex     sync.Mutex
	}

	Cached_Doc struct {
		Cached_at time.Time
		Document  Document
	}
)

// Must be run as a go routine. Runs an infinite loop to check the cache every 5 seconds to deletes expired cache
func (c *Cache) runCachePurge() {
	for {
		c.mutex.Lock()
		for id, value := range c.documents {
			if value.Cached_at.Add(c.Timeout).Before(time.Now()) {
				delete(c.documents, id)
			}
		}
		c.mutex.Unlock()
		time.Sleep(time.Second * 1)
	}
}

func (c *Cache) Add(coll_ref Collection, doc Document) error {
	// Obtain Mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check how many documents are in cache
	num_of_cached_docs := len(c.documents)

	// If there are more or equal to the cache limit
	if num_of_cached_docs >= int(c.Limit) {
		oldest_doc := Cached_Doc{}

		// Loop through all cached documents and find the one that was cached the longest
		for _, doc := range c.documents {
			if oldest_doc.Cached_at.IsZero() {
				oldest_doc = doc
			} else {
				if doc.Cached_at.Before(oldest_doc.Cached_at) {
					oldest_doc = doc
				}
			}
		}
		c.Delete(coll_ref.collection_name, oldest_doc.Document.ID)
	}

	doc.FromCache = true
	cached_doc := Cached_Doc{Cached_at: time.Now(), Document: doc}
	c.documents[coll_ref.collection_name+"/"+doc.ID] = cached_doc
	return nil
}

func (c *Cache) GetDoc(collection_name string, document_id string) (Document, bool) {
	if val, ok := c.documents[collection_name+"/"+document_id]; ok {
		val.Cached_at = time.Now()
		c.documents[collection_name+"/"+document_id] = val
		return val.Document, true
	}
	return Document{}, false
}

// Delete Document from Cache
func (c *Cache) Delete(collection_name string, document_id string) {
	delete(c.documents, collection_name+"/"+document_id)
}
