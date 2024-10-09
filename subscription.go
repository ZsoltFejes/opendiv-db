package opendivdb

import (
	"fmt"

	"github.com/google/uuid"
)

// Subscription struct, either Collection or Filter needs to be specified
type Subscription struct {
	driver     *Driver
	id         string
	collection *Collection
	Channel    chan []Document
}

// // Create new subscription for the entire collection
func (c *Collection) Subscribe() (*Subscription, error) {
	channel := make(chan []Document)

	sub := Subscription{
		driver:     c.driver,
		id:         uuid.NewString(),
		collection: c,
		Channel:    channel,
	}

	c.driver.mutex.Lock()
	defer c.driver.mutex.Unlock()

	c.driver.subs[sub.id] = &sub
	go sub.push()
	return &sub, nil
}

func (d *Driver) checkSubscriptionPush(collection_name string, doc Document) {
	// Loop through each subscription
	for _, sub := range d.subs {
		// If the subscription's collection name doesn't matches the document's collection name carry on
		if sub.collection.collection_name != collection_name {
			continue
		}
		// If the subscription has a filter, need to check if this document is included
		if sub.collection.filter.field != "" {
			// Check if doc is included in the filter
			include, err := sub.collection.filter.included(doc)
			if err != nil {
				fmt.Println("[ERROR] unable to check if document should trigger a subscription push " + err.Error())
			}
			if !include {
				continue
			}
		}
		go sub.push()
	}
}

func (s *Subscription) push() {
	col, err := s.collection.Documents()
	if err != nil {
		fmt.Println("[ERROR] unable to retrieve documents for subscription push " + err.Error())
	}
	s.Channel <- col
}

func (s *Subscription) Unsubscribe() {
	close(s.Channel)
	s.driver.mutex.Lock()
	defer s.driver.mutex.Unlock()
	delete(s.driver.subs, s.id)
}
