package opendivdb

import (
	"fmt"

	"github.com/google/uuid"
)

// Subscription struct, either Collection or Filter needs to be specified
type (
	Subscription struct {
		driver       *Driver
		id           string
		collection   *Collection
		channel      chan Snapshot
		unsubscribed bool
	}

	Snapshot struct {
		Data  []Document
		Error error
	}
)

// // Create new subscription for the entire collection
func (c *Collection) Subscribe() (*Subscription, error) {
	channel := make(chan Snapshot)

	sub := Subscription{
		driver:       c.driver,
		id:           uuid.NewString(),
		collection:   c,
		channel:      channel,
		unsubscribed: false,
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
	if s.unsubscribed {
		return
	}
	snapshot := Snapshot{}
	col, err := s.collection.Documents()
	if err != nil {
		snapshot.Error = fmt.Errorf("unable to retrieve documents " + err.Error())
	}
	snapshot.Data = col
	s.channel <- snapshot
}

func (s *Subscription) Unsubscribe() {
	s.unsubscribed = true
	close(s.channel)
	s.driver.mutex.Lock()
	defer s.driver.mutex.Unlock()
	delete(s.driver.subs, s.id)
}

func (s *Subscription) Next() Snapshot {
	for {
		select {
		case snap, ok := <-s.channel:
			if !ok {
				return Snapshot{Error: fmt.Errorf("subscription has been closed")}
			}
			return snap
		default:
			if s.unsubscribed {
				return Snapshot{Error: fmt.Errorf("subscription has been closed")}
			}
		}
	}
}
