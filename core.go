package main

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type (
	// Driver is what is used to interact with the scribble database. It runs
	// transactions, and provides log output
	Driver struct {
		encryption_key []byte
		mutex          sync.Mutex
		mutexes        map[string]*sync.Mutex
		cache          Cache
		dir            string // the directory where scribble will create the database
	}

	Document struct {
		Id         string
		Updated_at time.Time
		FromCache  bool
		Hash       string // Hash of "Data" bytes
		Data       json.RawMessage
	}
)

func ValidateID(id string) error {
	if id == "" {
		return fmt.Errorf("empty value")
	}

	if strings.Contains(id, "/") || strings.Contains(id, `\`) {
		return fmt.Errorf(`unsupported character, can't contain '/' or '\'`)
	}
	return nil
}

func (d Document) DataTo(v interface{}) error {
	doc_b, err := json.Marshal(d.Data)
	if err != nil {
		return fmt.Errorf("Unable to marshal document data! " + err.Error())
	}

	return json.Unmarshal(doc_b, &v)
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

// New creates a new scribble database at the desired directory location, and
// returns a *Driver to then use for interacting with the database
func NewDB(dir string, config Config) (*Driver, error) {
	dir = filepath.Clean(dir)

	// Check for timeout, if not set by user set default
	var cache_limit float64
	if config.Cache_limit == 0 {
		cache_limit = 1000
	} else {
		cache_limit = config.Cache_limit
	}

	// Check for limit, if not set by user set default
	cache_timeout := time.Second * time.Duration(config.Cache_timeout)
	if cache_timeout == 0 {
		cache_timeout = time.Duration(time.Minute * 5)
	}

	// hash encryption key to SHA256
	var encryption_key []byte
	if config.Encryption_key != "" || config.Salt != "" {
		var hash [32]byte = sha256.Sum256([]byte(config.Encryption_key + config.Salt))
		encryption_key = hash[:]
	}

	// Build driver
	driver := Driver{
		encryption_key: encryption_key[:],
		dir:            dir,
		mutexes:        make(map[string]*sync.Mutex),
		cache:          Cache{Timeout: cache_timeout, Limit: cache_limit, documents: make(map[string]Cached_Doc)},
	}

	// if the database already exists, just use it
	if _, err := os.Stat(dir); err == nil {
		//l("Using '"+dir+"' (database already exists)", false, true)
		return &driver, nil
	}

	// if the database doesn't exist create it
	//l("Creating database at '"+dir+"'...", false, true)
	return &driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) RunCachePurge() {
	d.cache.RunCachePurge()
}

func (d *Driver) Collection(name string) *Collection_ref {
	return &Collection_ref{collection_name: name, driver: d}
}

func stat(path string) (fi os.FileInfo, err error) {

	// check for dir, if path isn't a directory check to see if it's a file
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path)
	}

	return
}

// getOrCreateMutex creates a new collection specific mutex any time a collection
// is being modified to avoid unsafe operations
func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]

	// if the mutex doesn't exist make it
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}
