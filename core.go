package opendivdb

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

	"gopkg.in/yaml.v3"
)

type (
	// Driver is what is used to interact with the scribble database. It runs
	// transactions, and provides log output
	Driver struct {
		encryption_key    []byte
		mutex             sync.Mutex
		mutexes           map[string]*sync.Mutex
		cache             cache
		dir               string // the directory where scribble will create the database
		doc_state         map[string]doc_state
		subs              map[string]*Subscription
		replication_hosts map[string]replication_host
		replication_pass  string
		replication_state string
		replication_port  int
	}

	Document struct {
		ID         string
		Collection string
		Updated_at time.Time
		From_cache bool
		Hash       string // Hash of "Data" bytes
		Data       json.RawMessage
	}

	Config struct {
		Encryption_key    string            `yaml:"encryption_key,omitempty"`    // Database encryption key must be 32 characters long for AES-256
		Salt              string            `yaml:"omitempty"`                   // Salt for encryption not included in the config file but in the binary
		Path              string            `yaml:"path,omitempty"`              // Path to the where the collections and documents will be placed
		Cache_timeout     float64           `yaml:"cache_timeout,omitempty"`     // Database cache timeout in seconds
		Cache_limit       float64           `yaml:"cache_limit,omitempty"`       // Maximum number of documents cached at a given time, when exceeded the oldest document is removed
		Replication_pass  string            `yaml:"replication_pass,omitempty"`  // Replication Password
		Replication_nodes map[string]string `yaml:"replication_nodes,omitempty"` // List of nodes that replicates the database
		Replication_port  int               `yaml:"replication_port,omitempty"`  // Port used replication
	}
)

func ValidateID(id string) error {
	if id == "" {
		return fmt.Errorf("empty value")
	} else if id == "_logs" {
		return fmt.Errorf("collection can not be called _logs")
	}

	if strings.Contains(id, "/") || strings.Contains(id, `\`) {
		return fmt.Errorf(`unsupported character, can't contain '/' or '\'`)
	}

	return nil
}

// Convert v interface object
func (d Document) DataTo(v interface{}) error {
	doc_b, err := json.Marshal(d.Data)
	if err != nil {
		return fmt.Errorf("Unable to marshal document data! " + err.Error())
	}

	return json.Unmarshal(doc_b, &v)
}

func GetMD5Hash(text []byte) string {
	hash := md5.Sum(text)
	return hex.EncodeToString(hash[:])
}

func LoadConfig(path_to_config_file string) (Config, error) {
	config := Config{Encryption_key: "", Path: "", Salt: ""}
	// Read config file located at in the same directory as the executable
	config_b, err := os.ReadFile(filepath.Join(path_to_config_file))
	// If  there was an error reading the file fall back to using environment variables
	if err != nil {
		return Config{}, fmt.Errorf("config file not found")
	}

	err = yaml.Unmarshal(config_b, &config)
	if err != nil {
		return Config{}, fmt.Errorf("unable to unmarshal configuration file")
	}

	// Check db path is specified.
	if config.Path == "" {
		return Config{}, fmt.Errorf("no database path was provided")
	}

	return config, nil
}

// New creates a new scribble database at the desired directory location, and
// returns a *Driver to then use for interacting with the database
func NewDB(config Config) (*Driver, error) {
	dir := filepath.Clean(config.Path)

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

	replication_nodes_temp := make(map[string]replication_host)
	for id, node := range config.Replication_nodes {
		replication_nodes_temp[id] = replication_host{host_address: node, state: "OFFLINE"}
	}

	// Build driver
	driver := Driver{
		encryption_key:    encryption_key[:],
		dir:               dir,
		mutexes:           make(map[string]*sync.Mutex),
		cache:             cache{timeout: cache_timeout, limit: cache_limit, documents: make(map[string]cached_doc)},
		doc_state:         make(map[string]doc_state),
		subs:              make(map[string]*Subscription),
		replication_hosts: replication_nodes_temp,
		replication_pass:  config.Replication_pass,
		replication_state: "SYNCING",
		replication_port:  config.Replication_port,
	}

	// if the database already exists, just use it
	if _, err := os.Stat(dir); err != nil {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	err := driver.loadDocState()
	if err != nil {
		return &driver, err
	}
	go driver.cache.runCachePurge()
	go driver.runReplication()

	return &driver, nil
}

func (d *Driver) Collection(name string) *Collection {
	return &Collection{collection_name: name, driver: d}
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
func (d *Driver) getOrCreateMutex(collection_document string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection_document]

	// if the mutex doesn't exist make it
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection_document] = m
	}

	return m
}
