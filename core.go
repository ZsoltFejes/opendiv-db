package opendivdb

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
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
		doc_state      map[string]string
		subs           map[string]*Subscription
	}

	Document struct {
		ID         string
		Updated_at time.Time
		FromCache  bool
		Hash       string // Hash of "Data" bytes
		Data       json.RawMessage
	}

	Config struct {
		Encryption_key string  `yaml:"encryption_key,omitempty"` // Database encryption key must be 32 characters long for AES-256
		Salt           string  `yaml:"omitempty"`                // Salt for encryption not included in the config file but in the binary
		Path           string  `yaml:"path,omitempty"`           // Path to the where the collections and documents will be placed
		Cache_timeout  float64 `yaml:"cache_timeout,omitempty"`  // Database cache timeout in seconds
		Cache_limit    float64 `yaml:"cache_limit,omitempty"`    // Maximum number of documents cached at a given time, when exceeded the oldest document is removed
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
		config.Encryption_key = os.Getenv("OPENDIV_DB_ENCRYPTION_KEY")
		config.Path = os.Getenv("OPENDIV_DB_PATH")
		config.Cache_limit, err = strconv.ParseFloat(os.Getenv("OPENDIV_DB_CACHE_LIMIT"), 64)
		if err != nil {
			config.Cache_limit = 0
		}
		timeout, err := strconv.ParseFloat(os.Getenv("OPENDIV_DB_CACHE_TIMEOUT"), 64)
		if err != nil {
			config.Cache_timeout = 0
		}
		config.Cache_timeout = timeout
	} else {
		err = yaml.Unmarshal(config_b, &config)
		if err != nil {
			return Config{}, fmt.Errorf("unable to unmarshal configuration file")
		}
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

	// Build driver
	driver := Driver{
		encryption_key: encryption_key[:],
		dir:            dir,
		mutexes:        make(map[string]*sync.Mutex),
		cache:          Cache{Timeout: cache_timeout, Limit: cache_limit, documents: make(map[string]Cached_Doc)},
		doc_state:      make(map[string]string),
		subs:           make(map[string]*Subscription),
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
