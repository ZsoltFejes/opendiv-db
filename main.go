package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

var (
	debug = flag.Bool("debug", false, "Set process to debug")
	DB    *Driver
	Salt  string
)

type Config struct {
	Encryption_key string  `yaml:"encryption_key,omitempty"` // Database encryption key must be 32 characters long for AES-256
	Salt           string  `yaml:"omitempty"`                // Salt for encryption not included in the config file but in the binary
	Path           string  `yaml:"path,omitempty"`           // Path to the where the collections and documents will be placed
	Cache_timeout  float64 `yaml:"cache_timeout,omitempty"`  // Database cache timeout in seconds
	Cache_limit    float64 `yaml:"cache_limit,omitempty"`    // Maximum number of documents cached at a given time, when exceeded the oldest document is removed
}

func l(message string, fatal bool, public bool) {
	if (public || *debug) && !fatal {
		log.Println(message)
	} else if fatal {
		log.Fatalln(message)
	}
}

func LoadConfig() (Config, error) {
	// Read config file located at in the same directory as the executable
	config_b, err := os.ReadFile(filepath.Join("db_config.yml"))
	config := Config{Encryption_key: "", Path: "", Salt: ""}
	// If  there was an error reading the file fall back to using environment variables
	if err != nil {
		l("Unable to read config.json file. Using Environment variables.", false, true)
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

func main() {
	config, err := LoadConfig()
	if err != nil {
		l(err.Error(), true, true)
	}

	config.Salt = Salt
	// Create database driver
	DB, err = NewDB(config.Path, config)
	if err != nil {
		l("Unable to create DB! "+err.Error(), true, true)
	}
	go DB.RunCachePurge()

	// Add infinite loop or actions
	fmt.Println(Salt)
}
