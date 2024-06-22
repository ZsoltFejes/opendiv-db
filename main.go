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
	WORKDIR string
	debug   = flag.Bool("debug", false, "Set process to debug")
	DB      *Driver
)

type Config struct {
	Encryption_key string  `yaml:"encryption_key,omitempty"`
	DB_path        string  `yaml:"db_path,omitempty"`
	Cache_timeout  float64 `yaml:"cache_timeout,omitempty"`
	Cache_limit    float64 `yaml:"cache_limit,omitempty"`
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
	config_b, err := os.ReadFile(filepath.Join(WORKDIR, "db_config.yml"))
	config := Config{Encryption_key: "", DB_path: ""}
	// If  there was an error reading the file fall back to using enviornment variables.
	if err != nil {
		l("Unable to read config.json file. Using Environment variables.", false, true)
		config.Encryption_key = os.Getenv("OPENDIV_DB_ENCRYPTION_KEY")
		config.DB_path = os.Getenv("OPENDIV_DB_PATH")
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
	if config.DB_path == "" {
		return Config{}, fmt.Errorf("no database path was provided")
	}

	// Check Encryption key length
	if len(config.Encryption_key) > 0 && len(config.Encryption_key) != 32 {
		return Config{}, fmt.Errorf("encryption key length must be 32 characters")
	}
	return config, nil
}

func main() {
	// Set working directory
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	WORKDIR = filepath.Dir(ex)

	config, err := LoadConfig()
	if err != nil {
		l(err.Error(), true, true)
	}

	// Create database driver
	DB, err = NewDB(config.DB_path, config)
	if err != nil {
		l("Unable to create DB! "+err.Error(), true, true)
	}
	go DB.Cache.RunCachePurge()

	// Add infinite loop or actions
}
