package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

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

func main() {
	// Set working directory
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	WORKDIR = filepath.Dir(ex)

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
			l("Unable to unmarshal configuration file! Make sure it is a valid json file and the values are correct. Have a look at README.md for the example configuration.", true, true)
		}
	}
	// Check db path is specified.
	if config.DB_path == "" {
		l("No Database path was provided! Make sure db_config.json is in the same directory as the executable or 'OPENDIV_DB_PATH' environment variable is set.", true, true)
	}

	// Check Encryption key length
	if len(config.Encryption_key) > 0 && len(config.Encryption_key) != 32 {
		l("Encryption key length must be 32 characters!", true, true)
	}

	// Create database driver
	DB, err = NewDB(config.DB_path, config)
	if err != nil {
		l("Unable to create DB! "+err.Error(), true, true)
	}
	go DB.Cache.RunCachePurge()

	// Testing //
	start := time.Now()

	err = DB.Collection("Test").Delete("")
	if err != nil {
		print(err.Error())
	}

	test1 := make(map[string]interface{})
	test1["Name"] = "test1"
	test1["Number"] = 1
	_, err = DB.Collection("Test").Add(test1)
	if err != nil {
		print(err.Error())
	}

	test2 := make(map[string]interface{})
	test2["Name"] = "test2"
	test2["Number"] = 2
	_, err = DB.Collection("Test").Add(test2)
	if err != nil {
		print(err.Error())
	}

	test3 := make(map[string]interface{})
	test3["Name"] = "test3"
	test3["Number"] = 3
	_, err = DB.Collection("Test").Add(test3)
	if err != nil {
		print(err.Error())
	}

	test4 := make(map[string]interface{})
	test4["Name"] = "test4"
	test4["Number"] = 4
	_, err = DB.Collection("Test").Add(test4)
	if err != nil {
		print(err.Error())
	}

	col, err := DB.Collection("Test").Where("Name", "==", "test1").Documents()
	if err != nil {
		print(err.Error())
	}
	for _, doc := range col {
		var data map[string]interface{}
		doc.DataTo(&data)
		fmt.Println(data)
	}
	end := time.Now()
	fmt.Println(end.Sub(start))

	fmt.Println(config)
}
