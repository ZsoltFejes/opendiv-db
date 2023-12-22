package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

var (
	WORKDIR string
	debug   = flag.Bool("debug", false, "Set process to debug")
	DB      *Driver
)

type Config struct {
	Encryption_key string `json:"encryption_key,omitempty"`
	DB_Path        string `json:"db_path,omitempty"`
}

func main() {
	// Set working directory
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	WORKDIR = filepath.Dir(ex)

	// Read Config
	config_b, err := os.ReadFile(filepath.Join(WORKDIR, "config.json"))
	config := Config{}
	if err != nil {
		l("Unable to read config.json file", true, true)
	}
	err = json.Unmarshal(config_b, &config)
	if err != nil {
		l("Unable to unmarshal config file", true, true)
	}

	// Check Encryption key length
	if len(config.Encryption_key) > 0 && len(config.Encryption_key) != 32 {
		l("Encryption key length must be 32 characters!", true, true)
	}

	DB, err = NewDB(config.DB_Path, config)
	if err != nil {
		l("Unable to create DB! "+err.Error(), true, true)
	}

	// Test start
	test_data := make(map[string]interface{})
	test_data["Test"] = "test"
	doc, err := DB.Add("Test", test_data)
	fmt.Println(doc.Id)

}
