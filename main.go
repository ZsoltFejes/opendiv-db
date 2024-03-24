package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
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
	// TODO Check if enviornment variables are defined, use them
	// TODO If no then read config file
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
	col, err := DB.Collection("Test").Where("Name", "==", "test1").Documents()
	if err != nil {
		print(err.Error())
	}
	for _, doc := range col.Documents {
		var data map[string]interface{}
		doc.DataTo(&data)
		fmt.Println(data)
	}
	end := time.Now()
	fmt.Println(end.Sub(start))
}
