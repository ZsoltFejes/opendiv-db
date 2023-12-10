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
)

type Config struct {
	Encryption_key string `json:"config.Encryption_key,omitempty"`
}

type Dog struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
	Age  int    `json:"age,omitempty"`
}

func main() {
	flag.Parse()
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

	db, err := NewDB("db")
	if err != nil {
		l("Unable to create DB! "+err.Error(), true, true)
	}

	liza := Dog{Name: "Liza", Age: 2, Type: "Weiner"}
	db.Write("Dogs", "Liza", liza, config.Encryption_key)

	buksi := Dog{Name: "Buksi", Age: 8, Type: "German Shepard"}
	db.Write("Dogs", "Buksi", buksi, config.Encryption_key)

	liza_read := Dog{}
	err = db.Read("Dogs", "Liza", &liza_read, config.Encryption_key)
	if err != nil {
		l("Unable to marshall data! "+err.Error(), false, true)
	}
	fmt.Println(liza_read)

	// Read all fish from the database, unmarshaling the response.
	records, err := db.ReadAll("Dogs", config.Encryption_key)
	if err != nil {
		l("Error"+err.Error(), true, true)
	}

	dogs := []Dog{}
	for _, f := range records {
		dogFound := Dog{}
		if err := json.Unmarshal([]byte(f), &dogFound); err != nil {
			fmt.Println("Error", err)
		}
		dogs = append(dogs, dogFound)
	}
	fmt.Println(dogs)

}
