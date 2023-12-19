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
	Encryption_key string `json:"encryption_key,omitempty"`
	DB_Path        string `json:"db_path,omitempty"`
}

type Dog struct {
	Name string
	Type string
	Age  int
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

	db, err := NewDB(config.DB_Path, config)
	if err != nil {
		l("Unable to create DB! "+err.Error(), true, true)
	}

	//////////
	// TEST //
	//////////
	// Create test documents
	liza := Dog{Name: "Liza", Age: 2, Type: "Weiner"}
	err = db.Write("Dogs", "Liza", liza)
	if err != nil {
		l("Unable to create Liza record! "+err.Error(), false, true)
	}

	buksi := Dog{Name: "Buksi", Age: 8, Type: "German Shepard"}
	err = db.Write("Dogs", "Buksi", buksi)
	if err != nil {
		l("Unable to create Buksi record! "+err.Error(), false, true)
	}

	liza_read := Dog{}
	doc, err := db.Read("Dogs", "Liza")
	if err != nil {
		l("Unable to marshall data! "+err.Error(), false, true)
	}
	err = doc.DataTo(&liza_read)
	if err != nil {
		l("Unable to convert document to object! "+err.Error(), false, true)
	}
	fmt.Println(liza_read)

	// Read all dogs from the database.
	records, err := db.ReadAll("Dogs")
	if err != nil {
		l("Error"+err.Error(), true, true)
	}

	dogs := []Dog{}
	for _, f := range records {
		dogFound := Dog{}
		err := f.DataTo(&dogFound)
		if err != nil {
			l("Unable to convert data to Dog! "+err.Error(), false, true)
		}
		dogs = append(dogs, dogFound)
	}
	fmt.Println(dogs)

}
