package main

import (
	"strconv"
	"testing"
)

func BenchmarkNonEncrypted(b *testing.B) {
	number_of_documents := b.N

	config, err := LoadConfig()
	//Set cache timeout for short for testing
	if err != nil {
		l(err.Error(), true, true)
	}

	config.Encryption_key = ""
	config.Salt = ""
	// Create database driver
	DB, err = NewDB(config.Path, config)
	if err != nil {
		b.Fatal("Unable to create DB! " + err.Error())
	}
	// Cache not needed right now for this test
	go DB.RunCachePurge()

	// Test sequential write
	for id := range number_of_documents {
		test := TestObject{String: "test" + strconv.Itoa(id), Number: float64(id)}
		_, err := DB.Collection("Test").Add(test)
		if err != nil {
			b.Fatal(err.Error())
		}
	}

	ClearTestDatabase(DB)
}

func BenchmarkEncrypted(b *testing.B) {
	number_of_documents := b.N

	config, err := LoadConfig()
	//Set cache timeout for short for testing
	if err != nil {
		l(err.Error(), true, true)
	}

	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	// Create database driver
	DB, err = NewDB(config.Path, config)
	if err != nil {
		b.Fatal("Unable to create DB! " + err.Error())
	}
	// Cache not needed right now for this test
	go DB.RunCachePurge()

	// Test sequential write
	for id := range number_of_documents {
		test := TestObject{String: "test" + strconv.Itoa(id), Number: float64(id)}
		_, err := DB.Collection("Test").Add(test)
		if err != nil {
			b.Fatal(err.Error())
		}
	}

	ClearTestDatabase(DB)
}
