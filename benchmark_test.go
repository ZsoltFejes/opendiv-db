package opendivdb

import (
	"strconv"
	"testing"

	"golang.org/x/sync/errgroup"
)

func Benchmark_NonEncrypted_Serial(b *testing.B) {
	var DB *Driver
	number_of_documents := b.N

	config, err := LoadConfig()
	//Set cache timeout for short for testing
	if err != nil {
		b.Fatal(err.Error())
	}

	config.Encryption_key = ""
	config.Salt = ""
	// Create database driver
	DB, err = NewDB(config)
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

func Benchmark_NonEncrypted_Parallel(b *testing.B) {
	var DB *Driver
	//fmt.Println(b.N)
	number_of_documents := b.N
	//number_of_documents := 1000

	config, err := LoadConfig()
	//Set cache timeout for short for testing
	if err != nil {
		b.Fatal(err.Error())
	}

	config.Encryption_key = ""
	config.Salt = ""
	// Create database driver
	DB, err = NewDB(config)
	if err != nil {
		b.Fatal("Unable to create DB! " + err.Error())
	}
	// Cache not needed right now for this test
	go DB.RunCachePurge()

	// Test parallel write
	eg := errgroup.Group{}
	eg.SetLimit(2000)
	for id := range number_of_documents {
		test := TestObject{String: "test" + strconv.Itoa(id), Number: float64(id)}
		eg.Go(func() error {
			//fmt.Println("Adding " + test.String)
			_, err := DB.Collection("Test").Add(test)
			if err != nil {
				return err
			}
			return nil
		})
	}
	eg.Wait()

	ClearTestDatabase(DB)
}

func Benchmark_Encrypted_Serial(b *testing.B) {
	var DB *Driver
	number_of_documents := b.N

	config, err := LoadConfig()
	//Set cache timeout for short for testing
	if err != nil {
		b.Fatal(err.Error())
	}

	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	// Create database driver
	DB, err = NewDB(config)
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

func Benchmark_Encrypted_Parallel(b *testing.B) {
	var DB *Driver
	//fmt.Println(b.N)
	number_of_documents := b.N
	//number_of_documents := 1000

	config, err := LoadConfig()
	//Set cache timeout for short for testing
	if err != nil {
		b.Fatal(err.Error())
	}

	config.Salt = "xvq-Gn2L4TvwrFQzTCUZzGNbQ.wKbuKB-KmDXLv8iJ.2syPbheC!KkCfhwip@@Mn_X2RdfAsdE6o9-hwwErc**UwVtaxZvBLWHTd"
	// Create database driver
	DB, err = NewDB(config)

	if err != nil {
		b.Fatal("Unable to create DB! " + err.Error())
	}
	// Cache not needed right now for this test
	go DB.RunCachePurge()

	// Test parallel write
	eg := errgroup.Group{}
	eg.SetLimit(2000)
	for id := range number_of_documents {
		test := TestObject{String: "test" + strconv.Itoa(id), Number: float64(id)}
		eg.Go(func() error {
			_, err := DB.Collection("Test").Add(test)
			if err != nil {
				return err
			}
			return nil
		})
	}
	eg.Wait()

	ClearTestDatabase(DB)
}
