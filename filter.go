package opendivdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Filter struct {
	field    string // Filed to filter by
	operator string // Accepted conditions ==, <=, >=, !=, >, <. Comparison is done in the following format: [field] [operator] [value]
	value    any    // Value of condition
}

// Filtered documents
func (c *Collection) filteredDocuments() ([]Document, error) {
	// Filtered docs
	var col []Document
	// ensure there is a collection to read
	if c.collection_name == "" {
		return col, fmt.Errorf("missing collection - unable to record location")
	}

	dir := filepath.Join(c.driver.dir, c.collection_name)

	// check to see if collection (directory) exists
	if _, err := stat(dir); err != nil {
		return col, err
	}

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, _ := os.ReadDir(dir)

	// iterate over each of the files, attempting to read the file. If successful
	// append the files to the collection of read files
	for _, file := range files {
		doc, err := c.Document(file.Name())
		if err != nil {
			return col, fmt.Errorf("unable to read file "+file.Name(), false, true)
		}
		included, err := c.filter.included(doc)
		if err != nil {
			return col, fmt.Errorf("error filtering document " + err.Error())
		}
		if included {
			col = append(col, doc)
		}
	}
	return col, nil
}

func (f *Filter) included(doc Document) (bool, error) {
	// Accepted operators
	operators := map[string]bool{
		"==": true,
		"<=": true,
		">=": true,
		"!=": true,
		"<":  true,
		">":  true}

	// Check to make sure correct condition is provided
	if _, ok := operators[f.operator]; !ok {
		return false, fmt.Errorf("Filter '" + f.operator + "' is not supported. Accepted conditions ==, <=, >=, !=, <, > ")
	}
	// Marshal document data into generic map for comparison
	var d map[string]interface{}
	if err := json.Unmarshal(doc.Data, &d); err != nil {
		panic(err)
	}

	// Find field
	field := d[f.field]
	// Check for provided field
	if field != nil {
		switch real := field.(type) {
		case string:
			switch filter_t := f.value.(type) {
			case time.Time:
				document_parsed_time, err := time.Parse(time.RFC3339Nano, real)
				if err != nil {
					return false, fmt.Errorf("document filed is RFC3339 formatted time but the filter isn't, unable to parse filter to date time")
				}
				switch f.operator {
				case "<":
					if document_parsed_time.Before(filter_t) {
						return true, nil
					}
				case ">":
					if document_parsed_time.After(filter_t) {
						return true, nil
					}
				case "==":
					if document_parsed_time.Equal(filter_t) {
						return true, nil
					}
				default:
					return false, fmt.Errorf("unsupported operator " + f.operator + " for time")
				}
			case string:
				switch f.operator {
				case "==":
					if real == filter_t {
						return true, nil
					}
				case "!=":
					if real != filter_t {
						return true, nil
					}
				default:
					return false, fmt.Errorf("unsupported operator " + f.operator + " for string")
				}
			default:
				return false, fmt.Errorf("document field and filter value are mismatched")
			}
		case float64:
			switch filter_t := f.value.(type) {
			case int:
				filter_t_float64 := float64(filter_t)
				return compareFloat64(real, f.operator, filter_t_float64), nil
			case int8:
				filter_t_float64 := float64(filter_t)
				return compareFloat64(real, f.operator, filter_t_float64), nil
			case int16:
				filter_t_float64 := float64(filter_t)
				return compareFloat64(real, f.operator, filter_t_float64), nil
			case int32:
				filter_t_float64 := float64(filter_t)
				return compareFloat64(real, f.operator, filter_t_float64), nil
			case int64:
				filter_t_float64 := float64(filter_t)
				return compareFloat64(real, f.operator, filter_t_float64), nil
			case float32:
				filter_t_float64 := float64(filter_t)
				return compareFloat64(real, f.operator, filter_t_float64), nil
			case float64:
				return compareFloat64(real, f.operator, filter_t), nil
			default:
				return false, fmt.Errorf("Filter Value is not float64. For more details: https://pkg.go.dev/encoding/json#Unmarshal")
			}
		case bool:
			switch filter_t := f.value.(type) {
			case bool:
				switch f.operator {
				case "==":
					if real == filter_t {
						return true, nil
					}
				case "!=":
					if real != filter_t {
						return true, nil
					}
				default:
					return false, fmt.Errorf("unsupported operator " + f.operator + " for bool")
				}
			default:
				return false, fmt.Errorf("document field and filter value are mismatched")
			}
		}
	}
	return false, nil
}

func compareFloat64(doc_value float64, operator string, compare_value float64) bool {
	switch operator {
	case "==":
		if doc_value == compare_value {
			return true
		}
	case "<=":
		if doc_value <= compare_value {
			return true
		}
	case ">=":
		if doc_value >= compare_value {
			return true
		}
	case "!=":
		if doc_value != compare_value {
			return true
		}
	case "<":
		if doc_value < compare_value {
			return true
		}
	case ">":
		if doc_value > compare_value {
			return true
		}
	}
	return false
}
