// s project main.go
package main

import (
	"encoding/csv"
	"fmt"
	"go-memorycache-example"
	"io"
	"log"
	"os"
	"time"

	//	"reflect"
	"strings"
)

type Parsed_data struct {
	dev_type string
	dev_id   string
	lat      string
	lon      string
	raw_apps []string
}

func cache_data(data []Parsed_data) {
	cache := memorycache.New(5*time.Minute, 10*time.Minute)
	for _, element := range data {
		fmt.Println(element)
	}
	cache.Set("myKey", "My value", 5*time.Minute)
}

func main() {
	f, err := os.Open("C:\\Users\\chernov.ilia\\20170929000000\\20170929000000.tsv")
	if err != nil {
		log.Fatal(err)
	}

	// remember to close the file at the end of the program
	defer f.Close()
	var x = 0
	//var slice = make([]string, 1)
	// read csv values using csv.Reader
	csvReader := csv.NewReader(f)
	var struct_slice []Parsed_data
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		x = x + 1
		//slice = append(slice, rec)
		s := strings.Split(rec[0], "	")
		struct_slice = append(struct_slice, Parsed_data{s[0], s[1], s[2], s[3], rec[1:]})
		if x == 10 {
			cache_data(struct_slice)
			break
		}
	}
}
