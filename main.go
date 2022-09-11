package main

import (
	"encoding/csv"
	"fmt"
	memorycache "github.com/maxchagin/go-memorycache-example"
	"io"
	"log"
	"os"
	"path/filepath"
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
		var key string = element.dev_type + ":" + element.dev_id
		var value string = strings.Join(element.raw_apps, " ")
		cache.Set(key, value, 5*time.Minute)
	}
}

func main() {
	files, _ := filepath.Glob("*.tsv")
	fmt.Printf("%q\n", files)
	f, err := os.Open(files[0])
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
