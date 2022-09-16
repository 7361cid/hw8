package main

import (
	"encoding/csv"
	"fmt"
	memorycache "go-memorycache-example"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
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
		var key string = element.dev_type + ":" + element.dev_id
		var value string = strings.Join(element.raw_apps, " ")
		cache.Set(key, value, 5*time.Minute)
		fmt.Println("Fragment loaded")
	}
}

func main() {
	t0 := time.Now()
	var wg sync.WaitGroup
	files, _ := filepath.Glob("*.tsv")
	fmt.Printf("%q\n", files)
	var lenght = len(files)
	for k := 0; k < lenght; k++ {
		f, err := os.Open(files[k])
		if err != nil {
			log.Fatal(err)
		}

		// remember to close the file at the end of the program
		defer f.Close()
		var x = 0
		var file_end = false
		//var slice = make([]string, 1)
		// read csv values using csv.Reader
		csvReader := csv.NewReader(f)
		var struct_slice [10][]Parsed_data
		fmt.Println(reflect.TypeOf(struct_slice))
		for {
			for i := 0; i < 10; i++ {
				for j := 0; j < 10; j++ {
					rec, err := csvReader.Read()
					if err == io.EOF {
						file_end = true
						break
					}
					s := strings.Split(rec[0], "	")
					struct_slice[i] = append(struct_slice[i], Parsed_data{s[0], s[1], s[2], s[3], rec[1:]})
					x = x + 1
				}
			}

			for i := 0; i < 9; i++ {
				wg.Add(1)
				go func(data []Parsed_data) {
					// Decrement the counter when the goroutine completes.
					defer wg.Done()
					cache_data(data)
				}(struct_slice[i])
			}
			if file_end {
				break
			}
			if x > 1000 {
				break
			}
		}
		wg.Wait()
	}
	fmt.Printf("Elapsed time: %v", time.Since(t0))
}

//memorycache "github.com/maxchagin/go-memorycache-example"
