package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/ratelimit"
	"github.com/jedib0t/go-pretty/table"
	"github.com/cheggaaa/pb/v3"
)

const (
	totalsApi     = "https://zkillboard.com/api/history/totals.json"
	historyFormat = "https://zkillboard.com/api/history/%s.json"

	storageDirectory = "./json"
)

var (
	rl ratelimit.Limiter
)

func main() {

	if len(os.Args) == 1 {
		// No args provided print help
		log.Println("options are 'download' or 'load'")
		return
	}

	switch os.Args[1] {
	case "download":
		downloadTotalHistory()
	case "load":
		if len(os.Args) < 3 {
			log.Println("Please provide the url for podded")
			return
		}

		u, err := url.ParseRequestURI(os.Args[2])
		if err != nil {
			log.Println("Please provide a valid url for podded")
			return
		}

		limit := 1

		if len(os.Args) == 4 {
			log.Printf("Attempting to set limter to %s", os.Args[3])
			i, err := strconv.Atoi(os.Args[3])
			if err != nil {
				log.Printf("Failed to set limiter: %v", err)
			}
			limit = i
			log.Printf("Limiter set to %drps\n", limit)
		}
		loadHistoryIntoPodded(u.String(), limit)
	default:
		log.Println("options are 'download' or 'load'")
	}
}

func loadHistoryIntoPodded(poddedURL string, limit int) error {

	idhp := make(map[int32]string)
	limiter := ratelimit.New(limit)

	files, err := ioutil.ReadDir(storageDirectory)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()
		jsonFile, err := os.Open(filepath.Join(storageDirectory, file.Name()))
		if err != nil {
			log.Fatalf("Error opening file: %s: %v", name, err)
			continue
		}
		defer jsonFile.Close()

		var totals map[int32]string

		br := bufio.NewReader(jsonFile)
		err = json.NewDecoder(br).Decode(&totals)
		if err != nil {
			log.Fatalf("Error decoding file: %s: %v", name, err)
			continue
		}

		for k,v := range totals {
			idhp[k]=v
		}
	}

	client := http.Client{
		Timeout: 5 * time.Second,
	}

	responseCodes := make(map[int]int)

	bar := pb.StartNew(len(idhp))
	bar.SetWriter(os.Stdout)

	for id, hash := range idhp {
		limiter.Take()

		jsn := fmt.Sprintf("{\"id\": %d, \"hash\": \"%s\"}", id, hash)
		resp, err := client.Post(poddedURL, "application/json", strings.NewReader(jsn))
		if err != nil {
			log.Printf("Failed to post id %d: %v", id, err)
			continue
		}
		responseCodes[resp.StatusCode] += 1
		bar.Increment()
	}
	bar.Finish()

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Response Code", "Count"})
	rs := 0
	for r, c := range responseCodes {
		t.AppendRow(table.Row{r,c})
		rs += c
	}
	t.AppendFooter(table.Row{"Total", rs})
	t.Render()

	return nil
}

func downloadTotalHistory() {
	log.Println("Getting the list of all zkillboard recorded days")

	rl = ratelimit.New(10)

	totals, err := getTotalsData()
	if err != nil {
		log.Fatalf("Failed to get zkillboard totals: %v\n", err)
	}

	if _, err := os.Stat(storageDirectory); os.IsNotExist(err) {
		// Path does not exist create it
		err = os.Mkdir(storageDirectory, 0755)
		if err != nil {
			log.Fatalf("Failed to create storage directory: %v\n", err)
		}
	}

	var wg sync.WaitGroup

	for k, v := range totals {
		date := k
		if !haveCorrectTotal(date, v) {
			wg.Add(1)
			go func() {
				err = downloadSingleHistory(date)
				if err != nil {
					log.Printf("Failed to download date %s: %v\n", date, err)
				} else {
					log.Printf("Downloaded history for %s\n", date)
				}
				wg.Done()
			}()
		}
	}

	wg.Wait()
	log.Println("COMPLETE! :D")
}

func downloadSingleHistory(date string) error {
	data, err := getUrlData(fmt.Sprintf(historyFormat, date))
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(filepath.Join(storageDirectory, fmt.Sprintf("%s.json", date)), data, 0644)
	if err != nil {
		return err
	}

	return nil

}

func haveCorrectTotal(date string, count int) bool {
	jsonFile, err := os.Open(filepath.Join(storageDirectory, fmt.Sprintf("%s.json", date)))
	if err != nil {
		return false
	}
	defer jsonFile.Close()

	var totals map[int32]string

	br := bufio.NewReader(jsonFile)
	err = json.NewDecoder(br).Decode(&totals)
	if err != nil {
		return false
	}

	if len(totals) == count {
		return true
	}
	return false

}

func getTotalsData() (map[string]int, error) {
	data, err := getUrlData(totalsApi)

	var totals map[string]int

	err = json.Unmarshal(data, &totals)
	if err != nil {
		return nil, err
	}

	return totals, nil
}

func getUrlData(url string) ([]byte, error) {

	rl.Take()

	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("Non happy status: %d", response.StatusCode))
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}
