package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	crawlerDb    = flag.String("crawler_db", "/tmp/crawler.db", "The extractor database file.")
	_            = fmt.Println
	outputSchema = flag.String("output_schema_file", "schema/create_output_database.sql", "The output file schema.")
	output       = flag.String("output", "output.db", "The output file.")

	useCachingFetcher = flag.Bool("use_caching_fetcher", false,
		"Whether to use a caching fetcher. Resources are cached in crawler database.")
)

// State by their state_id. See create_output_database.sql for id assignment.
var StateById = map[string]int{
	"AL": 1,
	"AK": 2,
	"AZ": 3,
	"AR": 4,
	"CA": 5,
	"CO": 6,
	"CT": 7,
	"DE": 8,
	"DC": 9,
	"FL": 10,
	"GA": 11,
	"HI": 12,
	"ID": 13,
	"IL": 14,
	"IN": 15,
	"IA": 16,
	"KS": 17,
	"KY": 18,
	"LA": 19,
	"ME": 20,
	"MD": 21,
	"MA": 22,
	"MI": 23,
	"MN": 24,
	"MS": 25,
	"MO": 26,
	"MT": 27,
	"NE": 28,
	"NV": 29,
	"NH": 30,
	"NJ": 31,
	"NM": 32,
	"NY": 33,
	"NC": 34,
	"ND": 35,
	"OH": 36,
	"OK": 37,
	"OR": 38,
	"PA": 39,
	"RI": 40,
	"SC": 41,
	"SD": 42,
	"TN": 43,
	"TX": 44,
	"UT": 45,
	"VT": 46,
	"VA": 47,
	"WA": 48,
	"WV": 49,
	"WI": 50,
	"WY": 51,
	"AS": 52,
	"GU": 53,
	"MP": 54,
	"PR": 55,
	"VI": 56,
	"UM": 57,
}

// FetchFn is an interface for a function that takes a url and returns the
// url's contents or an error.
type FetcherFn func(url string) (string, error)

// OpenOutput opens the specified outputFilename and loads outputSchema into the file.
// outputFilename must not exist.
// outputSchema must be a file containing DDL to set up the output file schema.
func OpenOutput(outputFilename, outputSchema string) (*sql.DB, error) {
	_ = os.Remove(outputFilename)
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		return nil, err
	}
	outputFile.Close()

	os, err := ioutil.ReadFile(outputSchema)
	if err != nil {
		return nil, err
	}
	odb, err := sql.Open("sqlite3", *output)
	if err != nil {
		return nil, err
	}

	_, err = odb.Exec(string(os))
	if err != nil {
		return nil, err
	}

	return odb, nil
}

// CrawlManifest crawls the specified manifestUrl and writes the crawl results into output.
func CrawlManifest(manifestUrl string, output *sql.DB, fetchFn FetcherFn) error {
	manifestBody, err := fetchFn(manifestUrl)
	if err != nil {
		return err
	}

	res, err := output.Exec(
		"INSERT INTO manifests (url, contents) VALUES (?, ?)",
		manifestUrl, manifestBody)
	if err != nil {
		return err
	}

	manifestId, err := res.LastInsertId()
	if err != nil {
		return err
	}

	var mf ManifestFile
	err = json.Unmarshal([]byte(manifestBody), &mf)
	if err != nil {
		return err
	}
	for _, o := range mf.Output {
		if o.FileType == "Location" {
			if err := CrawlLeafFile(&CrawlLeafFileOptions{
				FileInfo:         &o,
				ManifestId:       manifestId,
				FileTable:        "locations",
				StateJoinTable:   "location_state",
				StateJoinTableFK: "location_id",
				Fetcher:          fetchFn,
				Output:           output,
			}); err != nil {
				log.Printf("Unable to crawl location file %s: %s", o.Url, err)
			}
		} else if o.FileType == "Schedule" {
			if err := CrawlLeafFile(&CrawlLeafFileOptions{
				FileInfo:         &o,
				ManifestId:       manifestId,
				FileTable:        "schedules",
				StateJoinTable:   "schedule_state",
				StateJoinTableFK: "schedule_id",
				Fetcher:          fetchFn,
				Output:           output,
			}); err != nil {
				log.Printf("Unable to crawl schedule file %s: %s", o.Url, err)
			}
		} else if o.FileType == "Slot" {
			if err := CrawlLeafFile(&CrawlLeafFileOptions{
				FileInfo:         &o,
				ManifestId:       manifestId,
				FileTable:        "slots",
				StateJoinTable:   "slot_state",
				StateJoinTableFK: "slot_id",
				Fetcher:          fetchFn,
				Output:           output,
			}); err != nil {
				log.Printf("Unable to crawl slot file %s: %s", o.Url, err)
			}
		} else {
			log.Printf("Unknown output file type '%s' specified in manifest %s.", o.FileType, manifestUrl)
		}
	}

	return nil
}

// CrawlLeafFileOptions are the options for the CrawLeafFile function.
type CrawlLeafFileOptions struct {
	// File information, as parsed from the Manifest file's output JSON.
	FileInfo *ManifestFileOutput

	// The manifest file primary key which this leaf file belongs to.
	ManifestId int64

	// The table name in which to write the leaf file's contents, e.g. "locations".
	FileTable string

	// The state join table name in which to write state entensions, e.g. "slot_state".
	StateJoinTable string

	// The leaf file's state join table column name, e.g. "slot_id".
	StateJoinTableFK string

	// URL fetcher function.
	Fetcher FetcherFn

	// Output file to write crawl results to.
	Output *sql.DB
}

// CrawlLeafFile crawls a non-manifest file with the given options.
func CrawlLeafFile(opts *CrawlLeafFileOptions) error {
	body, err := opts.Fetcher(opts.FileInfo.Url)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("INSERT INTO %s (url, manifest_id, contents) VALUES (?, ?, ?)", opts.FileTable)
	res, err := opts.Output.Exec(sql, opts.FileInfo.Url, opts.ManifestId, body)
	if err != nil {
		return err
	}
	leafFileId, err := res.LastInsertId()
	if err != nil {
		return err
	}

	for _, state := range opts.FileInfo.Extension.State {
		stateId, ok := StateById[strings.ToUpper(state)]
		if !ok {
			log.Printf("Failed to find state id for %s", state)
			continue
		}
		joinTableSql := fmt.Sprintf("INSERT INTO %s (%s, state_id) VALUES (%d, %d)", opts.StateJoinTable,
			opts.StateJoinTableFK,
			leafFileId, stateId)
		_, err := opts.Output.Exec(joinTableSql)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	cdb, err := sql.Open("sqlite3", *crawlerDb)
	if err != nil {
		log.Fatal(err)
	}
	defer cdb.Close()

	odb, err := OpenOutput(*output, *outputSchema)
	if err != nil {
		log.Fatal(err)
	}
	defer odb.Close()

	knownManifests, err := LoadKnownManifests(cdb)
	if err != nil {
		log.Fatal(err)
	}

	var fetchFn FetcherFn
	if *useCachingFetcher {
		fetchFn = func(url string) (string, error) {
			return FetchResource(url, cdb, FetchOptions{Now: time.Now()})
		}
	} else {
		fetchFn = func(url string) (string, error) {
			resp, err := http.Get(url)
			if err != nil {
				return "", err
			}
			defer resp.Body.Close()
			bodyBuffer := new(strings.Builder)
			if _, err := io.Copy(bodyBuffer, resp.Body); err != nil {
				return "", err
			}
			return bodyBuffer.String(), nil
		}
	}

	crawlStart := time.Now()
	log.Printf("Crawling %d manifests.", len(knownManifests))
	for _, m := range knownManifests {
		if err := CrawlManifest(m.Url, odb, fetchFn); err != nil {
			log.Printf("Failed to crawl manifest %s: %s.", m.Url, err)
		}
	}

	log.Printf("Crawling %d manifests took %s.", len(knownManifests), time.Since(crawlStart).String())
	log.Printf("Output written to %s.", *output)
}
