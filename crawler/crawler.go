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
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	manifestUrls = flag.String("manifest_urls", "manifest_urls", "A file containing a list of manifest URLs on each line.")
	output       = flag.String("output", "/tmp/crawler_output.VERSION.sqlite", "The output file. 'VERSION' is replaced by the current unix epoch timestamp.")
)

// OutputSchema is the schema for the sqlite database written into the output file.
var OutputSchema = `
PRAGMA encoding = "UTF-8";
PRAGMA foreign_keys = ON;

-- Manifest files.
CREATE TABLE manifests(
    manifest_id INTEGER PRIMARY KEY,

    -- The URL of the manifest file.
    url TEXT NOT NULL,

    -- The manifest file's contents.
    -- See https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#manifest-file
    -- for file content definition.
    contents TEXT NOT NULL
);

-- Location files.
CREATE TABLE locations(
    location_id INTEGER PRIMARY KEY,

    -- The URL of the location file.
    url TEXT NOT NULL,

    -- The manifest file this file is associated with.
    manifest_id NOT NULL
      REFERENCES manifests(manifest_id)
        ON DELETE CASCADE,

    -- The location file's contents.
    -- See https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#location-file
    -- for file content definition.
    contents TEXT NOT NULL
);

-- Schedule files.
CREATE TABLE schedules(
    schedule_id INTEGER PRIMARY KEY,

    -- The URL of the schedule file.
    url TEXT NOT NULL,

    -- The manifest file this file is associated with.
    manifest_id NOT NULL
      REFERENCES manifests(manifest_id)
        ON DELETE CASCADE,

    -- The schedule file's contents.
    -- See https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#schedule-file
    -- for file content definition.
    contents TEXT NOT NULL
);

-- Slot files.
CREATE TABLE slots(
    slot_id INTEGER PRIMARY KEY,

    -- The URL of this slot file.
    url TEXT NOT NULL,

    -- The manifest file this file is associated with.
    manifest_id NOT NULL
      REFERENCES manifests(manifest_id)
        ON DELETE CASCADE,

    -- The slot file's contents.
    -- See https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#slot-file
    -- for file content definition.
    contents TEXT NOT NULL
);

-- States is a list of states. If files are annotated with a state extension in the manifest file,
-- the file_states table can be joined with states in order to find files for specific states.
CREATE TABLE states(
    state_id INTEGER PRIMARY KEY,

    -- Two character name of the state, e.g. "MA", "CA".
    name TEXT NOT NULL
);

-- Note: the state_ids are hard coded in application code.
INSERT INTO states(state_id, name)
  VALUES
    (1,  "AL"),
    (2,  "AK"),
    (3,  "AZ"),
    (4,  "AR"),
    (5,  "CA"),
    (6,  "CO"),
    (7,  "CT"),
    (8,  "DE"),
    (9,  "DC"),
    (10, "FL"),
    (11, "GA"),
    (12, "HI"),
    (13, "ID"),
    (14, "IL"),
    (15, "IN"),
    (16, "IA"),
    (17, "KS"),
    (18, "KY"),
    (19, "LA"),
    (20, "ME"),
    (21, "MD"),
    (22, "MA"),
    (23, "MI"),
    (24, "MN"),
    (25, "MS"),
    (26, "MO"),
    (27, "MT"),
    (28, "NE"),
    (29, "NV"),
    (30, "NH"),
    (31, "NJ"),
    (32, "NM"),
    (33, "NY"),
    (34, "NC"),
    (35, "ND"),
    (36, "OH"),
    (37, "OK"),
    (38, "OR"),
    (39, "PA"),
    (40, "RI"),
    (41, "SC"),
    (42, "SD"),
    (43, "TN"),
    (44, "TX"),
    (45, "UT"),
    (46, "VT"),
    (47, "VA"),
    (48, "WA"),
    (49, "WV"),
    (50, "WI"),
    (51, "WY"),
    (52, "AS"),
    (53, "GU"),
    (54, "MP"),
    (55, "PR"),
    (56, "VI"),
    (57, "UM");

-- Schedule state is the join table between schedules and state.
CREATE TABLE schedule_state(
  schedule_id NOT NULL
    REFERENCES schedules(schedule_id)
      ON DELETE CASCADE,

  state_id NOT NULL
    REFERENCES states(state_id)
      ON DELETE CASCADE
);

-- Location state is the join table between locations and state.
CREATE TABLE location_state(
  location_id NOT NULL
    REFERENCES locations(location_id)
      ON DELETE CASCADE,

  state_id NOT NULL
    REFERENCES states(state_id)
      ON DELETE CASCADE
);

-- Slot state is the join table between slots and state.
CREATE TABLE slot_state(
  slot_id NOT NULL
    REFERENCES slots(slot_id)
      ON DELETE CASCADE,

  state_id NOT NULL
    REFERENCES states(state_id)
      ON DELETE CASCADE
);
`

// State by their state_id. See OutputSchema for id assignment.
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

// ManifestFileOutput is the `extension` JSON object in the manifest file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#manifest-file
type ManifestFileOutputExtension struct {
	State []string `json:"state"`
}

// ManifestFileOutput is the `output` JSON object in the manifest file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#manifest-file
type ManifestFileOutput struct {
	FileType  string                      `json:"type"`
	Url       string                      `json:"url"`
	Extension ManifestFileOutputExtension `json:"extension"`
}

// ManifestFile is the JSON object representation of a manifest file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#manifest-file
type ManifestFile struct {
	TransactionTime string               `json:"transactionTime"`
	Request         string               `json:"request"`
	Output          []ManifestFileOutput `json:"output"`
}

// CrawlStats tracks statistics about the crawler.
// Thread safe.
type CrawlStats struct {
	// Maps fileType (manifest, location, etc...) -> count.
	fileTypeToCount map[string]int

	// Maps host -> count.
	hostToCount map[string]int

	// Records when the crawling started/ended.
	startTime time.Time
	endTime   time.Time

	// Guards all fields.
	mu sync.Mutex
}

// Record that the crawling started.
func (c *CrawlStats) CrawlStart() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.startTime = time.Now()
}

// Record that the crawling ended.
func (c *CrawlStats) CrawlEnd() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.endTime = time.Now()
}

// Record an instance of crawling url with fileType.
func (c *CrawlStats) Record(u, fileType string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.fileTypeToCount == nil {
		c.fileTypeToCount = make(map[string]int)
	}

	key := strings.ToLower(fileType)
	count := c.fileTypeToCount[key]
	c.fileTypeToCount[key] = count + 1

	ur, err := url.Parse(u)
	// Skip recording malformed URLs.
	if err != nil {
		return
	}

	if c.hostToCount == nil {
		c.hostToCount = make(map[string]int)
	}

	hostKey := ur.Host
	count = c.hostToCount[hostKey]
	c.hostToCount[hostKey] = count + 1
}

// Returns pretty printed stats.
func (c *CrawlStats) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	o := "\n\n\n"

	o += fmt.Sprintf("Crawling took %s.\n\n", c.endTime.Sub(c.startTime))
	o += "Crawled resources by type:\n"
	for k, v := range c.fileTypeToCount {
		if k == "slot" {
			o += fmt.Sprintf("%s:\t\t%d\n", k, v)
		} else {
			o += fmt.Sprintf("%s:\t%d\n", k, v)
		}
	}

	o += "\nCrawled resources by host:\n"
	for k, v := range c.hostToCount {
		o += fmt.Sprintf("%s:\t%d\n", k, v)
	}
	o += "\n\n"
	return o
}

// FetchFn is an interface for a function that takes a url and returns the
// url's contents or an error.
type FetcherFn func(url string) (string, error)

// OpenOutput opens the specified outputFilename and loads schema into the file.
// outputFilename must not exist.
// Returns the opened database, the actual filename, and error.
func OpenOutput(outputFilenameTemplate, schema string) (*sql.DB, string, error) {
	outputFilename := strings.ReplaceAll(
		outputFilenameTemplate, "VERSION", fmt.Sprintf("%d", time.Now().Unix()))

	_ = os.Remove(outputFilename)
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		return nil, "", err
	}
	outputFile.Close()

	odb, err := sql.Open("sqlite3", outputFilename)
	if err != nil {
		return nil, "", err
	}

	_, err = odb.Exec(schema)
	if err != nil {
		odb.Close()
		return nil, "", err
	}

	return odb, outputFilename, nil
}

// CrawlManifest crawls the specified manifestUrl and writes the crawl results into output.
func CrawlManifest(manifestUrl string, output *sql.DB, fetchFn FetcherFn, stats *CrawlStats) error {
	log.Printf("Crawling Manifest file: %s.", manifestUrl)
	stats.Record(manifestUrl, "manifest")
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
				Stats:            stats,
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
				Stats:            stats,
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
				Stats:            stats,
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

	// Stats - CrawlStats.Record(FileInfo.FileType) will be called.
	Stats *CrawlStats
}

// CrawlLeafFile crawls a non-manifest file with the given options.
func CrawlLeafFile(opts *CrawlLeafFileOptions) error {
	log.Printf("Crawling %s file: %s.", opts.FileInfo.FileType, opts.FileInfo.Url)
	opts.Stats.Record(opts.FileInfo.Url, opts.FileInfo.FileType)
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

// LoadManifestUrls reads manifestFile and returns a list of strings corresponding to each non-empty line of the file.
func LoadManifestUrls(manifestFile string) ([]string, error) {
	c, err := ioutil.ReadFile(manifestFile)
	if err != nil {
		return []string{}, err
	}

	var filtered []string
	for _, url := range strings.Split(string(c), "\n") {
		if url == "" {
			continue
		}
		filtered = append(filtered, url)
	}
	return filtered, nil
}

func Run() error {
	var stats CrawlStats
	log.Printf("Loading manifest urls from %s.", *manifestUrls)
	urls, err := LoadManifestUrls(*manifestUrls)
	if err != nil {
		return err
	}

	odb, outputFilename, err := OpenOutput(*output, OutputSchema)
	if err != nil {
		return err
	}
	defer odb.Close()

	var fetchFn FetcherFn = func(url string) (string, error) {
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

	stats.CrawlStart()
	log.Printf("Crawling %d manifests.", len(urls))
	for _, url := range urls {
		if err := CrawlManifest(url, odb, fetchFn, &stats); err != nil {
			log.Printf("Failed to crawl manifest %s: %s.", url, err)
		}
	}
	stats.CrawlEnd()
	log.Print(stats.String())
	log.Printf("Output written to %s.", outputFilename)

	return nil
}

func main() {
	if err := Run(); err != nil {
		log.Fatal(err)
	}
}
