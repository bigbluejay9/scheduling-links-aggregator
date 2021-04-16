package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

/* Crawler Database Object Models */

// ResourceCacheEntry represents a row in the resource_cache table of the crawler database.
type ResourceCacheEntry struct {
	Url          string
	FetchSec     time.Time
	ExpiresAtSec time.Time
	Etag         sql.NullString
	Data         string
}

// KnownManifest represents a row in the known_manifests table of the crawler database.
type KnownManifest struct {
	KnownManifestId int
	Url             string
}

// ManifestFetch represents a row in the manifest_fetches table of the crawler database.
type ManifestFetch struct {
	ManifestFetchId int
	Url             string
	KnownManifestId int
	ReadSec         time.Time
	FetchStatusCode int
	PollingHintSec  sql.NullInt64
	Contents        sql.NullString
}

// LocationFetch represents a row in the location_fetches table of the crawler database.
type LocationFetch struct {
	LocationFetchId int
	Url             string
	ManifestFetchId int
	ReadSec         int
	FetchStatusCode int
	PollingHintSec  sql.NullInt64
	Contents        sql.NullString
}

// ScheduleFetch represents a row in the schedule_fetches table of the crawler database.
type ScheduleFetch struct {
	ScheduleFetchId int
	Url             string
	ManifestFetchId int
	ReadSec         int
	FetchStatusCode int
	PollingHintSec  sql.NullInt64
	Contents        sql.NullString
}

// SlotFetch represents a row in the slot_fetches table of the crawler database.
type SlotFetch struct {
	SlotFetchId     int
	Url             string
	ManifestFetchId int
	ReadSec         int
	FetchStatusCode int
	PollingHintSec  sql.NullInt64
	Contents        sql.NullString
}

/* File Object Models */
/* As defined https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md */

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

// LocationFileTelecom is the `telecom` JSON object in the location file, as defined
type LocationFileTelecom struct {
	System string `json:"system"`
	Value  string `json:"value"`
}

// LocationFileAddress is the `address` JSON object in the location file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#location-file
type LocationFileAddress struct {
	Line       []string `json:"line"`
	City       string   `json:"city"`
	State      string   `json:"state"`
	PostalCode string   `json:"postalCode"`
	District   string   `json:"district"`
}

// LocationFilePosition is the `position` JSON object in the location file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#location-file
type LocationFilePosition struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// LocationFileIdentifier is the `identifier` JSON object in the location file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#location-file
type LocationFileIdentifier struct {
	System string `json:"system"`
	Value  string `json:"value"`
}

// LocationFile is the JSON object representation of a location file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#location-file
type LocationFile struct {
	ResourceType string                   `json:"resourceType"`
	Id           string                   `json:"id"`
	Name         string                   `json:"name"`
	Telecom      []LocationFileTelecom    `json:"telecom"`
	Address      LocationFileAddress      `json:"address"`
	Description  string                   `json:"description"`
	Position     LocationFilePosition     `json:"position"`
	Identifier   []LocationFileIdentifier `json:"identifier"`
}

// ScheduleFileActor is the `actor` JSON object in the schedule file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#schedule-file
type ScheduleFileActor struct {
	Reference string `json:"reference"`
}

// ScheduleFileServiceType is the `serviceType` JSON object in the schedule file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#schedule-file
type ScheduleFileServiceType struct {
	System  string `json:"system"`
	Code    string `json:"code"`
	Display string `json:"display"`
}

// ScheduleFileExtensionValueCoding is the `valueCoding` JSON object in the schedule file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#schedule-file
type ScheduleFileExtensionValueCoding struct {
	System  string `json:"system"`
	Code    string `json:"code"`
	Display string `json:"display"`
}

// ScheduleFileExtension is the `extension` JSON object in the schedule file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#schedule-file
type ScheduleFileExtension struct {
	Url          string                           `json:"url"`
	ValueCoding  ScheduleFileExtensionValueCoding `json:"valueCoding"`
	ValueInteger int                              `json:"valueInteger"`
}

// ScheduleFile is the JSON object representation of a schedule file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#schedule-file
type ScheduleFile struct {
	ResourceType string                    `json:"resourceType"`
	Id           string                    `json:"id"`
	Actor        []ScheduleFileActor       `json:"actor"`
	ServiceType  []ScheduleFileServiceType `json:"serviceType"`
	Extension    []ScheduleFileExtension   `json:"extension"`
}

// SlotFileExtension is the `schedule` JSON object in the slot file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#slot-file
type SlotFileSchedule struct {
	Reference string `json:"reference"`
}

// SlotFileExtension is the `extension` JSON object in the slot file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#slot-file
type SlotFileExtension struct {
	Url          string `json:"url"`
	ValueUrl     string `json:"valueUrl"`
	ValueString  string `json:"valueString"`
	ValueInteger int64  `json:"valueInteger"`
}

// SlotFile is the JSON object representation of a slot file, as defined
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#slot-file
type SlotFile struct {
	ResourceType string              `json:"resourceType"`
	Id           string              `json:"id"`
	Schedule     SlotFileSchedule    `json:"schedule"`
	Status       string              `json:"status"`
	Start        string              `json:"start"`
	End          string              `json:"end"`
	Extension    []SlotFileExtension `json:"extension"`
}

// FetchOptions controls the interaction of FetchResource with the resource_cache
// table in the crawler database.
type FetchOptions struct {
	// Time now.
	Now time.Time

	// The User-Agent to send with any GET requests.
	// If empty, "github.com/lazau/scheduling-links-aggregator/crawler" is used.
	UserAgent string

	// Whether to consult the resource_cache before attempt to fetch the resource.
	// If true, fetch calls will never reuse the result of a previous fetch.
	// If false, fetch calls will reuse the result of a previous fetch if the resource
	// has not yet expired.
	SkipCache bool

	// If SkipCache is not set, determines whether a If-None-Match
	// header is included in the GET request for the resource containing ETags
	// from any previously cached fetches.
	// If SkipCache is set, this option is ignored. I.e. If-None-Match header
	// will never be sent.
	DoNotSendIfNoneMatchHeader bool

	// If SkipCache is not set, determines whether a If-Modified-Since
	// header is included in the GET request for the resource. The header will contain the
	// time at which the cached fetch was issued.
	// If SkipCache is set, this option is ignored. I.e. If-Modified-Since header
	// will never be sent.
	DoNotSendIfModifiedSinceHeader bool

	// If a successful GET request was completed, whether to write the resource
	// into the cache.
	DoNotSaveToCache bool
}

func FetchResource(url string, db *sql.DB, opts FetchOptions) (string, error) {
	if opts.SkipCache {
		goto ACTUAL_FETCH
	}

	// Consult the cache for the latest cached entry, if any.
	row := db.QueryRow(`
    SELECT data FROM resource_cache WHERE
      url = ? AND expires_at_sec < ?
           `, url, opts.Now.Unix())

	var data string
	err := row.Scan(&data)

	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	if err == nil {
		return data, nil
	}

ACTUAL_FETCH:
	var userAgent = opts.UserAgent

	if userAgent == "" {
		userAgent = "github.com/lazau/scheduling-links-aggregator/crawler"
	}

	// XXX

	return "", nil
}

/* Parser Functions */

// Parses js to ManifestFile
// JS should be a JSON string containing
// https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#manifest-file
func parseManifestFile(js string) (ManifestFile, error) {
	var f ManifestFile
	err := json.Unmarshal([]byte(js), &f)
	return f, err
}

/* --- */

// LoadKnownManifests loads all rows of the known_manifests table in db.
func LoadKnownManifests(db *sql.DB) ([]KnownManifest, error) {
	rows, err := db.Query("SELECT known_manifest_id, url FROM known_manifests")
	if err != nil {
		return []KnownManifest{}, err
	}
	defer rows.Close()

	var (
		dest    KnownManifest
		results []KnownManifest
	)
	for rows.Next() {
		if err := rows.Scan(&dest.KnownManifestId, &dest.Url); err != nil {
			return results, err
		}
		results = append(results, dest)
	}

	return results, rows.Err()
}

// FetchManifest fetches the specified manifest file and returns a ManifestFetch with ManifestFetchId set to 0.
func (m *KnownManifest) FetchManifest(rateLimiting, cacheControl bool) (ManifestFetch, error) {
}

// LastManifestFetch returns the latest manifest fetch row associated with the given known manifest.
// If onlySuccess is set, only returns the last successful fetch.
func (m *KnownManifest) LastManifestFetch(db *sql.DB, onlySuccess bool) (ManifestFetch, error) {
	q :=
		`SELECT
				manifest_fetch_id, url, known_manifest_id, read_sec, fetch_status_code, polling_hint_sec, contents
				FROM manifest_fetches WHERE known_manifest_id = ? ORDER BY read_sec DESC LIMIT 1`
	if onlySuccess {
		q =
			`SELECT
				manifest_fetch_id, url, known_manifest_id, read_sec, fetch_status_code, polling_hint_sec, contents
				FROM manifest_fetches WHERE known_manifest_id = ? AND fetch_status_code = 200 ORDER BY read_sec DESC LIMIT 1`
	}
	row := db.QueryRow(q, m.KnownManifestId)
	var (
		f           ManifestFetch
		readSecUnix int
	)
	if err := row.Scan(&f.ManifestFetchId, &f.Url, &f.KnownManifestId, &readSecUnix, &f.FetchStatusCode,
		&f.PollingHintSec, &f.Contents); err != nil {
		return f, err
	}

	f.ReadSec = time.Unix(int64(readSecUnix), 0)
	return f, nil
}

// Returns whether we are able to fetch manifest URL again based on the last fetch time and pollingHintSec.
// By default, manifests are deemed fetchable again after 3 minutes since the last fetch.
func (m *ManifestFetch) ShouldFetchManifest(now time.Time) bool {
	nextFetchTime := m.ReadSec
	if m.PollingHintSec.Valid {
		nextFetchTime.Add(time.Duration(m.PollingHintSec.Int64) * time.Second)
	} else {
		nextFetchTime.Add(3 * time.Minute)
	}

	return !(nextFetchTime.After(now))
}

// Parses the manifest file's contents and return ManifestFile
func (m *ManifestFetch) ParseContents() (ManifestFile, error) {
	if !m.Contents.Valid {
		return ManifestFile{},
			fmt.Errorf("manifest fetch %d failed, cannot ParseContents", m.ManifestFetchId)
	}
	return parseManifestFile(m.Contents.String)
}
