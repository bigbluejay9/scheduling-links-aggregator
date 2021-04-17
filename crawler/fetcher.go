package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	crawlerUserAgent = flag.String("crawler_user_agent",
		"github.com/lazau/scheduling-links-aggregator/crawler",
		"The User-Agent string sent with all crawler HTTP requests")

	resourceRateLimitSec = flag.Int("resource_rate_limit_sec",
		90,
		"The minimum number of seconds to wait before sending another HTTP request for a resource.")

	defaultResourceExpirationSec = flag.Int("default_resource_expiration_sec",
		120,
		"The default resource expiration time, if none is specified. In other words, the length of time resources are cached for.")

	fetchTimeoutSec = flag.Int("fetch_timeout_sec", 10,
		"The timeout for sending HTTP requests.")
)

/* Crawler Database Object Models */

// FetchAttempt represents a row in the fetch_attemps table of the crawler database.
type FetchAttempt struct {
	Url        string
	FetchSec   time.Time
	StatusCode int
}

// ResourceCacheEntry represents a row in the resource_cache table of the crawler database.
type ResourceCacheEntry struct {
	ResourceCacheId int
	Url             string
	FetchSec        time.Time
	ExpiresAtSec    time.Time
	Etag            sql.NullString
	Data            string
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

/* Fetcher Functions */

// FetchOptions controls the interaction of FetchResource with the resource_cache
// table in the crawler database.
type FetchOptions struct {
	// Time now.
	Now time.Time

	// Whether to consult the resource_cache before attempt to fetch the resource.
	// If true, fetch calls will never reuse the result of a previous fetch.
	// If false, fetch calls will reuse the result of a previous fetch if the resource
	// has not yet expired.
	SkipCache bool

	// If it is determined that a GET request must be issued,
	// whether to ignore rate limiting rules. By default, a URL will not
	// be fetched more than once resource_rate_limit_sec.
	IgnoreRateLimiting bool

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

// lastFetchAttempt returns the most recent FetchAttempt for the given URL.
// If no last attempt was found, (nil, nil) is returned.
func lastFetchAttempt(url string, db *sql.DB) (*FetchAttempt, error) {
	f := FetchAttempt{Url: url}

	row := db.QueryRow(`
         SELECT fetch_sec, status_code FROM fetch_attemps WHERE
         url = ? ORDER BY fetch_sec DESC LIMIT 1
         `, url)

	var fetchSec int
	err := row.Scan(&fetchSec, &f.StatusCode)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	f.FetchSec = time.Unix(int64(fetchSec), 0)
	return &f, nil
}

// insertFetchAttempt inserts a new row into the fetch_attempts table.
func insertFetchAttempt(f *FetchAttempt, db *sql.DB) error {
	_, err := db.Exec(`
      INSERT INTO fetch_attempts(url, fetch_sec, status_code) VALUES(?, ?, ?)
      `, f.Url, f.FetchSec.Unix(), f.StatusCode)
	return err
}

// latestResourceCacheEntry returns the latest expiring cache entry for the given URL.
// If no cache entries are found, (nil, nil) will be returned.
func latestResourceCacheEntry(url string, db *sql.DB) (*ResourceCacheEntry, error) {
	cacheEntry := ResourceCacheEntry{Url: url}

	row := db.QueryRow(`
         SELECT resource_cache_id, fetch_sec, expires_at_sec, etag, data FROM
         resource_cache WHERE
         url = ? ORDER BY expires_at_sec DESC LIMIT 1
         `, url)
	var fetchSec, expiresAtSec int

	err := row.Scan(&cacheEntry.ResourceCacheId, &fetchSec,
		&expiresAtSec, &cacheEntry.Etag, &cacheEntry.Data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	cacheEntry.FetchSec = time.Unix(int64(fetchSec), 0)
	cacheEntry.ExpiresAtSec = time.Unix(int64(expiresAtSec), 0)
	return &cacheEntry, nil
}

// insertResourceCacheEntry inserts a new row into the resource_cache table.
// ResourceCacheId is ignored.
func insertResourceCacheEntry(e *ResourceCacheEntry, db *sql.DB) error {
	if e.Etag.Valid {
		_, err := db.Exec(`
      INSERT INTO resource_cache(url, fetch_sec, expires_at_sec, etag, data) VALUES(?, ?, ?, ?, ?)
      `, e.Url, e.FetchSec.Unix(), e.ExpiresAtSec.Unix(), e.Etag.String, e.Data)
		return err
	}
	_, err := db.Exec(`
      INSERT INTO resource_cache(url, fetch_sec, expires_at_sec, data) VALUES(?, ?, ?, ?)
      `, e.Url, e.FetchSec.Unix(), e.ExpiresAtSec.Unix(), e.Data)

	return err
}

var (
	// ErrRateLimited indicates that the resource cannot be fetched due to rate-limiting
	// constraints.
	ErrRateLimited = errors.New("cannot fetch resource - rate limited")

	// ErrFetchInternal indicates an error in the FetchResource routine.
	// More details should be logged.
	ErrFetchInternal = errors.New("fetch internal error - see logs")
)

// FetchResource attempts to fetch the resource indicated by url.
// FetchResource may return a result from the resource_cache table in db.
// See FetchOptions for fine grain control. Users would typically only need
// to set the `Now` field of FetchOptions.
func FetchResource(url string, db *sql.DB, opts FetchOptions) (string, error) {
	var (
		cacheEntry *ResourceCacheEntry
		err        error
	)

	if opts.SkipCache {
		goto ACTUAL_FETCH
	}

	// Consult the cache for the latest cached entry, if any.
	cacheEntry, err = latestResourceCacheEntry(url, db)
	if err != nil {
		return "", err
	}
	if cacheEntry == nil {
		goto ACTUAL_FETCH
	}

	// A cache entry exists - try to return immediately if the resource is not expired.
	if opts.Now.Before(cacheEntry.ExpiresAtSec) {
		return cacheEntry.Data, nil
	}

	// There is a condition not handled properly here.
	//
	// What if the cache entry is expired, and rate limiting prevents us from
	// issuing another GET request?
	// Given the configuration of resource_rate_limit_sec of 90 seconds, and
	// default_resource_expiration_sec of 120 seconds, this situtation would be rare.
	// However, if the resource is rate limited and expired (say, if the last fetch
	// resulted in a non-200 status), then ErrRateLimited will be returned.

ACTUAL_FETCH:
	lfa, err := lastFetchAttempt(url, db)
	if err != nil {
		return "", fmt.Errorf("cannot load latest fetch_attempt: %s", err)
	}
	// Between here and when a new FetchAttempt entry is written,
	// another instance of this binary may run and also issue a fetch for the URL.
	// This condition creates a potential for us to exceed rate limiting.
	// It is recommended to only run a single instance of this binary at a time to
	// prevent this race.
	// An alterntaive design is to hold a lock on the fetch_attempts table, but that
	// takes too long as the lock must be held over a HTTP request - way too long!
	// To ensure that only a single crawler executes at a time, use one of the many
	// job running solutions (see, for example,
	// https://guides.rubyonrails.org/active_job_basics.html#starting-the-backend)

	if !opts.IgnoreRateLimiting && lfa.FetchSec.Add(time.Duration(*resourceRateLimitSec)*
		time.Second).Before(opts.Now) {
		return "", ErrRateLimited
	}

	hc := &http.Client{
		Timeout: time.Duration(*fetchTimeoutSec) * time.Second,
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Add("User-Agent", *crawlerUserAgent)
	if cacheEntry != nil {
		if !opts.DoNotSendIfNoneMatchHeader && cacheEntry.Etag.Valid {
			req.Header.Add("If-None-Match", cacheEntry.Etag.String)
		}

		if !opts.DoNotSendIfModifiedSinceHeader {
			req.Header.Add("If-Modified-Since", cacheEntry.FetchSec.Format(http.TimeFormat))
		}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Ideally, insertFetchAttempt should occur after insertResourceCacheEntry,
	// as we read from the resource_cache prior to fetch_attempts. However,
	// we insertFetchAttempt here since it simplifies the control flow when the fetch
	// failed.
	err = insertFetchAttempt(&FetchAttempt{
		Url:        url,
		FetchSec:   opts.Now,
		StatusCode: resp.StatusCode}, db)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != 200 || resp.StatusCode != 304 {
		return "", fmt.Errorf("resource fetch failed (%d)", resp.StatusCode)
	}

	resourceExpiresAt := opts.Now.Add(
		time.Duration(*defaultResourceExpirationSec) * time.Second)
	if expr := resp.Header.Get(http.CanonicalHeaderKey("Expires")); expr != "" {
		exprTime, err := time.Parse(http.TimeFormat, expr)
		if err != nil {
			resourceExpiresAt = exprTime
		} else {
			log.Printf("Failed to parse Expires header %s: %s.", expr, err)
		}
	}

	if cc := resp.Header.Get(http.CanonicalHeaderKey("Cache-Control")); cc != "" {
		var ageSec int
		if _, err := fmt.Sscanf(cc, "max-age=%d", &ageSec); err != nil {
			log.Printf("Failed to parse Cache-Control header %s: %s.", cc, err)
		} else {
			resourceExpiresAt = opts.Now.Add(time.Duration(ageSec) * time.Second)
		}
	}

	if resp.StatusCode == 304 {
		if cacheEntry == nil {
			log.Printf("%s: 304 returned, but no If-None-Match/If-Modified-Since headers sent?", url)
			return "", ErrFetchInternal
		}

		_, err := db.Exec(`
             UPDATE resource_cache
               SET fetch_sec = ?, expires_at_sec = ?
               WHERE resource_cache_id = ?
             `, opts.Now.Unix(), resourceExpiresAt.Unix(), cacheEntry.ResourceCacheId)
		if err != nil {
			log.Printf("Failed to update resource_cache with new expires_at_usec for resource_cache_id %d: %s", cacheEntry.ResourceCacheId, err)
		}

		return cacheEntry.Data, nil
	}

	// StatusCode == 200

	bodyBuffer := new(strings.Builder)
	if _, err := io.Copy(bodyBuffer, resp.Body); err != nil {
		return "", fmt.Errorf("failed to read %s body: %s", url, err)
	}

	newCacheEntry := &ResourceCacheEntry{
		Url:          url,
		FetchSec:     opts.Now,
		ExpiresAtSec: resourceExpiresAt,
		Data:         bodyBuffer.String(),
	}

	if etag := resp.Header.Get(http.CanonicalHeaderKey("ETag")); etag != "" {
		newCacheEntry.Etag.String = etag
		newCacheEntry.Etag.Valid = true
	}
	err = insertResourceCacheEntry(newCacheEntry, db)
	if err != nil {
		log.Printf("Failed to insert new resource_cache entry %#v: %s", newCacheEntry, err)
	}

	return newCacheEntry.Data, nil
}

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
