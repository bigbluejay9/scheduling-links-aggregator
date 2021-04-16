package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	dbFile = flag.String("db_file", "/tmp/crawler.db", "The extractor database file.")
	_      = fmt.Println
)

func main() {
	db, err := sql.Open("sqlite3", *dbFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	knownManifests, err := LoadKnownManifests(db)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Inspecting %d manifests.\n", len(knownManifests))

	// knownManifestToFetch maps KnownManifest -> whether the file should be fetched.
	var (
		knownManifestToFetch map[KnownManifest]bool = make(map[KnownManifest]bool)
		now                  time.Time              = time.Now()
	)

	for _, m := range knownManifests {
		mf, err := m.LastManifestFetch(db, false /*onlySuccess*/)
		if err != nil && err != sql.ErrNoRows {
			log.Fatal(err)
		}
		knownManifestToFetch[m] = mf.ShouldFetchManifest(now)
	}

	log.Printf("Manifests to fetch\n%#v", knownManifestToFetch)
}
