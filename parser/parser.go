package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	crawlerOutputFile = flag.String("crawler_output_file", "crawler_output.db", "The output file produced by the crawler.")

	outputSchema = flag.String("output_schema_file", "schema/create_parsed_database.sql", "The output file schema.")
	output       = flag.String("output", "output.db", "The output file.")
)

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

func ParseLocations(input *sql.DB) ([]LocationFile, error) {
	var results []LocationFile
	rows, err := input.Query("SELECT contents FROM locations")
	if err != nil {
		return []LocationFile{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var contents string
		if err := rows.Scan(&contents); err != nil {
			return []LocationFile{}, err
		}

		for _, c := range strings.Split(contents, "\n") {
			var r LocationFile
			if err := json.Unmarshal([]byte(c), &r); err != nil {
				return []LocationFile{}, err
			}
			results = append(results, r)
		}
	}

	return results, rows.Err()
}

func ParseSchedules(input *sql.DB) ([]ScheduleFile, error) {
	var results []ScheduleFile
	rows, err := input.Query("SELECT contents FROM schedules")
	if err != nil {
		return []ScheduleFile{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var contents string
		if err := rows.Scan(&contents); err != nil {
			return []ScheduleFile{}, err
		}

		for _, c := range strings.Split(contents, "\n") {
			var r ScheduleFile
			if err := json.Unmarshal([]byte(c), &r); err != nil {
				return []ScheduleFile{}, nil
			}
			results = append(results, r)
		}
	}

	return results, rows.Err()
}

func ParseSlots(input *sql.DB) ([]SlotFile, error) {
	var results []SlotFile
	rows, err := input.Query("SELECT contents FROM schedules")
	if err != nil {
		return []SlotFile{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var contents string
		if err := rows.Scan(&contents); err != nil {
			return []SlotFile{}, err
		}

		for _, c := range strings.Split(contents, "\n") {
			var r SlotFile
			if err := json.Unmarshal([]byte(c), &r); err != nil {
				return []SlotFile{}, err
			}
			results = append(results, r)
		}
	}

	return results, rows.Err()
}

func Run() error {
	crawlerOutput, err := sql.Open("sqlite3", *crawlerOutputFile)
	if err != nil {
		return err
	}
	defer crawlerOutput.Close()

	odb, err := OpenOutput(*output, *outputSchema)
	if err != nil {
		return err
	}
	defer odb.Close()

	parseStart := time.Now()
	locations, err := ParseLocations(crawlerOutput)
	if err != nil {
		return err
	}

	schedules, err := ParseSchedules(crawlerOutput)
	if err != nil {
		return err
	}

	slots, err := ParseSlots(crawlerOutput)
	if err != nil {
		return err
	}

	log.Printf("Parsed %d locations, %d schedules, %d slots in %s",
		len(locations), len(schedules), len(slots),
		time.Since(parseStart).String())
	log.Printf("Wrote output to %s.", *output)

	return nil
}

func main() {
	if err := Run(); err != nil {
		log.Fatal(err)
	}
}
