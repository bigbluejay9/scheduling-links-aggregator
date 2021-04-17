package main

import (
	"database/sql"
	"encoding/json"
  "fmt"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	crawlerOutputFile = flag.String("crawler_output_file", "/tmp/crawler_output.VERSION.sqlite", "The output file produced by the crawler. Be sure to provide the desired VERSION of the output.")
	output       = flag.String("output", "/tmp/parser_output.VERSION.sqlite", "The output file. 'VERSION' is replaced by the current unix epoch timestamp.")
)

// OutputSchema is the schema for the sqlite database written into the output file.
var OutputSchema = `
PRAGMA encoding = "UTF-8";
PRAGMA foreign_keys = ON;

-- Location, Schedule, and Slot file as SQL tables.
-- JSON definitions:
-- Location: https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#location-file
-- Schedule: https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#schedule-file
-- Slot: https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#slot-file
--
-- In general, fields are renamed from camelCase to snake_case.
-- Arrays fields are stored in a FILE-TYPE_FIELD table, and joined using the FILE-TYPE's primary key.
-- E.g. the array of telecom JSON objects in Location is stored in location_telecoms and joined on location_id.

-- A Location object.
-- https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#location-file
CREATE TABLE locations(
    location_id INTEGER PRIMARY KEY,

    -- Note that since we aggregate data from multiple publishers, id
    -- is not guaranteed to be unique like the spec says.
    id TEXT NOT NULL,
    name TEXT NOT NULL,

    description TEXT NOT NULL
);

-- Location.telecom object.
CREATE TABLE location_telecoms(
    location_telecom_id INTEGER PRIMARY KEY,

    system TEXT NOT NULL,
    value TEXT NOT NULL,

    location_id NOT NULL
      REFERENCES locations(location_id)
        ON DELETE CASCADE
);

-- Location.address object.
CREATE TABLE location_addresses(
    location_address_id INTEGER PRIMARY KEY,

    -- ", " joined strings of the 'Location.address.line' JSON array.
    lines TEXT NOT NULL,

    city TEXT NOT NULL,
    state TEXT NOT NULL,
    postal_code TEXT NOT NULL,
    district TEXT NOT NULL,

    location_id NOT NULL
      REFERENCES locations(location_id)
        ON DELETE CASCADE
);

-- Location.position object.
CREATE TABLE location_positions(
    location_position_id INTEGER PRIMARY KEY,

    latitude REAL NOT NULL,
    longitude REAL NOT NULL,

    location_id NOT NULL
      REFERENCES locations(location_id)
        ON DELETE CASCADE
);

-- Location.identifier object.
CREATE TABLE location_identifiers(
    location_identifier_id INTEGER PRIMARY KEY,

    system TEXT NOT NULL,
    value TEXT NOT NULL,

    location_id NOT NULL
      REFERENCES locations(location_id)
        ON DELETE CASCADE
);

-- A Schedule object.
-- https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#schedule-file
CREATE TABLE schedules(
    schedule_id INTEGER PRIMARY KEY,

    -- Note that since we aggregate data from multiple publishers, id
    -- is not guaranteed to be unique like the spec says.
    id TEXT NOT NULL,

    -- Although actor is a JSON array. It can only have one object with a string "reference" field.
    -- We put the reference string here directly instead of another child table.
    actor_reference TEXT NOT NULL
);

-- Schedule.serviceType object.
CREATE TABLE schedule_service_types(
    schedule_service_type_id INTEGER PRIMARY KEY,

    system TEXT NOT NULL,
    code TEXT NOT NULL,
    display TEXT NOT NULL,

    schedule_id NOT NULL
      REFERENCES schedules(schedule_id)
        ON DELETE CASCADE
);

-- Schedule.extension object.
CREATE TABLE schedule_extensions(
    schedule_extension_id INTEGER PRIMARY KEY,

    url TEXT NOT NULL,

    -- value_integer will not be null if url is
    -- "http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-dose"
    value_integer INTEGER,

    -- system, code, and display will not be null if url is
    -- "http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-product"
    system TEXT,
    code TEXT,
    display TEXT,

    schedule_id NOT NULL
      REFERENCES schedules(schedule_id)
        ON DELETE CASCADE
);

-- A Slot object.
-- https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#slot-file
CREATE TABLE slots(
    slot_id INTEGER PRIMARY KEY,

    -- Note that since we aggregate data from multiple publishers, id
    -- is not guaranteed to be unique like the spec says.
    id TEXT NOT NULL,

    -- Although schedule is a JSON object, it can only have one string "reference" field.
    -- We put the reference string here directly instead of another child table.
    schedule_reference TEXT NOT NULL,

    status TEXT NOT NULL,

    -- 'start' field as seconds since Unix epoch.
    start_sec INTEGER NOT NULL,

    -- 'end' field as seconds since Unix epoch.
    end_sec INTEGER NOT NULL
);

-- Slot.extension object.
CREATE TABLE slot_extensions(
    slot_extension_id INTEGER PRIMARY KEY,

    url TEXT NOT NULL,

    -- value_url will not be null if url is
    -- "http://fhir-registry.smarthealthit.org/StructureDefinition/booking-deep-link"
    value_url TEXT,

    -- value_string will not be null if url is
    -- "http://fhir-registry.smarthealthit.org/StructureDefinition/booking-phone"
    value_string TEXT,

    -- value_integer will not be null if url is
    -- "http://fhir-registry.smarthealthit.org/StructureDefinition/slot-capacity"
    value_integer TEXT,

    slot_id NOT NULL
      REFERENCES slots(slot_id)
        ON DELETE CASCADE
);
`

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

// ParseLocations parses all rows in the locations table into LocationFiles.
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

// ParseSchedules parses all rows in the schedules table into ScheduleFiles.
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

// ParseSlots parses all rows in the slots table into SlotFiles.
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

	odb, outputFilename, err := OpenOutput(*output, OutputSchema)
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
	log.Printf("Wrote output to %s.", outputFilename)

	return nil
}

func main() {
	if err := Run(); err != nil {
		log.Fatal(err)
	}
}
