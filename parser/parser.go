package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	crawlerOutputFile = flag.String("crawler_output_file", "", "The output file produced by the crawler. If empty, finds the latest crawler output matching '/tmp/crawler_output.*.sqlite'.")
	output            = flag.String("output", "/tmp/parser_output.VERSION.sqlite", "The output file. 'VERSION' is replaced by the current unix epoch timestamp.")
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
    value_integer INTEGER,

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

/* Serialization

   Serialization routines assumes that the data is well formed.
   Invalid data is silently dropped.
*/

/* Location File Serialization */
// Serializes LocationFileTelecom and writes to the location_telecoms table.
func (l *LocationFileTelecom) Write(db *sql.DB, locationId int64) error {
	_, err := db.Exec(
		"INSERT INTO location_telecoms (system, value, location_id) VALUES (?, ?, ?)",
		l.System, l.Value, locationId)
	return err
}

// Serializes LocationFileAddress and writes to the location_addresses table.
func (l *LocationFileAddress) Write(db *sql.DB, locationId int64) error {
	_, err := db.Exec(`
      INSERT INTO location_addresses
        (lines, city, state, postal_code, district, location_id)
      VALUES (?, ?, ?, ?, ?, ?)`,
		strings.Join(l.Line, ", "),
		l.City, l.State, l.PostalCode, l.District, locationId)
	return err
}

// Serializes LocationFilePosition and writes to the location_positions table.
func (l *LocationFilePosition) Write(db *sql.DB, locationId int64) error {
	_, err := db.Exec(
		`INSERT INTO location_positions
        (latitude, longitude, location_id)
      VALUES (?, ?, ?)`,
		l.Latitude, l.Longitude, locationId)
	return err
}

// Serializes LocationFileIdentifier and writes to the location_identifiers table.
func (l *LocationFileIdentifier) Write(db *sql.DB, locationId int64) error {
	_, err := db.Exec(
		`INSERT INTO location_identifiers
        (system, value, location_id)
      VALUES (?, ?, ?)`,
		l.System, l.Value, locationId)
	return err
}

// Serializes LocationFile and writes to the locations table.
func (l *LocationFile) Write(db *sql.DB) error {
	res, err := db.Exec(
		"INSERT INTO locations (id, name, description) VALUES (?, ?, ?)",
		l.Id, l.Name, l.Description)
	if err != nil {
		return err
	}

	locationId, err := res.LastInsertId()
	if err != nil {
		return err
	}

	for _, v := range l.Telecom {
		if err := v.Write(db, locationId); err != nil {
			return err
		}
	}

	if err := l.Address.Write(db, locationId); err != nil {
		return err
	}
	if err := l.Position.Write(db, locationId); err != nil {
		return err
	}

	for _, v := range l.Identifier {
		if err := v.Write(db, locationId); err != nil {
			return err
		}
	}

	return nil
}

/* Schedule File Serialization */

// Serializes ScheduleFileExtension and writes to the schedule_extensions table.
func (s *ScheduleFileExtension) Write(db *sql.DB, scheduleId int64) error {
	if s.Url == "http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-dose" {
		_, err := db.Exec(
			`INSERT INTO schedule_extensions
        (url, value_integer, schedule_id) VALUES (?, ?, ?)`,
			s.Url, s.ValueInteger, scheduleId)
		return err
	} else if s.Url == "http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-product" {
		_, err := db.Exec(
			`INSERT INTO schedule_extensions
        (url, system, code, display, schedule_id) VALUES (?, ?, ?, ?, ?)`,
			s.Url, s.ValueCoding.System, s.ValueCoding.Code, s.ValueCoding.Display, scheduleId)
		return err
	} else {
		log.Print("Ignoring invalid ScheduleFileExtension - unrecognized Url: %#v", s)
		// Invalid Extension - do not write.
		return nil
	}
}

// Serializes ScheduleFileServiceType and writes to the schedule_service_types table.
func (s *ScheduleFileServiceType) Write(db *sql.DB, scheduleId int64) error {
	_, err := db.Exec(
		`INSERT INTO schedule_service_types
        (system, code, display, schedule_id)
      VALUES (?, ?, ?, ?)`,
		s.System, s.Code, s.Display, scheduleId)
	return err
}

// Serializes ScheduleFile and writes to the schedules table.
func (s *ScheduleFile) Write(db *sql.DB) error {
	// Actor must be an array with a single object containing a JSON object with the
	// field 'reference'.
	if len(s.Actor) == 0 {
		log.Print("Ignoring bad ScheduleFile - missing actor.reference: %#v", s)
		return nil
	}

	res, err := db.Exec(
		"INSERT INTO schedules (id, actor_reference) VALUES (?, ?)",
		s.Id, s.Actor[0].Reference)
	if err != nil {
		return err
	}

	scheduleId, err := res.LastInsertId()
	if err != nil {
		return err
	}

	for _, v := range s.ServiceType {
		if err := v.Write(db, scheduleId); err != nil {
			return err
		}
	}

	for _, v := range s.Extension {
		if err := v.Write(db, scheduleId); err != nil {
			return err
		}
	}

	return nil
}

/* Slot File Serialization */

const (
	ISO8601TimeFormat = "2006-01-02T15:04:05Z0700"
)

// Serializes SlotFileExtension and writes to the slot_extensions table.
func (s *SlotFileExtension) Write(db *sql.DB, slotId int64) error {
	if s.Url == "http://fhir-registry.smarthealthit.org/StructureDefinition/booking-deep-link" {
		_, err := db.Exec(
			`INSERT INTO slot_extensions
        (url, value_url, slot_id) VALUES (?, ?, ?)`,
			s.Url, s.ValueUrl, slotId)
		return err
	} else if s.Url == "http://fhir-registry.smarthealthit.org/StructureDefinition/booking-phone" {
		_, err := db.Exec(
			`INSERT INTO slot_extensions
        (url, value_string, slot_id) VALUES (?, ?, ?)`,
			s.Url, s.ValueString, slotId)
		return err
	} else if s.Url == "http://fhir-registry.smarthealthit.org/StructureDefinition/slot-capacity" {
		_, err := db.Exec(
			`INSERT INTO slot_extensions
        (url, value_integer, slot_id) VALUES (?, ?, ?)`,
			s.Url, s.ValueInteger, slotId)
		return err
	} else {
		// Invalid Extension - do not write.
		log.Print("Ignoring invalid SlotFileExtension - unrecognized Url: %#v", s)
		return nil
	}
}

// Serializes SlotFile and writes to the slots table.
func (s *SlotFile) Write(db *sql.DB) error {
	start, err := time.Parse(ISO8601TimeFormat, s.Start)
	if err != nil {
		log.Printf("Ignoring bad ISO8601 timestamp in SlotFile.Start: %#v", s)
		start = time.Time{}
	}

	end, err := time.Parse(ISO8601TimeFormat, s.End)
	if err != nil {
		log.Printf("Ignoring bad ISO8601 timestamp in SlotFile.End: %#v", s)
		end = time.Time{}
	}

	res, err := db.Exec(`INSERT INTO
      slots (id, schedule_reference, status, start_sec, end_sec)
      VALUES (?, ?, ?, ?, ?)`,
		s.Id, s.Schedule.Reference, s.Status, start.Unix(), end.Unix())
	if err != nil {
		return err
	}

	slotId, err := res.LastInsertId()
	if err != nil {
		return err
	}

	for _, v := range s.Extension {
		if err := v.Write(db, slotId); err != nil {
			return err
		}
	}

	return nil
}

/* Program */

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

// ReadFileHandleLine executes query on input. The query must select
// two string columns. The second string column is then split by '\n' and
// passed into handle. Any errors returned by handle immediately terminates
// the read and is returned by ReadFileHandleLine.
func ReadFileHandleLine(input *sql.DB, query string,
	handle func(int, []byte) error) error {
	rows, err := input.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var url, contents string
		if err := rows.Scan(&url, &contents); err != nil {
			return err
		}
		log.Printf("Processing %s.", url)

		for index, c := range strings.Split(contents, "\n") {
			if err := handle(index, []byte(c)); err != nil {
				return err
			}
		}
	}

	return rows.Err()
}

func Run() error {
	inputFile := *crawlerOutputFile
	if inputFile == "" {
		pattern := "/tmp/crawler_output.*.sqlite"
		files, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("cannot find crawler output file matching '%s': %s", pattern, err)
		}
		if len(files) == 0 {
			return fmt.Errorf("cannot find crawler output file matching '%s': did you run the crawler?", pattern)
		}
		sort.Strings(files)
		inputFile = files[len(files)-1]
	} else {
		_, err := os.Stat(inputFile)
		if os.IsNotExist(err) {
			return fmt.Errorf("%s does not exist", inputFile)
		}
	}

  log.Printf("Opening input file %s.", inputFile)
	crawlerOutput, err := sql.Open("sqlite3", inputFile)
	if err != nil {
		return err
	}
	defer crawlerOutput.Close()

	odb, outputFilename, err := OpenOutput(*output, OutputSchema)
	if err != nil {
		return err
	}
	defer odb.Close()

	start := time.Now()
	log.Print("Parsing locations")
	if err := ReadFileHandleLine(
		crawlerOutput, "SELECT url, contents FROM locations",
		func(lineNumber int, line []byte) error {
			var r LocationFile
			if err := json.Unmarshal(line, &r); err != nil {
				log.Printf("Unable to unmarshal line %d << %s >> %s", lineNumber, string(line), err)
				return nil
			}
			return r.Write(odb)
		}); err != nil {
		return err
	}

	log.Print("Parsing schedules")
	if err := ReadFileHandleLine(
		crawlerOutput, "SELECT url, contents FROM schedules",
		func(lineNumber int, line []byte) error {
			var r ScheduleFile
			if err := json.Unmarshal(line, &r); err != nil {
				log.Printf("Unable to unmarshal line %d: %s.", lineNumber, string(line))
				return nil
			}
			return r.Write(odb)
		}); err != nil {
		return err
	}

	log.Print("Parsing slots")
	if err := ReadFileHandleLine(
		crawlerOutput, "SELECT url, contents FROM slots",
		func(lineNumber int, line []byte) error {
			var r SlotFile
			if err := json.Unmarshal(line, &r); err != nil {
				log.Printf("Unable to unmarshal line %d: %s.", lineNumber, string(line))
				return nil
			}
			return r.Write(odb)
		}); err != nil {
		return err
	}

	log.Printf("Parsed and wrote in %s",
		time.Since(start).String())
	log.Printf("Wrote output to %s.", outputFilename)

	return nil
}

func main() {
	if err := Run(); err != nil {
		log.Fatal(err)
	}
}
