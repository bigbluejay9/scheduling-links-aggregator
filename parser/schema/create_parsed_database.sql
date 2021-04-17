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
