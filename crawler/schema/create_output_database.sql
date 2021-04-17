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
