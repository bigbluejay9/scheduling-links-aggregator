PRAGMA encoding = "UTF-8";
PRAGMA foreign_keys = ON;

-- Resource Cache is a table of fetched URLs and their contents.
CREATE TABLE resource_cache(
    -- The URL of the resource.
    url TEXT NOT NULL,

    -- When the fetch occurred. Seconds since Unix epoch.
    fetch_sec INTEGER NOT NULL,

    -- When the resource is set to expired in seconds since Unix epoch.
    -- As directed by Cache-Control, or Expires header.
    expires_at_sec INTEGER NOT NULL,

    -- The ETag value of the resource. May be empty.
    etag TEXT,

    -- The contents of the resource.
    data TEXT NOT NULL,
);
CREATE UNIQUE INDEX resource_cache_by_url ON resource_cache(url);

-- Known Manifest files, i.e. public URLs of Manifest files from various publishers.
CREATE TABLE known_manifests(
    known_manifest_id INTEGER PRIMARY KEY,

    -- The full URL of the manifest file, e.g. http://example.org/path/$bulk-publish
    url TEXT NOT NULL
);
CREATE UNIQUE INDEX known_manifests_by_url ON known_manifests(url);

-- Manifest file fetch attempt.
-- Each known manifest may have multiple manifest fetch attempts associated.
-- 
-- If a known manifest has no associated manifest file, the URL has not yet been polled.
CREATE TABLE manifest_fetches(
    manifest_fetch_id INTEGER PRIMARY KEY,

    -- The URL of the manifest file.
    url TEXT NOT NULL,

    -- The associated known manifest file.
    known_manifest_id NOT NULL
      REFERENCES known_manifests(known_manifest_id)
        ON DELETE CASCADE,

    -- Unix seconds when this fetch was attempted.
    read_sec INTEGER NOT NULL,

		-- The HTTP status code of the fetch
		fetch_status_code INTEGER NOT NULL,

    -- When reading files, a Cache-Control: max-age=XX header may be set to indicate
    -- the suggested next polling time. If the header is set, the value is extracted here.
    -- If unset, the suggested polling interval is 1-5 minutes.
    -- Polling interval documentation (at end):
    -- https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#quick-start-guide
    -- Cache-Control documentation:
    -- https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#performance-considerations
    polling_hint_sec INTEGER,

    -- The manifest file's contents. Empty if the fetch failed.
    -- See https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#manifest-file
    -- for file content definition.
    contents TEXT
);

-- Location file fetch attempt.
CREATE TABLE location_fetches(
    location_fetch_id INTEGER PRIMARY KEY,

    -- The URL of the location file.
    url TEXT NOT NULL,

    -- The manifest fetch this location fetch is associated with.
    manifest_fetch_id NOT NULL
      REFERENCES manifest_fetches(manifest_fetch_id)
        ON DELETE CASCADE,

    -- Unix seconds when this fetch was attempted.
    read_sec INTEGER NOT NULL,

		-- The HTTP status code of the fetch
		fetch_status_code INTEGER NOT NULL,

    -- When reading files, a Cache-Control: max-age=XX header may be set to indicate
    -- the suggested next polling time. If the header is set, the value is extracted here.
    -- If unset, the suggested polling interval is 1-5 minutes.
    polling_hint_sec INTEGER,

    -- The location file's contents. Empty if the fetch failed.
    -- See https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#location-file
    -- for file content definition.
    contents TEXT
);

-- Schedule file fetch attempt.
CREATE TABLE schedule_fetches(
    schedule_fetch_id INTEGER PRIMARY KEY,

    -- The URL of the schedule file.
    url TEXT NOT NULL,

    -- The manifest fetch this location fetch is associated with.
    manifest_fetch_id NOT NULL
      REFERENCES manifest_fetches(manifest_fetch_id)
        ON DELETE CASCADE,

    -- Unix seconds when this fetch was attempted.
    read_sec INTEGER NOT NULL,

		-- The HTTP status code of the fetch
		fetch_status_code INTEGER NOT NULL,

    -- When reading files, a Cache-Control: max-age=XX header may be set to indicate
    -- the suggested next polling time. If the header is set, the value is extracted here.
    -- If unset, the suggested polling interval is 1-5 minutes.
    polling_hint_sec INTEGER,

    -- The schedule file's contents. Empty if the fetch failed.
    -- See https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#schedule-file
    -- for file content definition.
    contents TEXT
);

-- Slot file fetch attempt.
CREATE TABLE slot_fetches(
    slot_fetch_id INTEGER PRIMARY KEY,

    -- The URL of this slot file.
    url TEXT NOT NULL,

    -- The manifest fetch this location fetch is associated with.
    manifest_fetch_id NOT NULL
      REFERENCES manifest_fetches(manifest_fetch_id)
        ON DELETE CASCADE,

    -- Unix seconds when this fetch was attempted.
    read_sec INTEGER NOT NULL,

		-- The HTTP status code of the fetch
		fetch_status_code INTEGER NOT NULL,

    -- When reading files, a Cache-Control: max-age=XX header may be set to indicate
    -- the suggested next polling time. If the header is set, the value is extracted here.
    -- If unset, the suggested polling interval is 1-5 minutes.
    polling_hint_sec INTEGER,

    -- The slot file's contents. Empty if the fetch failed.
    -- See https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#slot-file
    -- for file content definition.
    contents TEXT
);
