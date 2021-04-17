PRAGMA encoding = "UTF-8";
PRAGMA foreign_keys = ON;

CREATE TABLE fetch_attempts(
    -- The URL of the resource.
    url TEXT NOT NULL,

    -- When the fetch was issued. Seconds since Unix epoch.
    fetch_sec INTEGER NOT NULL,

    -- The HTTP status code of the fetch.
    status_code INTEGER NOT NULL
);
CREATE INDEX fetch_attempts_by_url ON fetch_attempts(url);

-- Resource Cache is a table of fetched URLs and their contents.
CREATE TABLE resource_cache(
    resource_cache_id INTEGER PRIMARY KEY,

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
    data TEXT NOT NULL
);
CREATE UNIQUE INDEX resource_cache_by_url ON resource_cache(url);

-- Known Manifest files, i.e. public URLs of Manifest files from various publishers.
CREATE TABLE known_manifests(
    known_manifest_id INTEGER PRIMARY KEY,

    -- The full URL of the manifest file, e.g. http://example.org/path/$bulk-publish
    url TEXT NOT NULL
);
CREATE UNIQUE INDEX known_manifests_by_url ON known_manifests(url);
