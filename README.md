# Scheduling Links Aggregator

Binaries for aggregating Slot Publisher data specified for https://github.com/smart-on-fhir/smart-scheduling-links.

## Overview

This repository contains tools that can be used to download and parse data provided by Slot Publishers in the format of
https://github.com/smart-on-fhir/smart-scheduling-links.

- Crawler: given a list of Manifest URLs, specified by the `--manifest_urls` flag, the crawler will download all
  manifest files and location, schedule, and slot files specified by those manifests.
  The output is written into a SQLite database file specified by the `--output` flag. The output file's schema can be
  found [here](crawler/crawler.go#L26).
- Parser: given the output of the crawler, the parser parses the JSON files and writes the output to a SQLite database
  file specified the `--output` flag. The output file's schema can be found [here](parser/parser.go#L23).
- Validator: validates that JSON files conform to the scheduling links spec
  https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md. `./validator help` for more info.

It is intended for these files to be published (e.g. via S3 or equivalent service) so that front ends can read and
display vaccination data to end users. For frontend clients that prefer raw JSON, the output of the crawler is
appropriate. For frontend clients that prefer structured data, the output of the parser is appropriate.

## Usage

Build binaries
```sh
$ rake
```

Create a seed `manifest_files` used for testing and development
```sh
$ rake seed
```

Run crawler
```sh
$ rake && bin/crawler --manifest_urls=bin/manifest_urls
```

### Crawler

**Please do NOT use the Crawler without a caching proxy!**

The crawler issues GET requests to the specified manifest URLs, and any resources those manifest files name.
It is imperative to put a caching HTTP(s) proxy in front of the crawler in order to cache requests between multiple
crawler invocations.
