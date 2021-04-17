# Scheduling Links Aggregator

Binaries for aggregating Slot Publisher data specified for https://github.com/smart-on-fhir/smart-scheduling-links.

## Overview

This repository contains tools that can be used to download and parse data provided by Slot Publishers in the format of
https://github.com/smart-on-fhir/smart-scheduling-links. There are two main components:

- Crawler: given a list of Manifest URLs, specified by the `--manifest_urls` flag, the crawler will download all
  manifest files and location, schedule, and slot files specified by those manifests.
  The output is written into a SQLite database file specified by the `--output` flag. The output file's schema can be
  found [here](crawler/crawler.go#L24).
- Parser: given the output of the crawler, the parser parses the JSON files and writes the output to a SQLite database
  file specified the `--output` flag. The output file's schema can be found [here](parser/parser.go#L21).

It is the intended for these files to be published (e.g. via S3 equvialent service) so that front ends can read and
display vaccination data to end users. For frontend clients that prefer raw JSON, the output of the crawler is
appropriate. For frontend clients that prefer structured data, the output of the parser is appropriate.

## Usage
