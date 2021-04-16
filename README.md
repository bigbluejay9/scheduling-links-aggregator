# Scheduling Links Aggregator

An aggregator for 'Slot Consumer's of https://github.com/smart-on-fhir/smart-scheduling-links.

## Purpose

A tool for Slot Consumers to reliably load Slot Publisher's data. When deployed, the tool will query known manifest
files and dump the aggregation of all manifest files into an output file. The tool respects rate-limiting and caching
rules indicated by Slot Publishers.

## Usage
