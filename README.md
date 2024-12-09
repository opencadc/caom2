# OpenCADC CAOM System

This is the Common Archive Observation Model (CAOM) software to manage
metadata in an astronimical data archive.


# software components

## libraries
- caom2: java data model implementation
- caom2-compute: metadata computation and validation
- caom2-viz: diagnostic tool for visualizing positional coverage metadata (inrternal)
- caom2-validator: command-line tool to run standard validation code
- fits2caom: OBSOLETE
- caom2-artifact-resolvers: StorageResolver implementations for different Artifact URI schemes
- caom2-dm: old home for CAOM data model - see: https://github.com/opencadc/CAOM
- caom2persistence: database interface library
- caom2-persist: common interface code
- caom2-repo : simple client library to support metadata-sync from a repository service
- caom2-test-repo: integration test suite for a caom2-repo-server service
- caom2-access-control: access control, plugin code to use rules to add group URIs to observations (DEPRECATED?)
- caom2-repo-server: library for implementing a CAOM-2.x repository web service supporting curation of a CAOM database
- caom2-artifact-sync : library for implementing application(s) that maintains a local copy of artifacts (files) for a data collection (DEPRECATED: code will be moved into applications when feasible)
- caom2-artifact-store-si: ArtifactStore (caom2-persist) implementation for use with https://github.com/opencadc/storage-inventory

## applications and services
- caom2-artifact-discover: application to run the discover mode of caom2-artifact-sync (docker container)
- caom2-artifact-download: application to run the download mode of caom2-artifact-sync (docker container)
- caom2-remove: command-line application to remove a collection from a caom2 database
- caom2-collection-validator: command-line application to run CAOM validation on a collection in a caom2 database
- caom2harvester: caom-metadata-sync command-line application for incremental harvesting from one caom2 database to another (DEPRECATED)
- icewind: caom-metadata-sync process (docker container)
- torkeep: caom2 repository service (docker container)

