# OpenCADC CAOM System

This is the Common Archive Observation Model (CAOM) software to manage
metadata in an astronimical data archive.

The CAOM data model development is here: https://github.com/opencadc/CAOM

# software components

## libraries
- caom2: data model implementation
- caom2-compute: metadata computation and validation
- caom2-artifact-resolvers: StorageResolver implementations for different Artifact URI schemes
- caom2persistence: database library PLAN: rename to caom2-db for CAOM-2.5
- caom2-persist: common interface code PLAN: merge into caom2
- caom2-repo : simple client library to support metadata-sync from a repository service
- caom2-repo-server: (DEPRECATED) see torkeep
- caom2-test-repo: integration test suite for a caom2-repo-server service TODO: merge into torkeep
- caom2-access-control: (DEPRECATED) access control plugin code to use rules to add group URIs to observations TODO: merge into torkeep
- caom2-artifact-store-si: ArtifactStore (caom2-persist) implementation for use with https://github.com/opencadc/storage-inventory
- caom2-artifact-sync : (DEPRECATED?) library for implementing application(s) that maintains a local copy of artifacts (files) for a data collection TODO: code will be moved into applications when feasible

- caom2-tap: basic CAOM-2.x TAP client to get observations, planes, or artifacts from a TAP service
- caom2-tap-server: (DEPREACTED) see argus
- caom2-datalink-server: (DEPRECATED) see bifrost
- caom2-meta-server: (DEPRECATED) TODO: make read-only torkeep a viable alternative
- caom2-pkg-server: simple CAOM-2.x package service that generates tar|zip packages TODO: convert to service (docker container)
- caom2-soda-server: IVOA SODA implementation for CAOM-2.x

## tools
- caom2-validator: command-line tool to run standard validation code (internal utility)
- caom2-collection-validator: command-line application to run CAOM validation on a collection in a caom2 database
- caom2-artifact-discover: application to run the discover mode of caom2-artifact-sync (docker container)
- caom2-artifact-download: application to run the download mode of caom2-artifact-sync (docker container)
- caom2harvester: (DEPRECATED) see icewind
- caom2-remove: command-line application to remove a collection from a caom2 database
- fits2caom2: OBSOLETE
- icewind: caom2 metadata sync process (docker container)

## services
- torkeep: caom2 repository service (docker container)
- argus: CAOM TAP service (docker container)
- bifrost: IVOA DataLink service for CAOM (docker container)
- TBD: CAOM package service (from caom2-pkg-server)
- TBD: CAOM SODA service?

# TODO
1. restore metadata-validate mode in `icewind` and combine it with retry mode so it is validate-and-repair
2. add artifact-validate mode to `caom2-artifact-discover`
3. rename `caom2-artifact-discover` and `caom2-artifact-download`?


