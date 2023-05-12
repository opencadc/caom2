# caom2db
Common Archive Observation Model - database implementation

- caom2-access-control: access control, plugin code to use rules to add group URIs to observations
- caom2-persist: common interface code
- caom2persistence: database interface library
- caom2-artifact-store-si: ArtifactStore (caom2-persist) implementation for use with https://github.com/opencadc/storage-inventory

- caom2-repo : simple client library for use in caom2harvester to support harvest from a repository service
- caom2-repo-server: library for implementing a CAOM-2.x repository web service supporting curation of a CAOM database
- caom2-test-repo: integration test suite for a caom2repo service

- caom2harvester: command-line application for incremental harvesting from one caom2 database and writes to another
- caom2-remove: command-line application to remove a collection from a caom2 database
- caom2-collection-validator: comand-line application to run CAOM validation on a collection in a caom2 database
 
- caom2-artifact-sync : library for implementing an application that maintains a local copy of artifacts (files) for a data collection
- caom2-artifact-discover: application to run the discover mode of caom2-artifact-sync, container build, BETA
- caom2-artifact-download: application to run the download mode of caom2-artifact-sync, container build, BETA

Known shortcomings: 

- documentation is somewhat non-existent :-)

