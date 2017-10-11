# caom2db
Common Archive Observation Model - database implementation

- caom2-persist: common code
- caom2persistence: database interface library

- caom2-repo : simple client library for use in caom2harvester to support harvest from a repository service
- caom2-repo-server: library for implementing a CAOM-2.x repository web service supporting curation of a CAOM database
- caom2-test-repo: integration test suite for a caom2repo service

- caom2harvester: command-line application for incremental harvesting from one caom2 database and writes to another
- caom2-artifact-sync : library for implementing an application that maintains a local copy of artifacts (files) for a data collection

Known shortcomings: 

- Sybase persistence does not store any computed metadata (deprecated, won't fix)
- documentation is somewhat non-existent :-)

