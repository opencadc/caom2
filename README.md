# caom2db  
Common Archive Observation Model - database implementation

- caom2persistence: database interface library
- caom2-repo-server: library for implementing a CAOM-2.x repository web service supporting 
  curation of a CAOM database
- caom2-test-repo: integration test suite for a caom2repo service
- caom2harvester: command-line application for incremental harvesting from one caom2 database and writes to another

Known shortcomings: 

- caom2repo hard-coded to use a Sybase backend
- Sybase persistence does not store any computed metadata
- PostgreSQL persistence always stores all computed metadata
- documentation is somewhat non-existent :-)

