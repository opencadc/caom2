# caom2micro
Common Archive Observation Model - micro-service implementations

- caom2-tap: CAOM-2.x TAP client to get observations, planes, or artifacts from a TAP service
- caom2-datalink-server: IVOA DataLink implementation for CAOM-2.x
- caom2-meta-server: simple metadata service that returns a single CAOM-2.x observation in various formats
- caom2-pkg-server: simple CAOM-2.x package service that generates a package (tar) with all artifacts of a single plane
- caom2-soda-server: IVOA SODA implementation for CAOM-2.x

Known shortcomings:
- the various server packages run using the cadc-uws-server SyncServlet and are currently hard-coded to 
use a PostgresJobPersistence
- the back-end TAP service resourceIdentifier is hard-coded to use the CADC; this needs to be configurable

