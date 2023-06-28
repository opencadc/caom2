# caom2service

Common Archive Observation Model service implementation libraries. The master branch is now updated to work with DALI-1.1 (cadc-dali-1.1) and TAP-1.1 (cadc-tap-server-1.1). The other services that use the CAOM TAP service also assume TAP-1.1.

## CAOM TAP service

* caom2-tap: basic CAOM-2.x TAP client to get observations, planes, or artifacts from a TAP service
* caom2-tap-server: CAOM-2.x TAP service implementation for use with cadc-tap-server
* argus: complete container-based CAOM TAP service

## CAOM services that use the CAOM TAP service

* caom2-datalink-server: IVOA DataLink implementation for CAOM-2.x
* caom2-meta-server: simple metadata service that returns a single CAOM-2.x observation in various formats
* caom2-pkg-server: simple CAOM-2.x package service that generates a package (tar) with all artifacts of a single plane
* caom2-soda-server: IVOA SODA implementation for CAOM-2.x
* ???: complete container-based CAOM DataLink service (COMING SOON)

These server-side libraries make use of the cadc-uws-server package and provide a JobRunner implementation to use 
with that framework. They use the caom2-tap client library to query a CAOM TAP service.
