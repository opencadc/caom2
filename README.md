# caom2service

Common Archive Observation Model service implementation libraries.

## CAOM TAP service

* caom2-tap: basic CAOM-2.x TAP client to get observations, planes, or artifacts from a TAP service
* caom2-tap-server: CAOM-2.x TAP service implementation for use with cadc-tap-server

## CAOM services that use the CAOM TAP service

* caom2-datalink-server: IVOA DataLink implementation for CAOM-2.x
* caom2-meta-server: simple metadata service that returns a single CAOM-2.x observation in various formats
* caom2-pkg-server: simple CAOM-2.x package service that generates a package (tar) with all artifacts of a single plane
* caom2-soda-server: IVOA SODA implementation for CAOM-2.x

These server-side libraries make use of the cadc-uws-server package and provide a JobRunner implementation to use 
with that framework. They use the caom2-tap client library to query a CAOM TAP service.
