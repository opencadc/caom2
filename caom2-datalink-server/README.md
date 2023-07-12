# IVOA DataLink server-side implementation for CAOM-2.x

## DEPRECATED - this code was forked to create the `bifrost` service
This library may not be maintained if `bifrost` can satisfy all use cases.

This package provides the LinkQueryRunner class that can be deployed using the SyncServlet in
cadc-uws-server to provide DataLink links resource(s). The LinkQueryRunner uses caom2-tap
to query a TAP service, using the input ID as a CAOM-2.x plane identifier, and outputs links
for each artifact of that plane.

A webapp wanting to use the LinkQueryRunner must implement a JobManager (cadc-uws-server) that 
configures a JobPersistence and a JobExecutor (SyncJobExecutor implementation).

