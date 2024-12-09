# CAOM-2.x packager server-side implementation

This package provides the PackageRunner class that can be deployed using the SyncServlet in
cadc-uws-server to provide on-the-fly tar file generation. The PackageRunner uses caom2-tap
to query a TAP service, using the input ID as a CAOM-2.x plane identifier, and outputs a single tar
file with all artifacts *except* those marked as previews (Artifact.productType = preview|thumbnail).


A webapp wanting to use the PackageRunner must implement a JobManager (cadc-uws-server) that 
configures a JobPersistence and a JobExecutor (SyncJobExecutor implementation).

