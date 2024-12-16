# CAOM-2.x metadata server-side implementation

This package provides the MetaQueryRunner class that can be deployed using the SyncServlet in
cadc-uws-server to provide DataLink links resource(s). The MetaQueryRunner uses caom2-tap
tom extract a single complete CAOM-2.x observation from a TAP service and output it in a 
document format (XML compliant to the latest CAOM-2.x XSD or an equivalent JSON document).

A webapp wanting to use the MetaQueryRunner must implement a JobManager (cadc-uws-server) that 
configures a JobPersistence and a JobExecutor (SyncJobExecutor implementation).

