# IVOA SODA server-side implementation for CAOM-2.x

This package provides the SodaJobRunner class that can be deployed using the SyncServlet in
cadc-uws-server to provide SODA async and/or sync resource(s). The SodaJobRunner uses caom2-tap-query
to query a TAP service, using the input ID(s) as CAOM-2.x artifact URI(s). The artifact metadata is
used to convert the SODA cutout specification (parameters) into cfitsio-style pixel cutouts and then
generate URL(s) to a separate web service that accepts and processes such requests. So, the current
implementation works in concert with and assumes that the data is accessible via a service that handles
cfitsio-style pixel cutouts synchronously.

TODO: define an interface so implementers can provide a plugin to support pixel cutouts.


A webapp wanting to use the SodaJobRunner must implement a JobManager (cadc-uws-server) that 
configures a JobPersistence and a JobExecutor. 

