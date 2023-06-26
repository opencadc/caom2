# CAOM metadata-sync process (icewind)

Process to sync Observations from a remote CAOM repository service to a local CAOM database.

## configuration

See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java)
image docs for general config requirements.

Runtime configuration must be made available via the `/config` directory.

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### icewind.properties
```
# log level
org.opencadc.icewind.logging={info|debug}

# Source repository service
org.opencadc.icewind.repoService={uri}

# The collections to sync, one collection per line
org.opencadc.icewind.collection={collection name}

# Maximum sleep between runs (seconds)
org.opencadc.icewind.maxIdle={integer}

# Destination caom2 database settings
org.opencadc.icewind.caom.schema={CAOM schema name}
org.opencadc.icewind.caom.username={username for CAOM admin}
org.opencadc.icewind.caom.password={password for CAOM admin}
org.opencadc.icewind.caom.url=jdbc:postgresql://{server}/{database}

# Base for generating Plane publisherID values
org.opencadc.icewind.basePublisherID={uri}

# (optional) exit after processing collections once
#org.opencadc.icewind.exitWhenComplete=true
```

The _caom_ database account owns and manages (create, alter, drop) CAOM database objects
and manages all the content (insert, update, delete) in the CAOM schema. The database is 
specified in the JDBC URL. Note: the CAOM TAP service does not support a configurable schema 
and is hard-coded to expect the _schema_ name to be "caom2".

The _repoService_ is the resource identifier for a registered CAOM repository service 
(e.g. ivo://cadc.nrc.ca/ams).

The _collection_ specifies the name (Observation.collecion) used to query for Observation(s) 
in the repository.One or more CAOM collections can be synced by an single instance.  For 
multiple collections use multiple lines, one collection per line. Currently, `icewind` 
processes collections sequentially in a simple loop.

The _maxIdle_ time is the maximum time (seconds) to idle (sleep) before querying the 
repository for new observations. The idle time defaults to 30 seconds and doubles
when no new observations are found until maxIdle is reached. The idle time 
resets to the default when new content is found.

The _basePublisherID_ is used to generate Plane.publisherID values. The base 
is a URI of the form `ivo://<authority>[/<path>]` and generated publisherID values
are `<basePublisherID>/<collection>?<observationID>/<productID>`. This pattern 
allows for the whole "data collection" to be registered in an IVOA registry using
`<basePublisherID>/<collection>` as the resource identifier (optional, TBD).

`icewind` normally runs forever; the _exitWhenComplete_ flag (optional) can
be set to `true` to make the sync process to exit after syncing each collection once.

### cadcproxy.pem
Optional certificate in /config is used to authenticate to the _repoService_ if 
challenged for a client certificate. If cadcproxy.pem is not present, queries to 
the repository service are made anonymously.


## building it
```
gradle clean build
docker build -t icewind -f Dockerfile .
```

## checking it
```
docker run -it icewind:latest /bin/bash
```

## running it
```
docker run --user opencadc:opencadc -v /path/to/external/config:/config:ro --name icewind icewind:latest
```
