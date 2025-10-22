# CAOM metadata-sync process (icewind)

`icewind` is an application that synchronizes [Common Archive Observation Model](https://www.opencadc.org/caom2/)
(CAOM) metadata from a remote CAOM repository service to a local CAOM database.

## deployment
This application requies a PostgreSQL database backend to store the CAOM content. 
The `citext` extension is used for several _keywords_ columns; the `pgsphere` extension
is used for spherical geometry columns and spatial queries.

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

# optional: number of observations to read into memory per batch (default: 100)
org.opencadc.icewind.batchSize={num}

# optional: number of threads used to read observations from repoService (default: 1 + batchSize/10)
org.opencadc.icewind.numThreads={num}

# optional: limit harvesting to sufficiently old changes to avoid volatile head of sequence (default: 300)
org.opencadc.icewind.lookbackTime={seconds}

# Destination caom2 database settings
org.opencadc.icewind.caom.schema={CAOM schema name}
org.opencadc.icewind.caom.username={username for CAOM admin}
org.opencadc.icewind.caom.password={password for CAOM admin}
org.opencadc.icewind.caom.url=jdbc:postgresql://{server}/{database}

# Base for generating Plane publisherID values
org.opencadc.icewind.basePublisherID={uri}

# (optional) exit after processing each collection once
#org.opencadc.icewind.exitWhenComplete=true

# (optional mode) retry previously failed (skipped) observations
# this mode always assumes exitWhenComplete=true
org.opencadc.icewind.retrySkipped = true

# (optional mode) validate remote and local observation sets for consistency
# this mode always assumes exitWhenComplete=true
# validate mode always assumes retrySkipped and performs retries after validation
org.opencadc.icewind.validate = true
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

  The _lookbackTime_ is the amount of time (seconds) in the past to try to harvest metadata. This is
used to set a maximum on the timestamp to harvest of _now_ - _lookbackTime. If the head of the sequence
of updates is volatile (does not monotomically increase) incremental harvest can miss updates that are
inserted slightly behind the most recent. This option limits (delays) harvesting to slightly older 
and this a stable part of the sequence. The default (300 seconds) is normally sufficient, but a larger
value may be necessary if the source is poorly behaved.

The _basePublisherID_ is used to generate Plane.publisherID values. The base 
is a URI of the form `ivo://<authority>[/<path>]` and generated publisherID values
are `<basePublisherID>/<collection>?<observationID>/<productID>`. This pattern 
allows for the whole "data collection" to be registered in an IVOA registry using
`<basePublisherID>/<collection>` as the resource identifier (optional, TBD).

`icewind` normally runs forever; the _exitWhenComplete_ flag (optional) can
be set to `true` to cause the process to exit after syncing each collection once.

`icewind` normally does incremental harvest of new or modified observations from 
the source; the _retrySkipped_ flag (optional) can be set to `true` to cause it to
retry previously failed (skipped) observations listed in the `caom2.HarvestSkipURI`
table. This mode always assumes _exitWhenComplete_ so it terminates after one pass
through the list.

The `icewind` _validate_ mode queries the _repoService_ and local database asnd compares the
two sets of observations, identifies discrepancies (missed delete, missed observation, or 
Observation.accMetaChecksum discrepancy) and schedules a retry by creating a new record
in the `caom2.HarvestSkipURI` table.

### cadcproxy.pem (optional)
This client certificate can be provided in /config directory. If present, it is used to 
authenticate to the _repoService_ if the service requests a client certificate. If 
the certificate is not present or the service does not request it, queries to 
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
