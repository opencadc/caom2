# CAOM Meta Sync process

Process to sync Observations from a CAOM repository service
to a CAOM database.

## configuration

See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java)
image docs for general config requirements.

Runtime configuration must be made available via the `/config` directory.

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### caom2-meta-sync.properties
```
# log level
org.opencadc.caom2.metasync.logging={info|debug}

# Source repository service
org.opencadc.caom2.metasync.repoService={uri}

# The collections to sync, one collection per line
org.opencadc.caom2.metasync.collection={collection name}

# Maximum number of seconds to pause between runs
org.opencadc.caom2.metasync.maxIdle={integer}

# Destination caom2 database settings
org.opencadc.caom2.metasync.db.schema={schema}
org.opencadc.caom2.metasync.db.username={dbuser}
org.opencadc.caom2.metasync.db.password={dbpassword}
org.opencadc.caom2.metasync.db.url=jdbc:postgresql://{server}/{database}

# Base for generating Plane publisherID values
org.opencadc.caom2.metasync.basePublisherID={uri}

# (optional) exit after processing collections once
#org.opencadc.caom2.metasync.exitWhenComplete=true
```

The _repoService_ is the resource identifier for a registered CAOM repository service 
(e.g. ivo://cadc.nrc.ca/ams).

One or more CAOM collections can be synced by an single instance. The _collection_ 
specifies the name (Observation.collecion) used to query for Observation(s) in the 
repository. For multiple collections use multiple lines, one collection per line.

The _maxIdle_ time is the maximum time (seconds) to idle (sleep) before querying the 
repository for new observations. The idle time defaults to 30 seconds and doubles
every time no new observations are found until maxIdle is reached. The idle time 
resets to the default when new content is found.

The _basePublisherID_ is used to generate Plane.publisherID values. The base 
is a URI of the form `ivo://<authority>[/<path>]` and generated publisherID values
are `<basePublisherID>/<collection>?<observationID>/<productID>`.

`caom2-meta-sync` normally runs forever; the _exitWhenComplete_ flag (optional) can
be set to `true` to make the sync process to exit after syncing each collection once.

### cadcproxy.pem
Optional certificate in /config is used to authenticate https calls 
to other services if challenged for a client certificate. 
If cadcproxy.pem is not present, queries to the repository service 
are made anonymously.


## building it
```
gradle clean build
docker build -t caom2-meta-sync -f Dockerfile .
```

## checking it
```
docker run -it caom2-meta-sync:latest /bin/bash
```

## running it
```
docker run --user opencadc:opencadc -v /path/to/external/config:/config:ro --name caom2-meta-sync caom2-meta-sync:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag caom2-meta-sync:latest caom2-meta-sync:$t
done
unset TAGS
docker image list caom2-meta-sync
```
