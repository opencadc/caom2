# CAOM2 Meta Sync process

Process to sync Observations from a CAOM2 repository service
to a CAOM2 database. Process runs continuously exiting only
when source queries return no results.

## configuration

See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java)
image docs for general config requirements.

Runtime configuration must be made available via the `/config` directory.


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

# Optional - exit after processing collections once
org.opencadc.caom2.metasync.exitWhenComplete=true|false
```

_repoService_ is the resource identifier for a registered 
caom2 repository service (e.g. ivo://cadc.nrc.ca/ams)

_collection_ is the collection name used to query for Artifacts 
in the repository service. For multiple collections use multiple lines, 
one collection per line.

_maxIdle_ is the maximum time in seconds to pause between runs 
when _exitWhenComplete_ is _false_. The idle time starts at 60 seconds, 
doubling every time no data is found to sync, until maxIdle is reached. 
The idle time will reset to 60 seconds when data is found to sync.

_basePublisherID_ is the base for generating Plane 
publisherID values. The base is an uri of the form ivo://<authority>[/<path>]
publisherID values: <basePublisherID>/<collection>?<observationID>/<productID>

_exitWhenComplete_ is optional and defaults to _false_. 
When _true_ each collection is processed once, and then the application exits. 
The default is collections are continuously processed in a loop.


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
