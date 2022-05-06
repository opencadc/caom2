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

# Destination caom2 database settings
org.opencadc.caom2.metasync.destination.schema={schema}
org.opencadc.caom2.metasync.destination.username={dbuser}
org.opencadc.caom2.metasync.destination.password={dbpassword}
org.opencadc.caom2.metasync.destination.url=jdbc:postgresql://{server}/{database}

# Source service resource identifier
org.opencadc.caom2.metasync.source.resourceID={uri}

# The collection to sync
org.opencadc.caom2.metasync.collection={collection name}

# Base for generating Plane publisherID values
org.opencadc.caom2.metasync.basePublisherID={uri}

# Number of threads used to read from the source service
org.opencadc.caom2.metasync.threads={integer}

# Number of observations to sync per batch
org.opencadc.caom2.metasync.batchSize={integer}

```

`org.opencadc.caom2.metasync.source.resourceID` is the resource identifier for 
a registered caom2 repository service (e.g. ivo://cadc.nrc.ca/ams)

`org.opencadc.caom2.metasync.collection` The collection name used to query
for Artifacts in the repository service.

`org.opencadc.caom2.metasync.basePublisherID` is the base for generating Plane 
publisherID values. The base is an uri of the form ivo://<authority>[/<path>]
publisherID values: <basePublisherID>/<collection>?<observationID>/<productID>

`org.opencadc.caom2.metasync.source.threads` is the number of threads used to
read observations from the source repository service.

`org.opencadc.caom2.metasync.batchSize` is the number of Observations 
processed as a single batch. It's a limit on the maximum number of 
Observations returned from a repository service query.


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
