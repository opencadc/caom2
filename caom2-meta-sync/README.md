# CAOM2 Meta Sync process

Process to sync Observations from a CAOM2 repository service
to a CAOM2 database.

## configuration

See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java)
image docs for general config requirements.

Runtime configuration must be made available via the `/config` directory.


### caom2-meta-sync.properties
```
# log level
org.opencadc.caom2.metasync.logging={info|debug}

# Destination caom2 database settings
org.opencadc.caom2.metasync.destination.server={server}
org.opencadc.caom2.metasync.destination.database={database}
org.opencadc.caom2.metasync.destination.schema={schema}

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

# Check for work but don't do anything
org.opencadc.caom2.metasync.dryRun={true|false}

# Minimum Observation.maxLastModified to consider
org.opencadc.caom2.metasync.minDate={UTC timestamp}

# Maximum Observation.maxLastModified to consider
org.opencadc.caom2.metasync.maxDate={UTC timestamp}

# Disable metadata checksum comparison
org.opencadc.caom2.metasync.noChecksum={true|false}
```
`org.opencadc.caom2.metasync.destination.server={server}`
`org.opencadc.caom2.metasync.destination.database={database}` the server and database connection info will be found in $HOME/.dbrc

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

If `org.opencadc.caom2.metasync.dryRun` is true, changes in the destination
database are not applied during the process. Default is false.

`org.opencadc.caom2.metasync.minDate` when set, returns Observation from the
repository service with an Observation.maxLastModified greater than or 
equal to the minDate. If not set, there is no lower limit on 
Observation.maxLastModified.
minDate is a UTC Timestamp (e.g. 2021-04-27T14:59:40.148)

`org.opencadc.caom2.metasync.maxDate` when set, returns Observation from the
repository service with an Observation.maxLastModified less than or equal to
the maxDate. If not set, there is no upper limit on Observation.maxLastModified.
maxDate is a UTC Timestamp (e.g. 2021-04-27T14:59:40.148)

If `org.opencadc.caom2.metasync.noChecksum` is true, does not compare computed 
and harvested Observation.accMetaChecksum. If false (default), requires 
checksum match or fails.

### cadcproxy.pem
Certificate in /config is used to authenticate https calls to other services
if challenged for a client certificate.


### .dbrc
Expected to be in the container $HOME directory. Maps the configuration 
server and database to the database connection info. The .dbrc format is:
`$server $database $username $password [$driver $url]`
```
server database db-username db-password org.postgresql.Driver  jdbc:postgresql://server:5432/database
```


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
