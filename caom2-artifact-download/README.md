# CAOM2 Artifact Download process

Process to download Artifacts to an ArtifactStore. 
It finds Artifacts to download in a harvest table 
populated by `caom2-artifact-discover`.


## configuration

See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) 
image docs for general config requirements.

caom2-artifact-download uses an implementation of the ArtifactStore interface
[caom2-persist](https://github.com/opencadc/caom2db/tree/master/caom2-persist).

Runtime configuration must be made available via the `/config` directory.


### caom2-artifact-download.properties
```
# log level
ca.nrc.cadc.caom2.artifactsync.logging={info|debug}

# Profile task execution
ca.nrc.cadc.caom2.artifactsync.profile={true|false}

# caom2 database settings
ca.nrc.cadc.caom2.artifactsync.host={hostname}
ca.nrc.cadc.caom2.artifactsync.database={database}
ca.nrc.cadc.caom2.artifactsync.schema={schema}
ca.nrc.cadc.caom2.artifactsync.username={dbuser}
ca.nrc.cadc.caom2.artifactsync.password={dbpassword}

# ArtifactStore implementation
ca.nrc.cadc.caom2.artifactsync.artifactStore={fully qualified class name for ArtifactStore implementation}

# The collection to use
ca.nrc.cadc.caom2.artifactsync.collection={collection name}

# Max skip URIs to download (default: 1000)
ca.nrc.cadc.caom2.artifactsync.batchSize={integer}

# Repeat batches until no work left
ca.nrc.cadc.caom2.artifactsync.continue={true|false}

# Number of download threads (default: 1)
ca.nrc.cadc.caom2.artifactsync.threads={integer}

# Hours after failed downloads should be retried (default: 24)
ca.nrc.cadc.caom2.artifactsync.retryAfter={integer}

# Download even when checksum is null
ca.nrc.cadc.caom2.artifactsync.tolerateNullChecksum={true|false}

# Artifact count which triggers download to stop at the current batch
ca.nrc.cadc.caom2.artifactsync.downloadThreshold={integer}
```

`ca.nrc.cadc.caom2.artifactsync.artifactStore` is the fully qualified 
class name to an ArtifactStore implementation, which may require 
properties file(s) in /config.

`ca.nrc.cadc.caom2.artifactsync.collection` The collection name used to query 
for Artifacts in the caom2 database.

`ca.nrc.cadc.caom2.artifactsync.batchSize` is the number of Artifact processed 
as a single batch. It's a limit on the maximum number of Artifacts returned 
from a caom2 database query.

If `ca.nrc.cadc.caom2.artifactsync.continue` is true, Artifacts will be 
processed in `batchSize` increments until the Artifact query returns empty. 
If false, only the first `batchSize` Artifacts will be processed.

`ca.nrc.cadc.caom2.artifactsync.retryAfter` is the number of hours after 
failed downloads should be retried.

if `ca.nrc.cadc.caom2.artifactsync.tolerateNullChecksum` is true, download
Artifacts with a null checksum.

`ca.nrc.cadc.caom2.artifactsync.downloadThreshold` is the Artifact count
which triggers download to stop at the current batch.


### cadcproxy.pem
Certificate in /config used to authenticate when querying the ArtifactStore.


## building it
```
gradle clean build
docker build -t caom2-artifact-discover -f Dockerfile .
```

## checking it
```
docker run -it caom2-artifact-discover:latest /bin/bash
```

## running it
```
docker run --user opencadc:opencadc -v /path/to/external/config:/config:ro --name caom2-artifact-discover caom2-artifact-discover:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag caom2-artifact-discover:latest caom2-artifact-discover:$t
done
unset TAGS
docker image list caom2-artifact-discover
```
