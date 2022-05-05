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
org.opencadc.caom2.download.logging={info|debug}

# Profile task execution
org.opencadc.caom2.download.profile={true|false}

# caom2 database settings
org.opencadc.caom2.download.schema={schema}
org.opencadc.caom2.download.username={dbuser}
org.opencadc.caom2.download.password={dbpassword}
org.opencadc.caom2.download.url=jdbc:postgresql://{server}/{database}

# ArtifactStore implementation
ca.nrc.cadc.caom2.artifact.ArtifactStore={fully qualified class name for ArtifactStore implementation}

# The collection to use
org.opencadc.caom2.download.collection={collection name}

# Number of download threads (default: 1)
org.opencadc.caom2.download.threads={integer}

# Hours after failed downloads should be retried (default: 24)
org.opencadc.caom2.download.retryAfter={integer}

# Download even when checksum is null
org.opencadc.caom2.download.tolerateNullChecksum={true|false}
```

`org.opencadc.caom2.download.artifactStore` is the fully qualified 
class name to an ArtifactStore implementation, which may require 
properties file(s) in /config.

`org.opencadc.caom2.download.collection` The collection name used to query 
for Artifacts in the caom2 database.

`org.opencadc.caom2.download.retryAfter` is the number of hours after 
failed downloads should be retried.

if `org.opencadc.caom2.download.tolerateNullChecksum` is true, download
Artifacts with a null checksum.


### cadcproxy.pem
Certificate in /config is used to authenticate https calls to other services 
if challenged for a client certificate.


## building it
```
gradle clean build
docker build -t caom2-artifact-download -f Dockerfile .
```

## checking it
```
docker run -it caom2-artifact-download:latest /bin/bash
```

## running it
```
docker run --user opencadc:opencadc -v /path/to/external/config:/config:ro --name caom2-artifact-download caom2-artifact-download:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag caom2-artifact-download:latest caom2-artifact-download:$t
done
unset TAGS
docker image list caom2-artifact-discover
```
