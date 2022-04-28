# CAOM2 Artifact Discover process

Process to discover Artifacts to be downloaded to an ArtifactStore. 
It finds new and modified Artifacts from a CAOM2 database, adding them to 
a harvest table used by `caom2-artifact-download` to download Artifacts 
to an ArtifactStore.


## configuration

See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) 
image docs for general config requirements.

caom2-artifact-discover uses an implementation of the ArtifactStore interface
[caom2-persist](https://github.com/opencadc/caom2db/tree/master/caom2-persist).

Runtime configuration must be made available via the `/config` directory.


### caom2-artifact-discover.properties
```
# log level
org.opencadc.caom2.discover.logging={info|debug}

# Profile task execution
org.opencadc.caom2.discover.profile={true|false}

# caom2 database settings
org.opencadc.caom2.discover.schema={schema}
org.opencadc.caom2.discover.username={dbuser}
org.opencadc.caom2.discover.password={dbpassword}
org.opencadc.caom2.discover.url=jdbc:postgresql://{server}/{database}

# ArtifactStore implementation
org.opencadc.caom2.discover.artifactStore={fully qualified class name for ArtifactStore implementation}

# The collection to use
org.opencadc.caom2.discover.collection={collection name}
```

`org.opencadc.caom2.discover.artifactStore` is the fully qualified class name 
to an ArtifactStore implementation, which may require properties file(s) in /config.

`org.opencadc.caom2.discover.collection` The collection name used to
 query for Artifacts in the caom2 database.


### cadcproxy.pem
Certificate in /config used to authenticate https calls to other services
if challenged for a client certificate.


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
