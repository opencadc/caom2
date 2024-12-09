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

# caom2 database settings
org.opencadc.caom2.download.schema={schema}
org.opencadc.caom2.download.username={dbuser}
org.opencadc.caom2.download.password={dbpassword}
org.opencadc.caom2.download.url=jdbc:postgresql://{server}/{database}

# ArtifactStore implementation
ca.nrc.cadc.caom2.artifact.ArtifactStore={fully qualified class name for ArtifactStore implementation}

# storage namespace aka Artifact.uri prefix
org.opencadc.caom2.download.namespace={Artifact.uri prefix}

# Prefix of artifacts to download
org.opencadc.caom2.download.buckets={uriBucket prefix or range of prefixes}

# Number of download threads
org.opencadc.caom2.download.threads={number of download threads}

# Hours after failed downloads should be retried
org.opencadc.caom2.download.retryAfter={integer}
```

`org.opencadc.caom2.download.artifactStore` is the fully qualified 
class name to an ArtifactStore implementation, which may require 
properties file(s) in /config.

`org.opencadc.caom2.download.namespace` The storage namespace is specified
as a prefix of Artifact.uri values; it must end with a `:` or `/` so that it
won't accidentally match a different namespace.

`org.opencadc.caom2.download.buckets` The range of uriBucket prefixes 
is specified with two values separated by a single - (dash) character; 
whitespace is ignored.

`org.opencadc.caom2.download.threads` The number of download threads indirectly 
configures a database connection pool that is shared between file sync jobs
(approximately 3 threads per connection).

`org.opencadc.caom2.download.retryAfter` is the number of hours after 
failed downloads should be retried.

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
docker run --rm --user opencadc:opencadc -it caom2-artifact-download:latest /bin/bash
```

## running it
```
docker run --rm --user opencadc:opencadc -v /path/to/external/config:/config:ro --name caom2-artifact-download caom2-artifact-download:latest
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
