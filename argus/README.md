# sc2tap

CAOM TAP service for the Multiple Archive Query (MAQ) 

This service allows queries to CAOM metadata using
IVOA <a href="http://www.ivoa.net/documents/TAP/20190927/">TAP-1.1</a> web service API.

### deployment
The `argus` war file can be renamed at deployment time in order to support an alternate
service name, including introducing additional path elements (see war-rename.conf).

This service instance is expected to have a database backend to store the TAP metadata and which
also includes the CAOM tables.

## configuration
The following configuration files must be available in the `/config` directory.

See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image
docs for expected deployment and common config requirements. The `sc2tap` war file can be renamed
at deployment time in order to support an alternate service name, including introducing
additional path elements (see war-rename.conf).

Runtime configuration must be made available via the `/config` directory.

### catalina.properties
This file contains java system properties to configure the tomcat server and some of the java 
libraries used in the service.

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a>
for system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a>
for common system properties.

`argus` includes multiple IdentityManager implementations to support authenticated access:
 - See <a href="https://github.com/opencadc/ac/tree/master/cadc-access-control-identity">cadc-access-control-identity</a> for CADC access-control system support.
 - See <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> for OIDC token support.

`argus` requires 3 connections pools:
```
# database connection pools
org.opencadc.argus.uws.maxActive={max connections for jobs pool}
org.opencadc.argus.uws.username={database username for jobs pool}
org.opencadc.argus.uws.password={database password for jobs pool}
org.opencadc.argus.uws.url=jdbc:postgresql://{server}/{database}

org.opencadc.argus.tapadm.maxActive={max connections for jobs pool}
org.opencadc.argus.tapadm.username={database username for jobs pool}
org.opencadc.argus.tapadm.password={database password for jobs pool}
org.opencadc.argus.tapadm.url=jdbc:postgresql://{server}/{database}

org.opencadc.argus.query.maxActive={max connections for jobs pool}
org.opencadc.argus.query.username={database username for jobs pool}
org.opencadc.argus.query.password={database password for jobs pool}
org.opencadc.argus.query.url=jdbc:postgresql://{server}/{database}
```

The `uws` pool manages (create, alter, drop) uws tables and manages the uws content 
(creates and modifies jobs in the uws schema when jobs are created and executed by users).

The `tapadm` pool manages (create, alter, drop) tap_schema tables and manages the tap_schema content
for `tap_schema`, `caom2`, and `ivoa` schemas.

The `tapuser` pool is used to run TAP queries, including creating tables in the tap_upload schema. 

All three pools must have the same JDBC URL (e.g. use the same database) with PostgreSQL.

In addition, the TAP service does not currently support a configurable schema name: it assumes a schema 
named `caom2` holds the content.

### cadc-registry.properties
See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### argus.properties
```
# configure temporary storage manager implementation
org.opencadc.argus.storageManager = {implementation class name}

## option 1: configure file system storage manager
#org.opencadc.argus.storageManager = org.opencadc.tap.tmp.TempStorageManager

## option 2: configure http storage manager
#org.opencadc.argus.storageManager = org.opencadc.tap.tmp.HttpStorageManager
```

The _storageManager_ property picks one of the two available implementation classses to handle 
async query results and inline tap_upload files. See `cadc-tap-tmp.properties` below for additional
configuration.

### cadc-tap-tmp.properties
`argus` uses the [cadc-tap-tmp](https://github.com/opencadc/tap/tree/master/cadc-tap-tmp) library to
manage temporary storage. If configured to use the _TempStorageManager_ implementation, the base URL 
must include "/results" as the last path component (e.g. `https://example.net/argus/results`).

## building it
```
gradle clean build
docker build -t argus -f Dockerfile .
```

## checking it
```
docker run --rm -it argus:latest /bin/bash
```

## running it
```
docker run --rm --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name argus argus:latest
```
