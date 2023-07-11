# bifrost

`bifrost` is a [https://www.ivoa.net/documents/DataLink/](DataLink) service
for CAOM [https://www.opencadc.org/caom2/](Common Archive Observation Model). It is 
designed to work with [ttps://github.com/opencadc/caom2service/argus](argus) 
(the CAOM TAP service).

## deployment
The `bifrost` war file can be renamed at deployment time in order to support an 
alternate service name, including introducing additional path elements using the
[https://github.com/opencadc/docker-base/tree/master/cadc-tomcat](war-rename.conf) 
feature.

This service instance is expected to have a PostgreSQL database backend to store UWS
job information. It is safe to use the same database as `argus` and have both services
store jobs in the same tables.

## configuration
The following configuration files must be available in the `/config` directory.

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
 
 `bifrost` requires one connection pool to store requests:
```
# database connection pools
org.opencadc.bifrost.uws.maxActive={max connections for jobs pool}
org.opencadc.bifrost.uws.username={database username for jobs pool}
org.opencadc.bifrost.uws.password={database password for jobs pool}
org.opencadc.bifrost.uws.url=jdbc:postgresql://{server}/{database}
```

The _uws_ pool manages (create, alter, drop) uws tables and manages the uws content
(creates and modifies jobs in the uws schema when jobs are created and executed by users.
If `bifrost` and `argus` are configured to use the same database for UWS jobs, the 
management does create a tight coupling between the two services in the version of the
UWS libraries used; however, changes in the UWS database schema are uncommon so this is
not too hard to manage.

### cadc-registry.properties
See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### bifrost.properties
`bifrost` must be configured to use a single CAOM TAP service (`argus`) to execute queries.
```
# CAOM TAP service
org.opencadc.bifrost.queryService = {argus resourceID}

# artifact locator service
org.opencadc.bifrost.locatorService = {resourceID of global transfer negotiation service}

# read grant providers (optional)
org.opencadc.bifrost.readGrantProvider = {resourceID of a grant provider}
```
The _queryService_ is resolved by a registry lookup and that service is used to query
for CAOM content. It is assumed that this service is deployed "locally" since there can
be many calls to `bifrost` and low latency is very desireable.

The _locatorService_ is a data storage service that allows `bifrost` to resolve a CAOM 
Artifact URI into a URL. Current hack: `bifrost` assumes the _locatorService_ also supports
the simpler [https://github.com/opencadc/storage-inventory](storage-inventory) "files" API 
and constructs URLs to the global `raven` service (which in turn will redirect the caller to
one of the copies of the specified file).

The optional _readGrantProvider_ configures `bifrost` use the grant provider(s) (multiple can be
specified) to predict that the caller will be authorized when using generated links. In adddition
to CAOM metadata that grants access, this will be used to determine a value for the DataLink
_linkAuthorized_ field.

`bifrost` will attempt to use the caller's identity to query so that CAOM proprietary metadata
protections are enforced, but the details of this depend on the configured IdentityManager 
and local A&A service configuration.


## building it
```
gradle clean build
docker build -t bifrost -f Dockerfile .
```

## checking it
```
docker run --rm -it bifrost:latest /bin/bash
```

## running it
```
docker run --rm --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name bifrost bifrost:latest
```
