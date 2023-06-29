# argus

`argus` is a [Table Access Protocol (TAP)](https://www.ivoa.net/documents/TAP/) service
for [Common Archive Observation Model (CAOM)](https://www.opencadc.org/caom2/).

## deployment
The `argus` war file can be renamed at deployment time in order to support an alternate service 
name, including introducing additional path elements using the 
[cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) war-rename.conf feature.

This service instance is expected to have a PostgreSQL database backend to store the TAP metadata and which
also includes the CAOM tables. The `citext` extension is used for several keywords columns; the `pgsphere`
extension is used for spherical geometry columns and spatial queries.

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

The _uws_ pool manages (create, alter, drop) uws tables and manages the uws content 
(creates and modifies jobs in the uws schema when jobs are created and executed by users).

The _tapadm_ pool manages (create, alter, drop) tap_schema tables and manages the tap_schema content
for the `tap_schema`, `caom2`, and `ivoa` schemas.

The _query_ pool is used to run TAP queries, including creating tables in the tap_upload schema. 

All three pools must have the same JDBC URL (e.g. use the same database) with PostgreSQL.

In addition, the TAP service does not currently support a configurable schema name: it assumes a schema 
named `caom2` holds the content.

### cadc-registry.properties
See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### cadc-tap-tmp.properties
`argus` uses the [cadc-tap-tmp](https://github.com/opencadc/tap/tree/master/cadc-tap-tmp) library to
manage temporary storage and configured to use the _DelegatingStorageManager_ implementation. If
using the _TempStorageManager_, the base URL must include "/results" as the last path component 
(e.g. `https://example.net/argus/results`).

### argus.properties
This file may be needed in the future but is not currently used.

### injecting VOTable resources into query results
Some columns in the CAOM metadata have _ID_ values that are asssigned to the VOTable FIELD in the output.
These columns can be referred to using the standard XML IDREF mechanism. The primary purpose is to inject
a DataLink service descriptor resource that tells clients about a service they can call using values from
the column.

The following _ID_ values are available in CAOM, with the suggested use:
|ID|column|intended usage|
|--|:----:|:------------:|
|caomPublisherID|caom2.Plane.publisherID|inject DataLink "links" descriptor|
|caomArtifactID|caom2.Artifact.uri|experimental: link to direct data access service|
|caomObservationURI|caom2.Observation.observationURI|experimental: link to complete observation metadata|

To enable injection of resources, a VOTable xml file named `{ID}.xml` is added to the config
directory. It must be a valid VOTable with one or more resources of `type="meta"`.

Example _links_ descritor in a file named `caomPublisherID.xml`:
```
<?xml version="1.0" encoding="UTF-8"?>
<VOTABLE xmlns="http://www.ivoa.net/xml/VOTable/v1.3" 
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.4">

    <!-- template service descriptor to go with the caom2:Plane.publisherID column -->
    
    <!-- need a valid ID in the template for the ref="caomPublisherID" below -->
    <INFO name="tmp" ID="caomPublisherID" value="this will be dropped..." />
    
    <!-- service descriptor for the associated DataLink#links service -->
    <RESOURCE type="meta" utype="adhoc:service">
        <!-- tell clients this is a standard links-1.1 service -->
        <PARAM name="standardID" datatype="char" arraysize="*" value="ivo://ivoa.net/std/DataLink#links-1.1" />

        <!-- option: include the accessURL directly (suitable for unregistered links service) -->
        <PARAM name="accessURL" datatype="char" arraysize="*" value="https://example.net/datalink/links" />
        
        <!-- for a datalink service in the registry, include resourceIdentifier -->
        <PARAM name="resourceIdentifier" datatype="char" arraysize="*" value="ivo://example/links" />
        <!-- option: the accessURL PARAM is dynamically inserted by resolving the resourceIdentifier,
            but will not replace an existing accessURL param such as below -->
            
        <GROUP name="inputParams">
            <PARAM name="ID" datatype="char" arraysize="*" ref="caomPublisherID" value=""/>
        </GROUP>
    </RESOURCE>
<VOTABLE>
```
Whenever the column is selected, the resource(s) in the file will be injected into the VOTable query result.

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
