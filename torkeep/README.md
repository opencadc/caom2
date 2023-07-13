# CAOM metadata repository service (torkeep)


### deployment
The `torkeep` war file can be renamed at deployment time in order to support an alternate service name, 
including introducing additional path elements (see war-rename.conf).

`torkeep` requires a PostgreSQL database with citext and pg_sphere extensions with a `caom2` schema.
When the service is started it checks/creates/upgrades the `caom2` tables and database content.


### configuration
The following configuration files must be available in the `/config` directory.


### catalina.properties
This file contains java system properties to configure the tomcat server and some of the java libraries used in the service.

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a>
for system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a> for common system properties.

`torkeep` uses a database connection pool:
```
# caom2 database connection pool
org.opencadc.torkeep.caom2.maxActive={max connections for caom2 pool}
org.opencadc.torkeep.caom2.username={username for caom2 pool}
org.opencadc.torkeep.caom2.password={password for caom2 pool}
org.opencadc.torkeep.caom2.url=jdbc:postgresql://{server}/{database}
```

This user account owns and manages (create, alter, drop) caom2 database objects and manages all the content (insert, update, delete).
In addition, the TAP service does not currently support a configurable schema name: it assumes a schema named `caom2` holds the content.


### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.


### torkeep.properties
The torkeep.properties configures the services that provide grants to access collections, and collection specific properties.
Each collection in the properties file configures a collection name, followed by collection-specific properties.

```
# permission services (one per line) that provide read and write grants
org.opencadc.torkeep.grantProvider = {URI}

# collection name and one or more collection properties
org.opencadc.torkeep.collection = {CAOM collection name}
{collection name}.basePublisherID = {URI}
{collection name}.computeMetadata = {true | false}
{collection name}.proposalGroup = {true | false}
```
_grantProvider_ is URI to a permissions service that provides grants to read and write CAOM collections.
The URI is a resourceID (e.g. ivo://opencadc.org/baldur) to a permissions service defined in a registry service, 
one line per permissions service.

_collection_ specifies the CAOM collection name and defines a new set of config keys for that collection.

_bashPublisherID_ is the base for generating Plane publisherID values.
The base is an uri of the form `ivo://<authority>[/<path>]`
publisherID values: `<basePublisherID>/<collection>?<observationID>/<productID>`

_computeMetadata_ enables computation and persistence of computed metadata(generally, Plane metadata
aggregated from the artifacts). (default: false)

_proposalGroup_ is a boolean flag which indicates whether a grant is generated to allow the proposal group 
to access CAOM metadata and/or data (if needed because it is not public).


## building it
```
gradle clean build
docker build -t torkeep -f Dockerfile .
```

## checking it
```
docker run --rm -it torkeep:latest /bin/bash
```

## running it
```
docker run --rm --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name torkeep torkeep:latest
```