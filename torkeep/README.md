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

`torkeep` includes multiple IdentityManager implementations to support authenticated access:
- See <a href="https://github.com/opencadc/ac/tree/master/cadc-access-control-identity">cadc-access-control-identity</a> for CADC access-control system support.
- See <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> for OIDC token support.

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
The URI is a resourceID (e.g. ivo://opencadc.org/baldur) to a permissions service defined in the registry, 
one line per permissions service. `torkeep` will requerst grant information for CAOM metadata objects 
(observations) in the form `caom:{collection}/{observationID}` so for baldur rules that grant read-only and/or
read-write access to identifier patterns that start with `caom:{collection}/` are necessary to match the
requests that `torkeep` will make.

_collection_ specifies the CAOM collection name and defines a new set of config keys for that collection.

_bashPublisherID_ is the base for generating Plane publisherID values.
The base is an uri of the form `ivo://<authority>[/<path>]`
publisherID values: `<basePublisherID>/<collection>?<observationID>/<productID>`

_computeMetadata_ (optional, default: false) enables computation and persistence of computed metadata
(generally, Plane metadata aggregated from the artifacts).

_proposalGroup_ (optional, default: false) is a boolean flag which indicates whether CAOM read access grant(s)
are generated to allow the proposal group to access CAOM metadata and/or data (if needed because it is not 
currently public). A proposal group is always of the form 
`{GMS service resourceID}?{Observation.collection}-{Observation.proposal.id}` 
where the GMS resourceID is the one configured as the local GMS service (see `cadc-registry` above) and the 
group name is generated from the CAOM collection and proposal ID as shown. NOTE: not yet enabled in this code 
pending code re-org. `torkeep` *does not* check if the group exists or attempt to create it.

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
