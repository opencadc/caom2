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
# optional: permission services (one per line) that provide read and write grants
org.opencadc.torkeep.grantProvider = {URI}

# optional: operations permissions
org.opencadc.torkeep.archiveOperator = {identity}
org.opencadc.torkeep.metaSyncOperator = {identity}

# collection name and one or more collection properties
org.opencadc.torkeep.collection = {collection}

# required: define REST API path to Observation.uri mapping
{collection}.obsIdentifierPrefix = {URI prefix}

# define an alternate base URI for Plane.publisherID values (optional)
{collection}.basePublisherID = {URI}

# define collection-specific extendced validation (optional)
{collection}.validationPolicy = {policy config file}
 
# TODO: generate read group grants fvrom Observation.proposal (optional, default: false)
{collection}.proposalGroup = true|false
```
_grantProvider_ is URI to a permissions service that provides grants to read and write CAOM collections.
The URI is a resourceID (e.g. ivo://opencadc.org/baldur) to a permissions service defined in the registry, 
one line per permissions service. `torkeep` will requerst grant information for CAOM metadata objects 
(observations) in the form `caom:{collection}/{observationID}` so for baldur rules that grant read-only and/or
read-write access to identifier patterns that start with `caom:{collection}/` are necessary to match the
requests that `torkeep` will make.

The optional _archiveOperator_ and _metaSyncOperator_ grant permissions directly to operational accounts. An 
_archiveOperator_ has read-write permisison to **ALL** collections and metadata and can be used to enable processes
to put metadata into the archive. A _metaSyncOperator_ has read-only permission  to **ALL** collections and can be
used to enable metadata-sync (`icewind` running at a different location). If the identities are X509 distinguished 
names, these can be used with no additional AAI components, but they do require front-end proxy setup and an 
IdentityManager implementation to support X509 client certificate use. **TODO:** additional docs for use with OpenID.

_collection_ specifies the CAOM collection name and defines a new set of config keys for that collection.

_obsIdentifierPrefix_ defines the mapping from the REST API (`/observations/{collection}/{obs identifier}`) to
the Observation.uri style used for the collection. Two styles are supported:
* `{collection}.obsIdentifierPrefix = caom:` maps to URIs of the form `caom:{collection}/{obs identifier}`
* `{collection}.obsIdentifierPrefix = ivo://{authority}/` maps to URIs of the form `ivo://{authority}/{collection}?{obs identifier}`
An _obsIdentifierPrefix_ using the `ivo` scheme must end in a trailing `/`.

_basePublisherID_ is the base for generating a local Plane publisherID value from the Plane.uri value. This would be
ommitted if the Plane.uri values used for the collection are already `ivo` identifiers; in this case the Plane.uri 
(nominally the creator id in IVOA use) is simply copied to the publisherID field. If the Plane.uri values use the traditional internal style (`caom:{collection}/{plane identifier}`) then the _basePublisherID_ is required to transform
those internal values into `ivo` URIs of the form `{basePublisherID}/{collection}?{plane identifier}`. The _basePublisherID_
must end with a trailing `/`.

For example,
```
org.opencadc.torkeep.collection = CFHT
{collection name}.obsIdentifierPrefix = ivo://opencadc.org/
```
would transform a Plane.uri `caom:CFHT/123456p` into publisherID `ivo://opencadc.org/CFHT?123456p`.

The _validationPolicy_ specifies a secondary configuration file to configure the validation of observations in the
collection. See [ValidationPolicy](ValidationPolicy.md) for details.

Not implemented yet: _proposalGroup_ (optional, default: false) is a boolean flag which indicates whether CAOM 
read access grant(s) are generated to allow the proposal group to access CAOM metadata and/or data (if needed 
because it is not currently public). A proposal group is always of the form `{GMS service resourceID}?{Observation.collection}-{Observation.proposal.id}` where the GMS resourceID is the one configured as the local GMS service (see `cadc-registry` above) and the group name is generated from the CAOM collection and proposal ID as shown.
NOTE: not yet enabled in this code pending code re-org. `torkeep` *does not* check if the group exists or attempt to
create it.


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
