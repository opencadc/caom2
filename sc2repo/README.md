# torkeep

CAOM repository service for the CADC data engineering sandbox. 

## configuration
See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image
docs for expected deployment and common config requirements. The `torkeep` war file can be renamed
at deployment time in order to support an alternate service name, including introducing
additional path elements (see war-rename.conf).

This service instance is expected to have a database backend to store the TAP metadata and which
also includes the caom2 tables.

Runtime configuration must be made available via the /config directory.

### catalina.properties (cadc-tomcat)
When running torkeep.war in tomcat, parameters of the connection pool in META-INF/context.xml need to be configured in catalina.properties:

```
# caom2 database connection pool
org.opencadc.torkeep.caom2.maxActive={max connections for caom2 pool}
org.opencadc.torkeep.caom2.username={username for caom2 pool}
org.opencadc.torkeep.caom2.password={password for caom2 pool}
org.opencadc.torkeep.caom2.url=jdbc:postgresql://{server}/{database}
```

The `admin` account owns and manages (create, alter, drop) caom2 database objects and manages all the content (insert, update, delete).
In addition, the TAP service does not currently support a configurable schema name: it assumes a schema named `caom2` holds the content.


### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.


### torkeep.properties
The torkeep.properties configures a collection with the desired proposal group, operator group and staff group.
Each entry in the properties file configures a collection. Each entry has a name, followed by entry-specific properties.

```
# required properties
org.opencadc.torkeep.collection = {CAOM collection name}
{collection name}.basePublisherID = {URI}

# optional properties
{collection name}.readOnlyGroup = {group URI}
{collection name}.readWriteGroup = {group URI}
{collection name}.anon = {true | false}
{collection name}.computeMetadata = {true | false}
{collection name}.proposalGroup = {true | false}
```

_org.opencadc.torkeep.collection_ specifies the CAOM collection name.

_{collection name}.bashPublisherID_  is the base for generating Plane publisherID values.
The base is an uri of the form `ivo://<authority>[/<path>]`
publisherID values: `<basePublisherID>/<collection>?<observationID>/<productID>`

_{collection name}.readOnlyGroup_ specifies a group (one per line) that can read (get) matching assets (default: empty list).

_{collection name}.readWriteGroup_ specifies a group (one per line) that can read and write (get/put/update/delete)
matching assets (default: empty list).

_{collection name}.anon_  specifies that all users (including anonymous) can read matching assets (default: false).

_{collection name}.computeMetadata_ enables computation and persistence of computed metadata(generally, Plane metadata
aggregated from the artifacts). (default: false)

_{collection name}.proposalGroup_ is a boolean flag which indicates whether a proposal group is to be generated
in the observation for the collection. (default: false)


### database tables
torkeep requires a PostgreSQL database with citext and pg_sphere extensions with a `caom2` schema. 
When the service is started it checks/creates/upgrades the `caom2` tables and database content.


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