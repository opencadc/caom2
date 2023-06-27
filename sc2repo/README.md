# sc2repo

CAOM repository service for the CADC data engineering sandbox. 

## configuration
See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image
docs for expected deployment and common config requirements. The `sc2repo` war file can be renamed
at deployment time in order to support an alternate service name, including introducing
additional path elements (see war-rename.conf).

This service instance is expected to have a database backend to store the TAP metadata and which
also includes the caom2 tables.

Runtime configuration must be made available via the /config directory.

### catalina.properties (cadc-tomcat)
When running sc2repo.war in tomcat, parameters of the connection pool in META-INF/context.xml need to be configured in catalina.properties:

```
# database connection pools
sc2repo.test.maxActive={max connections for test admin pool}
sc2repo.test.username={username for test admin pool}
sc2repo.test.password={password for test admin pool}
sc2repo.test.url=jdbc:postgresql://{server}/{database}

sc2repo.sandbox.maxActive={max connections for sandbox admin pool}
sc2repo.sandbox.username={username for sandbox admin pool}
sc2repo.sandbox.password={password for sandbox admin pool}
sc2repo.sandbox.url=jdbc:postgresql://{server}/{database}
```

The `admin` account owns and manages (create, alter, drop) sandbox database objects and manages all the content (insert, update, delete).
In addition, the TAP service does not currently support a configurable schema name: it assumes a schema 
named `caom2` holds the content.


### LocalAuthority.properties
The LocalAuthority.properties file specifies which local service is authoritative for various site-wide functions. 
Documentation for the LocalAuthority.properties file can be found at [cadc-registry](https://github.com/opencadc/reg/tree/master/cadc-registry)


### sc2repo.properties
The sc2repo.properties configures a collection with the desired proposal group, operator group and staff group.
Each entry in the properties file configures a collection. Each entry has a name, followed by entry-specific properties.

```
# required properties
org.opencadc.csc2repo.entry = {entry name}
{entry name}.datasource = {datasource name}
{entry name}.database = {database}
{entry name}.schema = {schema}
{entry name}.obsTable = {Observation table}
{entry name}.sqlGenerator = {SQL generator class}
{entry name}.basePublisherID = {URI}

# optional properties
{entry name}.readOnlyGroup = {group URI}
{entry name}.readWriteGroup = {group URI}
{entry name}.operatorGroup = {group URI}
{entry name}.staffGroup = {group URI}
{entry name}.proposalGroup = {true | false}
{entry name}.computeMetadata = {true | false}
{entry.name}.computeMetadataValidation = {true | false}
{entry.name}.artifactPattern = 
{entry.name}.altPattern = 

```

_org.opencadc.caom2.repo.entry_ creates a new rule with the specified entry name.

_{entry name}.datasource_ is the JNDI datasource name for the entry, specified in the service deployment descriptor.

_{entry name}.database_ is the CAOM database name for the entry.

_{entry name}.schema_ is the CAOM schema name for the entry.

_{entry name}.obsTable_ is the CAOM Observation table name.

_{entry name}.sqlGenerator_ is a plugin implementation to support the database. There is currently only one
implementation that is tested with PostgeSQL (10+), ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator.
Making this work with other database servers in future may require a different implementation.

_{entry name}.bashPublisherID_  is the base for generating Plane publisherID values.
The base is an uri of the form `ivo://<authority>[/<path>]`
publisherID values: `<basePublisherID>/<collection>?<observationID>/<productID>`

_{entry name}.readOnlyGroup_ specifies a group (one per line) that can read (get) matching assets (default: empty list).

_{entry name}.readWriteGroup_ entry specifies a group (one per line) that can read and write (get/put/update/delete)
matching assets (default: empty list).

_{entry name}.operatorGroup_ is an operator group that spans across all collections. (default: no operator group)

_{entry name}.staffGroup_ is a staff group is associated with a collection, and is added as an admin
to the proposal group. Both groups use the same GMS service, with the resourceID of the service
coming from the staff group. (default: no staff group)

_{entry name}.proposalGroup_ is a boolean flag which indicates whether a proposal group is to be generated
for the collection. When proposalGroup=true is specified, a proposal group with an arbitrary prefix is created,
using a GMS service, for the collection. (default: false)

_{entry name}.computeMetadata_ enables computation and persistence of computed metadata(generally, Plane metadata
aggregated from the artifacts). (default: false)

_{entry name}.computeMetadataValidation_ enables extra validation by performing the metadata computations, 
but the values are not persisted (default: false)

_{entry.name}.artifactPattern_ =

_{entry.name}.altPattern_ =

## How to create groups/tuples

In the properties file, configure each collection with the desired proposal group, operator group and staff group.

The proposal group option is a boolean flag which indicates whether a proposal group is to be generated
for the collection. When proposalGroup=true is specified, a proposal group with an arbitrary prefix is created,
using a GMS service, for the collection.

An operator group spans across all collections.

A staff group is associated with a collection, and is added as an admin to the proposal group.
Both groups use the same GMS service, with the resourceID of the service coming from the staff group.

## Configuring groups/tuples

The _operatorGroup_ and _staffGroup_ properties can be used to specify an arbitrary operator group and staff group.
The _proposalGroup_ flag enables proposal groups to be checked/created and granted access to observations.
If created, proposal groups have URIs of the form:

```
<GMS service URI>?<collection>-<Observation.proposal.id>
```

Where <GMS service URI> is extracted from the staff group (the staff group isn't assigned admin permission
on the proposal group so both groups must exist in the same GMS service). For example:

```
org.opencadc.sc2repo.entry = TEST
TEST.datasource = jdbc/caom2repo
TEST.database = caom2test
TEST.schema = dbo
TEST.obsTable = caom2_Observation
TEST.sqlGenerator = ca.nrc.cadc.caom2.repo.PostgreSQLGeneratorImpl
TEST.basePublisherID = ivo://cadc.nrc.ca
TEST.readOnlyGroup = ivo://cadc.nrc.ca/gms?caom2TestGroupRead
TEST.readWriteGroup = ivo://cadc.nrc.ca/gms?caom2TestGroupWrite
TEST.staffGroup = ivo://cadc.nrc.ca/gms?JCMT-Staff
TEST.proposalGroup = true
```

configures the `TEST` collection with a staff group and enables proposal group creation.


```
org.opencadc.sc2repo.entry = TEST1
TEST1.datasource = jdbc/caom2repo
TEST1.database = caom2test
TEST1.schema = dbo
TEST1.obsTable = caom2_Observation
TEST1.sqlGenerator = ca.nrc.cadc.caom2.repo.PostgreSQLGeneratorImpl
TEST1.basePublisherID = ivo://cadc.nrc.ca
TEST1.readOnlyGroup = ivo://cadc.nrc.ca/gms?caom2TestGroupRead
TEST1.readWriteGroup = ivo://cadc.nrc.ca/gms?caom2TestGroupWrite
TEST1.operatorGroup = ivo://cadc.nrc.ca/gms?CADC
TEST1.staffGroup = ivo://cadc.nrc.ca/gms?JCMT-Staff
TEST1.proposalGroup = false
```

configures the `TEST1` collection with an operator group and a staff group without a proposal group, and is equivalent to:


```
org.opencadc.sc2repo.entry = TEST1
TEST1.datasource = jdbc/caom2repo
TEST1.database = caom2test
TEST1.schema = dbo
TEST1.obsTable = caom2_Observation
TEST1.sqlGenerator = ca.nrc.cadc.caom2.repo.PostgreSQLGeneratorImpl
TEST1.basePublisherID = ivo://cadc.nrc.ca
TEST1.readOnlyGroup = ivo://cadc.nrc.ca/gms?caom2TestGroupRead
TEST1.readWriteGroup = ivo://cadc.nrc.ca/gms?caom2TestGroupWrite
TEST1.operatorGroup = ivo://cadc.nrc.ca/gms?CADC
TEST1.staffGroup = ivo://cadc.nrc.ca/gms?JCMT-Staff
```

because the default value of `proposalGroup` is `false`.


### database tables
sc2repo requires a PostgreSQL database with citext and pg_sphere extensions with a `caom2` schema. The `caom2` tables are created by calling the /sc2repo/availability endpoint, which checks/creates/upgrades the caom2 database content.


## building it
```
gradle clean build
docker build -t sc2repo -f Dockerfile .
```

## checking it
```
docker run --rm -it sc2repo:latest /bin/bash
```

## running it
```
docker run --rm --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name sc2repo sc2repo:latest
```