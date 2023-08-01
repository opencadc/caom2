# caom2-repo-server

## OBSOLETE: this library is obsolete; all functionality has been forked into `torkeep` (complete service build for container
depoyment).

The CaomRepoConfig.properties file provides a simple example that could be used for testing. For real deployment, this library will look for a file named <service name>.properties in ${user.home}/config of the user running the application server (e.g. tomcat). The <service name> is the first path element in URLs and typically matches the name of the deployed war file. For example, The CADC deploys this service as caom2repo.war so the config file is called ${user.home}/config/caom2repo.properties. Note that this config file is read from the filesystem for each request so changes are "immediately live".

The CaomRepoConfig.properties file uses the following format:

```
collection = <datasource name> <database> <schema> <obs table> <read-only group> <read-write group> <SQL generator class> basePublisherID=<ivo uri> [<key1=value1 key2=value2 ...>]
```

Each entry in the properties file configures a collection. Except for basePublisherID (mandatory), key=value pairs are optional. The computeMetadata option enables computation and persistence of <<computed>> metadata (generally, Plane metadata aggregated from the artifacts). The computeMetadataValidation options enables extra validation by performing the metadata computations, but the values are not persisted.


**How to create groups/tuples**

In the CaomRepoConfig.properties file, configure each collection with the desired proposal group, operator group and staff group.

The proposal group option is a boolean flag which indicates whether a proposal group is to be generated for the collection. When proposalGroup=true is specified, a proposal group with an arbitrary prefix is created, using a GMS service, for the collection. 

An operator group spans across all collections. 

A staff group is associated with a collection, and is added as an admin to the proposal group. Both groups use the same GMS service, with the resourceID of the service coming from the staff group.

**Configuring groups/tuples**

The optional key=value pairs can be used to specify an abitratry operator group, an arbitrary staff group and a flag to enable proposal groups to be checked/created and granted access to observations. If created, proposal groups have URIs of the form 

```
<GMS service URI>?<collection>-<Observation.proposal.id>
```

Where <GMS service URI> is extracted from the staff group (the satff group isn assigned admin permission on the proposal group so both groups must exist in the same GMS service). For example:

```
TEST = jdbc/caom2repo caom2test dbo caom2_Observation ivo://cadc.nrc.ca/gms?caom2TestGroupRead ivo://cadc.nrc.ca/gms?caom2TestGroupWrite ca.nrc.cadc.caom2.repo.PostgreSQLGeneratorImpl basePublisherID=ivo://cadc.nrc.ca proposalGroup=true staffGroup=ivo://cadc.nrc.ca/gms?JCMT-Staff
```

configures the 'TEST' collection with a staff group and enables proposal group creation.

```
TEST1 = jdbc/caom2repo caom2test dbo caom2_Observation ivo://cadc.nrc.ca/gms?caom2TestGroupRead ivo://cadc.nrc.ca/gms?caom2TestGroupWrite ca.nrc.cadc.caom2.repo.PostgreSQLGeneratorImpl proposalGroup=false operatorGroup=ivo://cadc.nrc.ca/gms?CADC staffGroup=ivo://cadc.nrc.ca/gms?JCMT-Staff
```

configures the 'TEST1' collection with an operator group and a staff group without a proposal group, and is equivalent to:

```
TEST1 = jdbc/caom2repo caom2test dbo caom2_Observation ivo://cadc.nrc.ca/gms?caom2TestGroupRead ivo://cadc.nrc.ca/gms?caom2TestGroupWrite ca.nrc.cadc.caom2.repo.PostgreSQLGeneratorImpl operatorGroup=ivo://cadc.nrc.ca/gms?CADC staffGroup=ivo://cadc.nrc.ca/gms?JCMT-Staff 
```

because the default value of 'proposalGroup' is 'false'.
