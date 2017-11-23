The CaomRepoConfig.properties file is an example that works for development; it goes in $HOME/config (TBD).

**How to create groups/tuples**

In the CaomRepoConfig.properties file, configure each collection with the desired proposal group, operator group and staff group.

The proposal group option is a boolean flag which indicates whether a proposal group is to be generated for the collection. When proposalGroup=true is specified, a proposal group with an arbitrary prefix is created, using a GMS service, for the collection. 

An operator group spans across all collections. 

A staff group is associated with a collection, and is added as an admin to the proposal group. Both groups use the same GMS service, with the resourceID of the service coming from the staff group.

**Configuring groups/tuples**

The CaomRepoConfig.properties file uses the following format:

```
collection = <datasource name> <database> <schema> <obs table> <read-only group> <read-write group> <SQL generator class> [<key1=value1,key2=value2,...>]
```

Each entry in the properties file configures a collection. The key=value pairs are optional. The key=value pairs can be used to specify an abitratry operator group, an arbitrary staff group and a proposal group, in addition to computeMetadata and computeMetadataValidation. For example:

```
TEST = jdbc/caom2repo caom2test dbo caom2_Observation ivo://cadc.nrc.ca/gms?caom2TestGroupRead ivo://cadc.nrc.ca/gms?caom2TestGroupWrite ca.nrc.cadc.caom2.repo.PostgreSQLGeneratorImpl proposalGroup=true,staffGroup=ivo://cadc.nrc.ca/gms?JCMT-Staff
```

configures the 'TEST' collection with a proposal group and a staff group.

```
TEST1 = jdbc/caom2repo caom2test dbo caom2_Observation ivo://cadc.nrc.ca/gms?caom2TestGroupRead ivo://cadc.nrc.ca/gms?caom2TestGroupWrite ca.nrc.cadc.caom2.repo.PostgreSQLGeneratorImpl proposalGroup=false,operatorGroup=ivo://cadc.nrc.ca/gms?CADC,staffGroup=ivo://cadc.nrc.ca/gms?JCMT-Staff
```

configures the 'TEST1' collection with an operator group and a staff group without a proposal group, and is equivalent to:

```
TEST1 = jdbc/caom2repo caom2test dbo caom2_Observation ivo://cadc.nrc.ca/gms?caom2TestGroupRead ivo://cadc.nrc.ca/gms?caom2TestGroupWrite ca.nrc.cadc.caom2.repo.PostgreSQLGeneratorImpl operatorGroup=ivo://cadc.nrc.ca/gms?CADC,staffGroup=ivo://cadc.nrc.ca/gms?JCMT-Staff 
```

because the default value of 'proposalGroup' is 'false'.
