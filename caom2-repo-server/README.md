The CaomRepoConfig.properties file is an example that works for development; it goes in $HOME/config (TBD).

**How to create groups/tuples**
1. In the CaomRepoConfig.properties file, configure each collection with the desired proposal group, operator group and staff group.
2. Run an application to harvest observations. Groups will be created for the configured collections.

An operator group spans across all collections. A staff group is associated with a collection.

**Configuring groups/tuples**

The CaomRepoConfig.properties file uses the following format:

```
collection = <datasource name> <database> <schema> <obs table> <read-only group> <read-write group> [<SQL generator class>] [<key1=value1,key2=value2,...>]
```

Each entry in the properties file configures a collection. The SQL generator and the key=value pairs are optional. The key=value pairs can be used to specify an abitratry operator group, an arbitrary staff group and a proposal group. For example:

```
TEST = jdbc/caom2repo caom2test dbo caom2_Observation ivo://cadc.nrc.ca/gms#caom2TestGroupRead ivo://cadc.nrc.ca/gms#caom2TestGroupWrite ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl proposalGroup=true,staffGroup=ivo://cadc.nrc.ca/gms?JCMT-Staff
```

configures the 'TEST' collection with a proposal group and a staff group.

```
TEST1 = jdbc/caom2repo caom2test dbo caom2_Observation ivo://cadc.nrc.ca/gms#caom2TestGroupRead ivo://cadc.nrc.ca/gms#caom2TestGroupWrite ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl proposalGroup=false,operatorGroup=ivo://cadc.nrc.ca/gms?CADC,staffGroup=ivo://cadc.nrc.ca/gms?JCMT-Staff
```

configures the 'TEST1' collection with an operator group and a staff group without a proposal group, and is equivalent to:

```
TEST1 = jdbc/caom2repo caom2test dbo caom2_Observation ivo://cadc.nrc.ca/gms#caom2TestGroupRead ivo://cadc.nrc.ca/gms#caom2TestGroupWrite ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl operatorGroup=ivo://cadc.nrc.ca/gms?CADC,staffGroup=ivo://cadc.nrc.ca/gms?JCMT-Staff 
```

because the default value of 'proposalGroup' is 'false'.

When proposalGroup=true is specified, a proposal group with an arbitrary prefix is created for the collection.
