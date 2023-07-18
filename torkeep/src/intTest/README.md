# integration testing

The intTest target currently relies on undocumented details specific to CADC so
cannot be feasibly run by others.

Client certificates named `torkeep-test-auth.pem` and `torkeep-test-noauth.pem` must exist in the directory $A/test-certificates.

The integration tests assume the collection name is `TEST`.

The `torkeep-test-auth.pem` must belong to a user identity that is a member of a configured read-write group 
for the `TEST` collection in a permissions granting service. 

The `torkeep-test-noauth.pem` must belong to a user who is not a member of a configured read-write group or 
read-only group, and does not have permissions to access the TEST collection.

### baldur config
```
# permissions for caom:TEST/
org.opencadc.baldur.entry = CAOM-TEST
org.opencadc.torkeep.collection = TEST
CAOM-TEST.pattern = ^caom:TEST/.*
CAOM-TEST.readWriteGroup = ivo://cadc.nrc.ca/gms?caom2TestGroupWrite
```
suitable for using cadcauthtest1 and cadcregtest1.

### torkeep config
For the caom:TEST/ collection:
```
org.opencadc.torkeep.collection = TEST
TEST.basePublisherID = ivo://opencadc.org/
TEST.computeMetadata = true
```
