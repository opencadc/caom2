# bifrost integration tests

The tests do a registry lookup of resourceID `ivo://opencadc.org/bifrost`.

Most of the tests are anonymous, but a few require authentication with an X509 client
certificate named `$A/test-certificates/bifrost.pem`.

## content
The tests expect a few IRIS observations in the CAOM database. These can be obtained
by running `icewind` (was caom2harvester) with basePublisherID=ivo://opencadc.org/
to give them the expected "developer" publisherID.

