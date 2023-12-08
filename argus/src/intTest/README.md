# argus integration tests

The tests do a registry lookup of resourceID `ivo://opencadc.org/argus`.

Most of the tests are anonymous, but a few require authentication with an X509 client and are only feasible 
for a CADC staff member to run. Required certificates:
* `$A/test-certificates/argus-auth.pem` : a user that can send async output to ivo://cadc.nrc.ca/vault
* `$A/test-certificates/argus-noauth.pem` : a valid user that has no permission to see proprietary metadata

The _auth_ identity has to be the (CADC) staff member's personal certificate because the test tries to output
to `vos://cadc.nrc.ca~vault/{user.name}/test/some-file-name`. The _noauth_ identity should be cadcregtest1.

