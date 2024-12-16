# fits2caom2

## OBSOLETE: this code is no longer maintained

This is a command-line tool that reads a FITS file and creates or updates a CAOM observation.

The general usage is to run fits2caom2 once per plane: either create an observation with one plane or
add|update a plane within an existing observation (document).

The current version is based on CAOM-2.4.

## changes since 2.3.x
New utypes can be used with CAOM 2.4 but nothing has been added to the default config or mappings.

The utype for CompositeObservation.members has changed to DerivedObservation.members to follow the model.

## integration tests
The intTest target is only usable by someone from CADC since some of the tests require a client certificate with
permission to read protected files from the CADC archive. This is to test that fits2caom2 performs authentication
correctly. The client certificate is $A/test-certificates/fits2caom2.pem (for CADC staff: that should be a symlink 
to the cadcauthtest1 proxy certificate).

