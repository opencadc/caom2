
This is a command-line interface to the validation that is included in the caom2 and caom2-compute libraries.

Without optional validator arguments, caom2-validator will read the input document and perform schema
and core data structure validation.

* --checksum

With the --checksum argument, caom2-validator recomputes checksums and compares them to the values in the
input document. The --depth option can be used to limit the checksum validation to just the observation 
(--depth=1), the plane (--depth=2), etc. The default is to validate everything (--depth=5). The --acc
option also enables compute and comparison of accumulated metadata checksum values (accMetaChecksum).

** --wcs
With the --wcs argument, caom2-validator performs WCS validation on the entire observation. Failures
are shown with the artifact uri, part name, and chunk uuid so that the offending chunk can be found.
NOTE: WCS validation requires a working wcslib (version 5.x).

