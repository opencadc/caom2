
This sub-module builds an alternate version of caom2harvester (same code) that enables some CADC-cpecific
features.

1. The --destination is used for both the source and destination database, so this build will read and write in
a single database.

2. --compute computes Plane metadata from artifact-part-chunk (WCS)

3. --generate-ac computes Observation and Plane read-access groups (grants) from config

These features are used to perform backfill or fixes when code or configured polciy changes.

