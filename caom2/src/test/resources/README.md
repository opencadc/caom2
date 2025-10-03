# static test files

These sample files are used by unit tests to verify that older versions of instance
documents can still be read and that code changes have not modified metaChecksum
computation.

- sample-composite-caom23.xml: complete CAOM-2.3 observation
- sample-composite-caom24.xml : complete CAOM-2.4 observation (but see below)
- sample-composite-caom24-2019.xml : complete CAOM-2.4 observation
- sample-composite-caom24-compareTo-sorted.xml : unused

Note about CAOM-2.4: The original sample (now `sample-composite-caom24-2019.xml`) was 
missing several values that are part the model: Plane.observable, Position.resolutionBounds,
Energy.resolvingPowerBounds, Time.Position.resolutionBounds. These were added to the 
test code and saved in an updated `sample-composite-caom24.xml`.
