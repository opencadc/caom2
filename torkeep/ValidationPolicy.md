# Validation Policy

Additional validation of input observations (PUT and POST) can be configured for each 
collection via a configuration file. In this initial prototype version, the config file
is simply a list of **vodml-id** values for values in the model that must be included
(are not allowed to be null). This list is in addition to any constraints specified by
the model itself.

This feature is currently an early prototype so file format may change in future. However,
it is possible that _not null_ and _not empty_ constraints can still be as simple as a
key without a value.

## Core Model Requirements
The CAOM data model requires that the following fields are not null:
```
Observation.collection
Observation.uri
Observation.algorithm
Algorithm.name

Plane.uri

Artifact.uri
Artifact.productType
Artifact.releaseType
```
Including these in the validation policy configuration is file but has no effect. The above 
list is subject to change before WD-CAOM-2.5 is finalized.

Note: Although Plane.uri cannot be null _in a Plane_, there is no requirement that an observation
actually contains any planes (e.g. that `Observation.planes` is not empty). Likewise, `Plane.artifacts`
may also be empty.

## Extended Not-Null Constraints
Additional required fields may be specified by including the `vodml-id` values for either classes 
or leaf (primitive) values. For example:
```
Observation.telescope
Observation.instrument
Observation.intent
```
would add constraints requiring every observation instance include telescope and instrument values
(the Telescope and Instrument classes respectively). Since both of those classes name a required `name`
field, that field does not have to be configured explicitly. The above also requires that the observation
`intent` has a value; the actual value will be validated using the defintion of the ObservationIntentType
enumeration in the model.

Optional fields in any class can be made required by including the `vodml-id`, but as described above that
requirement is only relative to the containing class. For example,
```
Telescope.trackingMode
```
would require that all instances of Telescope include a value for `trackingMode`, but to require that all
observations have a telescope **and** a trackingMode would be configured explicitly as:
```
Observation.telescope
Telescope.trackingMode
```

## Extended Not-Empty Constraints
The WD-CAOM-2.5 model does not require that observations contain any planes or that planes contain any artifacts.
Including the following `vodml-id` values in the validation policy will require _at least one_ such entity:
```
Observation.planes
Plane.artifacts
```
With these two requirements, the minimal observation will have one plane and that plane will have one artifact.

## Example
A fairly complete example that should be usable in most cases:
[validation-policy-example.properties](validation-policy-example.properties)
