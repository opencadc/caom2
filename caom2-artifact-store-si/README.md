# caom2-artifact-store-si

This libary provides a caom2-persist ArtifactStore implementation that
works with the CADC Storage Inventory backend (luskan and minoc).

The ArtifactStore implementation provided is `ca.nrc.cadc.caom2.artifactsync.InventoryArtifactStore`.

## configuration files

### caom2-artifact-store-si.properties

```
ca.nrc.cadc.caom2.artifactsync.locateService = {resourceID for global inventory locator service}
ca.nrc.cadc.caom2.artifactsync.queryService = {resourceID for global inventory query service}
```

Example for CADC deployment:
```
ca.nrc.cadc.caom2.artifactsync.locateService=ivo://cadc.nrc.ca/global/raven
ca.nrc.cadc.caom2.artifactsync.queryService=ivo://cadc.nrc.ca/global/luskan
```

### collection-prefix.properties (legacy)

This legacy config file is subject to change in name and content.

```
{collection} = {storage inventory namespace, URI prefix}
{collection}.policy = PublicOnly | All
```
One pair of properties configures one data collection namespace and storage policy.

Example for CADC deployment:
```
BLAST = cadc:BLAST/
BLAST.policy = All

HST = mast:HST/
HST.policy = PublicOnly
```
