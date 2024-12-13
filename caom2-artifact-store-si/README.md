# caom2-artifact-store-si

This libary provides a caom2-persist ArtifactStore implementation that
works with the CADC Storage Inventory backend (luskan and minoc).

The ArtifactStore implementation provided is `org.opencadc.caom2.inventory.InventoryArtifactStore`.

## configuration files

### caom2-artifact-store-si.properties

```
org.opencadc.caom2.inventory.locateService = {resourceID for global inventory locator service}
org.opencadc.caom2.inventory.queryService = {resourceID for global inventory query service}
```

Example for CADC deployment:
```
org.opencadc.caom2.inventory.locateService=ivo://cadc.nrc.ca/global/raven
org.opencadc.caom2.inventory.queryService=ivo://cadc.nrc.ca/global/luskan
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
