
-- delete key columns for keys from tables in the caom2 schema
delete from tap_schema.key_columns11 where
key_id in (select key_id from tap_schema.keys11 where 
    from_table in (select table_name from tap_schema.tables11 where schema_name = 'caom2')
    or
    target_table in (select table_name from tap_schema.tables11 where schema_name = 'caom2')
)
;

-- delete keys from tables in the caom2 schema
delete from tap_schema.keys11 where 
from_table in (select table_name from tap_schema.tables11 where schema_name = 'caom2')
or
target_table in (select table_name from tap_schema.tables11 where schema_name = 'caom2')
;

-- delete columns from tables in the caom2 schema
delete from tap_schema.columns11 where table_name in 
(select table_name from tap_schema.tables11 where schema_name = 'caom2')
;

-- delete tables in the caom2 schema
delete from tap_schema.tables11 where schema_name = 'caom2'
;

-- delete the caom2 schema
delete from tap_schema.schemas11 where schema_name = 'caom2'
;

select 'after cleanup' AS "after cleanup",table_name from tap_schema.tables11 where schema_name= 'caom2';

insert into tap_schema.schemas11 (schema_name,description,utype) values
('caom2', 'Common Archive Observation Model, version 2.5', 'caom2')
;

-- index start at 20
insert into tap_schema.tables11 (schema_name,table_name,table_type,description,utype,table_index) values
( 'caom2', 'caom2.Observation', 'table', 'the main CAOM Observation table', 'caom2:Observation' , 20),
( 'caom2', 'caom2.Plane', 'table', 'the products of the observation', 'caom2:Plane' , 21),
( 'caom2', 'caom2.Artifact', 'table', 'physical data artifacts (e.g. files)', 'caom2:Artifact' , 22),
( 'caom2', 'caom2.ObservationMember', 'table', 'composite to simple observation join table', NULL, 25),
( 'caom2', 'caom2.ProvenanceInput', 'table', 'plane.provenance to input plane join table', NULL, 26),
( 'caom2', 'caom2.ArtifactDescription', 'table', 'artifact description lookup table', 'caom2:ArtifactDescription', 27),
( 'caom2', 'caom2.DeletedObservationEvent', 'table', 'record indicating an Observation was deleted', 'caom2:DeletedObservationEvent', 28),
( 'caom2', 'caom2.DeletedArtifactDescriptionEvent', 'table', 'record indicating an ArtifactDescription was deleted', 'caom2:DeletedObservationEvent', 29)
;

-- Observation
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index,column_id) values
( 'caom2.Observation', 'uri', 'unique URI for this observation', 'caom2:Observation.uri', NULL, NULL, 'char', '*','uri', 1,1,1,1, 'caomObservationURI')
;

insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Observation', 'uriBucket', 'short hex string', 'caom2:Observation.uriBucket', NULL, NULL, 'char','3',NULL, 0,1,0 , 2),
( 'caom2.Observation', 'collection', 'data collection this observation belongs to', 'caom2:Observation.collection', NULL, NULL, 'char','32*',NULL, 1,1,1 , 3),

( 'caom2.Observation', 'algorithm_name', 'algorithm that defines the observation( exposure for simple, grouping algorithm for composites)', 'caom2:Observation.algorithm.name', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 5),
( 'caom2.Observation', 'obstype', 'type of data collected (e.g. FITS OBSTYPE header)', 'caom2:Observation.type', NULL, NULL, 'char', '32*', NULL, 1,0,1 , 6),
( 'caom2.Observation', 'intent', 'intended purpose of data (one of: science, calibration)', 'caom2:Observation.intent', NULL, NULL, 'char', '32*', NULL, 0,0,1, 7),
( 'caom2.Observation', 'sequenceNumber', 'sequence number assigned by the observatory', 'caom2:Observation.sequenceNumber', NULL, NULL, 'int', NULL, NULL, 0,0,0, 8),
( 'caom2.Observation', 'metaRelease', 'date the metadata for an observation is public (UTC)', 'caom2:Observation.metaRelease', NULL, NULL, 'char', '23*','timestamp', 0,0,0, 9),
( 'caom2.Observation', 'metaReadGroups', 'GMS groups that are authorized to see metadata', 'caom2:Observation.metaReadGroups', NULL, NULL, 'char', '*', NULL, 0,0,0, 10),

( 'caom2.Observation', 'proposal_id', 'collection-specific unique proposal identifier', 'caom2:Proposal.id', NULL, NULL, 'char', '128*',NULL, 0,1,0 , 20),
( 'caom2.Observation', 'proposal_pi', 'proposal principal investigator', 'caom2:Proposal.pi', NULL, NULL, 'char', '128*',NULL, 0,0,0 , 21),
( 'caom2.Observation', 'proposal_project', 'project that created/acquired the observation', 'caom2:Proposal.project', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 22),
( 'caom2.Observation', 'proposal_title', 'proposal title', 'caom2:Proposal.title', NULL, NULL, 'char', '256*',NULL, 0,0,0 , 23),
( 'caom2.Observation', 'proposal_keywords', 'proposal keywords (separated by |)', 'caom2:Proposal.keywords', NULL, NULL, 'char', '*',NULL, 0,0,0 , 24),
( 'caom2.Observation', 'proposal_reference', 'reference to external proposal info [new in 2.5]', 'caom2:Proposal.reference', NULL, NULL, 'char', '*',NULL, 0,0,0 , 25),

( 'caom2.Observation', 'target_name', 'name of intended target', 'caom2:Target.name', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 30),
( 'caom2.Observation', 'target_targetID', 'identifier of intended target [new in 2.4]', 'caom2:Target.targetID', NULL, NULL, 'char', '32*','uri', 1,0,0 , 31),
( 'caom2.Observation', 'target_type', 'classification of intended target', 'caom2:Target.type', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 32),
( 'caom2.Observation', 'target_standard', 'intended target is a standard (0=false, 1=true)', 'caom2:Target.standard', NULL, NULL, 'int', NULL,NULL, 0,0,0 , 33),
( 'caom2.Observation', 'target_redshift', 'redshift of intended target', 'caom2:Target.redshift', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 34),
( 'caom2.Observation', 'target_moving', 'flag for moving target', 'caom2:Target.moving', NULL, NULL, 'int', NULL,NULL, 0,0,0 , 35),
( 'caom2.Observation', 'target_keywords', 'target keywords (separated by |)', 'caom2:Target.keywords', NULL, NULL, 'char', '*',NULL, 0,0,0 , 36),

( 'caom2.Observation', 'targetPosition_coordsys', 'coordinate system for target position', 'caom2:TargetPosition.coordsys', NULL, NULL, 'char', '16*',NULL, 0,0,0 , 40),
( 'caom2.Observation', 'targetPosition_equinox', 'equinox of target position coordinate system', 'caom2:TargetPosition.equinox', NULL, NULL, 'double', NULL,NULL, 0,0,0 , 41),
( 'caom2.Observation', 'targetPosition_coordinates', 'intended position to observe', 'caom2:TargetPosition.coordinates', NULL, 'deg', 'double', '2', 'point', 0,0,0 , 41),

( 'caom2.Observation', 'telescope_name', 'name of telescope used to acquire observation', 'caom2:Telescope.name', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 45),
( 'caom2.Observation', 'telescope_geoLocationX', 'x component of geocentric location of telescope', 'caom2:Telescope.geoLocationX', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 46),
( 'caom2.Observation', 'telescope_geoLocationY', 'y component of geocentric location of telescope', 'caom2:Telescope.geoLocationY', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 47),
( 'caom2.Observation', 'telescope_geoLocationZ', 'z component of geocentric location of telescope', 'caom2:Telescope.geoLocationZ', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 48),
( 'caom2.Observation', 'telescope_keywords', 'telescope keywords (separated by |)', 'caom2:Telescope.keywords', NULL, NULL, 'char', '*', NULL, 0,0,0 , 49),
( 'caom2.Observation', 'telescope_trackingMode', 'telescope tracking mode [new in 2.5]', 'caom2:Telescope.trackingMode', NULL, NULL, 'char', '64*', NULL, 0,0,0 , 50),

( 'caom2.Observation', 'requirements_flag', 'flag describing satisfied proposal requirements (possible values: fail)', 'caom2:Observation.requirements.flag', NULL, NULL, 'char', '16',NULL, 1,0,0 , 50),

( 'caom2.Observation', 'instrument_name', 'name of instrument used to acquire observation', 'caom2:Instrument.name', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 60),
( 'caom2.Observation', 'instrument_keywords', 'instrument keywords (separated by |)', 'caom2:Instrument.keywords', NULL, NULL, 'char', '*',NULL, 0,0,0 , 61),

( 'caom2.Observation', 'environment_seeing', 'atmospheric seeing (FWHM)', 'caom2:Environment.seeing', NULL, 'arcsec', 'double', NULL, NULL, 0,0,0 , 70),
( 'caom2.Observation', 'environment_humidity', 'humidity at telescope during observation', 'caom2:Environment.humidity', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 71),
( 'caom2.Observation', 'environment_elevation', 'elevation above horizon (0 to 90)', 'caom2:Environment.elevation', NULL, 'deg', 'double', NULL, NULL, 0,0,0 , 72),
( 'caom2.Observation', 'environment_tau', 'zenith optical depth', 'caom2:Environment.tau', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 72),
( 'caom2.Observation', 'environment_wavelengthTau', 'wavelength at which Tau has been measured', 'caom2:Environment.wavelengthTau', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 74),
( 'caom2.Observation', 'environment_ambientTemp', 'ambient temperature telescope during observation', 'caom2:Environment.ambientTemp', NULL, 'C', 'double', NULL, NULL, 0,0,0 , 75),
( 'caom2.Observation', 'environment_photometric', 'conditions were photometric (0=false, 1=true)', 'caom2:Environment.photometric', NULL, NULL, 'int', NULL, NULL, 0,0,0 , 76),

( 'caom2.Observation', 'members', 'members of a composite observation (space-separated list of Observation URIs)', 'caom2:Observation.members', NULL, NULL, 'char','*',NULL, 0,0,0 , 80),
( 'caom2.Observation', 'typeCode', 'single character code to denote type: S(impleObservation) or D(erivedOvservation)', 'caom2:Observation.typeCode', NULL, NULL, 'char',NULL,NULL, 0,0,0 , 81),

( 'caom2.Observation', 'obsID', 'primary key', 'caom2:Entity.id', NULL, NULL, 'char','36','uuid', 0,1,0 , 120),
( 'caom2.Observation', 'metaProducer', 'identifier for the producer of this entity metadata (URI of the form {organisation}:{software}-{version}) [new in 2.4]', 'caom2:Entity.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 121),
( 'caom2.Observation', 'lastModified', 'timestamp of last modification of this row', 'caom2:Entity.lastModified', NULL, NULL, 'char', '23*','timestamp', 1,0,0 , 122),
( 'caom2.Observation', 'maxLastModified', 'timestamp of last modification of this entity+children', 'caom2:CaomEntity.maxLastModified', NULL, NULL, 'char', '23*','timestamp', 1,0,0 , 123),
( 'caom2.Observation', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Entity.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 124),
( 'caom2.Observation', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form {algorithm}:{hex value})', 'caom2:CaomEntity.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 125)
;

-- Plane
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index,column_id) values
( 'caom2.Plane', 'publisherID', 'unique publisher identifier for this product', NULL, NULL, NULL, 'char', '*', 'uri', 1,1,1,1, 'caomPublisherID')
;
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Plane', 'obsID', 'foreign key', NULL, NULL, NULL,                              'char','36','uuid', 0,1,0 , 2),
( 'caom2.Plane', 'uri', 'unique internal URI for this product', 'caom2:Plane.uri', NULL, NULL, 'char', '*', 'uri', 1,1,1 , 3),

( 'caom2.Plane', 'metaRelease', 'date the metadata for a plane is public (UTC)', 'caom2:Plane.metaRelease', NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 , 7),
( 'caom2.Plane', 'metaReadGroups', 'GMS groups that are authorized to see metadata [new in 2.4]', 'caom2:Plane.metaReadGroups', NULL, NULL, 'char', '*',NULL, 0,0,0, 8),
( 'caom2.Plane', 'dataRelease', 'date the data for a plane is public (UTC)', 'caom2:Plane.dataRelease', NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 , 9),
( 'caom2.Plane', 'dataReadGroups', 'GMS groups that are authorized to see data [new in 2.4]', 'caom2:Plane.dataReadGroups', NULL, NULL, 'char', '*',NULL, 0,0,0, 10),
( 'caom2.Plane', 'dataProductType', 'IVOA ObsCore data product type + extensions', 'caom2:Plane.dataProductType', NULL, NULL, 'char', '128*', NULL, 1,0,1 , 11),
( 'caom2.Plane', 'calibrationLevel', 'IVOA ObsCore calibration level + extensions (-1,0,1,2,3,4)', 'caom2:Plane.calibrationLevel', NULL, NULL, 'int', NULL, NULL, 1,0,1 , 12),

( 'caom2.Plane', 'provenance_name', 'name of the process that created this plane', 'caom2:Provenance.name', NULL, NULL, 'char', '128*', NULL, 0,0,1 , 20),
( 'caom2.Plane', 'provenance_version', 'version of the process/software', 'caom2:Provenance.version', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 21),
( 'caom2.Plane', 'provenance_reference', 'external reference (URL)', 'caom2:Provenance.reference', NULL, NULL, 'char', '256*', NULL, 0,0,0 , 22),
( 'caom2.Plane', 'provenance_producer', 'responsible entity (e.g. person)', 'caom2:Provenance.producer', NULL, NULL, 'char', '128*', NULL, 0,0,0, 23),
( 'caom2.Plane', 'provenance_project', 'responsible entity (e.g. person)', 'caom2:Provenance.project', NULL, NULL, 'char', '256*', NULL, 0,0,0, 24),
( 'caom2.Plane', 'provenance_runID', 'responsible entity (e.g. person)', 'caom2:Provenance.runID', NULL, NULL, 'char', '64*', NULL, 0,1,0, 25),
( 'caom2.Plane', 'provenance_lastExecuted', 'date this process was last executed', 'caom2:Provenance.lastExecuted', NULL, NULL, 'char', '23*', 'timestamp', 0,0,0 , 26),
( 'caom2.Plane', 'provenance_keywords', 'provenance keywords (separated by |)', 'caom2:Provenance.keywords', NULL, NULL, 'char', '*', NULL, 0,0,0 , 27),
( 'caom2.Plane', 'provenance_inputs', 'inputs of the process that created this plane (space-separated list of Plane URIs)', 'caom2:Provenance.inputs', NULL, NULL, 'char','*',NULL, 0,0,0 , 28),

( 'caom2.Plane', 'metrics_sourceNumberDensity', 'number density of sources', 'caom2:Metrics.sourceNumberDensity', NULL, 'deg-2', 'double', NULL, NULL, 0,0,0 , 30),
( 'caom2.Plane', 'metrics_background', 'background intensity', 'caom2:Metrics.background', NULL, 'Jy/pix', 'double', NULL,NULL, 0,0,0 , 31),
( 'caom2.Plane', 'metrics_backgroundStddev', 'standard deviation in background', 'caom2:Metrics.backgroundStddev', NULL, 'Jy/pix', 'double', NULL, NULL, 0,0,0 , 32),
( 'caom2.Plane', 'metrics_fluxDensityLimit', 'flux density limit where S:N=5 for point source', 'caom2:Metrics.fluxDensityLimit', NULL, 'Jy', 'double', NULL, NULL, 0,0,0 , 33),
( 'caom2.Plane', 'metrics_magLimit', 'AB magnitude limit where S:N=5 for point source', 'caom2:Metrics.magLimit', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 34),

( 'caom2.Plane', 'observable_ucd', 'UCD (Universal Content Descriptor) for the observed/measure quantity', 'caom2:Observable.ucd', NULL, NULL, 'char', '64*', NULL, 0,0,0 , 40),
( 'caom2.Plane', 'observable_calibration', 'calibration status of the observed/measure quantity [new in 2.5]', 'caom2:Observable.calibration', NULL, NULL, 'char', '64*', NULL, 0,0,0 , 41),
( 'caom2.Plane', 'quality_flag', 'flag describing the data quality (possible values: junk)', 'caom2:Quality.flag', NULL, NULL, 'char', '16*', NULL, 0,0,0 , 42),

( 'caom2.Plane', 'position_bounds', 'queryable positional coverage of the data', 'caom2:Position.bounds', NULL, 'deg', 'char', '*', 'shape', 0,0,0 , 50),
( 'caom2.Plane', 'position_samples', 'detailed positional coverage of the data', 'caom2:Position.samples', NULL, 'deg', 'char', '*', 'multishape', 0,0,0 , 51),
( 'caom2.Plane', 'position_minBounds', 'queryable minimum positional coverage... TBD [new in 2.5]', 'caom2:Position.minBounds', NULL, 'deg', 'char', '*', 'shape', 0,0,0 , 52),
( 'caom2.Plane', 'position_dimension', 'dimensions (number of pixels) along the spatial axes', 'caom2:Position.dimension', NULL, NULL, 'long', '2', NULL, 0,0,0 , 53),
( 'caom2.Plane', 'position_maxRecoverableScale', 'TBD [new in 2.5]', 'caom2:Position.maxRecoverableScale', NULL, 'arcsec', 'double', '2', 'interval', 0,0,0 , 54),
( 'caom2.Plane', 'position_resolution', 'median spatial resolution (FWHM)', 'caom2:Position.resolution', NULL, 'arcsec', 'double', NULL, NULL, 0,0,0 , 55),
( 'caom2.Plane', 'position_resolutionBounds', 'range of median spatial resolution (FWHM)', 'caom2:Position.resolutionBounds', NULL, 'arcsec', 'double', '2', 'interval', 0,0,0 , 55),
( 'caom2.Plane', 'position_sampleSize', 'median sample (pixel) size on spatial axes', 'caom2:Position.sampleSize', NULL, 'arcsec', 'double', NULL, NULL, 0,0,0 , 56),
( 'caom2.Plane', 'position_calibration', 'calibration of the position metadata (astrometry) [new in 2.5]', 'caom2:Position.timeDependent', NULL, NULL, 'char', '64*', NULL, 0,0,0 , 56),

( 'caom2.Plane', 'energy_bounds', 'queryable energy coverage', 'caom2:Energy.bounds', NULL, 'm', 'double', '2','interval', 0,1,0 , 61),
( 'caom2.Plane', 'energy_samples', 'detailed energy coverage', 'caom2:Energy.samples', NULL, 'm', 'double', '*', 'multiinterval', 0,0,0 , 62),
( 'caom2.Plane', 'energy_dimension', 'dimension (number of pixels) along energy axis', 'caom2:Energy.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 63),
( 'caom2.Plane', 'energy_resolvingPower', 'representative relative energy resolution aka resolving power (R)', 'caom2:Energy.resolvingPower', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 66),
( 'caom2.Plane', 'energy_resolvingPowerBounds', 'range of relative energy resolution (R)', 'caom2:Energy.resolvingPowerBounds', NULL, NULL, 'double', '2', 'interval', 0,0,0 , 65),
( 'caom2.Plane', 'energy_resolution', 'representative absolute energy resolution', 'caom2:Energy.resolution', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 66),
( 'caom2.Plane', 'energy_resolutionBounds', 'range of absolute energy resolution', 'caom2:Energy.resolutionBounds', NULL, 'm', 'double', '2', 'interval', 0,0,0 , 67),
( 'caom2.Plane', 'energy_sampleSize', 'representative sample (pixel) size on energy axis', 'caom2:Energy.sampleSize', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 68),
( 'caom2.Plane', 'energy_rest', 'rest energy of target spectral feature', 'caom2:Energy.rest', NULL, 'm', 'double', NULL, NULL, 0,1,0 , 69),
( 'caom2.Plane', 'energy_bandpassName', 'collection-specific name for energy band (e.g. filter name)', 'caom2:Energy.bandpassName', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 70),
( 'caom2.Plane', 'energy_energyBands', 'energy band names (Radio,Millimeter,Infrared,Optical,UV,EUV,X-ray,Gamma-ray), separated by |', 'caom2:Energy.energyBands', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 71),
( 'caom2.Plane', 'energy_calibration', 'calibration of the energy axis', 'caom2:Energy.calibration', NULL, NULL, 'char', '64*', NULL, 0,0,0 , 72),
( 'caom2.Plane', 'energy_transition_species', 'atom or molecule', 'caom2:Transition.species', NULL, NULL, 'char', '32*',NULL, 0,0,0 , 73),
( 'caom2.Plane', 'energy_transition_transition', 'specific energy transition of species', 'caom2:Transition.transition', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 74),

( 'caom2.Plane', 'time_bounds', 'time coverage (Modified Julian Day)', 'caom2:Time.bounds', NULL, 'd', 'double', '2', 'interval', 0,1,0 , 80),
( 'caom2.Plane', 'time_samples', 'detailed time coverage (Modified Julian Day)', 'caom2:Time.bounds.samples', NULL, 'd', 'double', '*', 'multiinterval', 0,0,0 , 81),
( 'caom2.Plane', 'time_dimension', 'dimension (number of pixels) along time axis', 'caom2:Time.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 84),
( 'caom2.Plane', 'time_resolution', 'median resolution on the time axis', 'caom2:Time.resolution', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 85),
( 'caom2.Plane', 'time_resolutionBounds', 'range of resolution on the time axis [new in 2.4]', 'caom2:Time.resolutionBounds', NULL, 'd', 'double', '2', 'interval', 0,0,0 , 86),
( 'caom2.Plane', 'time_exposure', 'representative exposure time (mean per pixel?)', 'caom2:Time.exposure', NULL, 's', 'double', NULL, NULL, 0,0,0 , 88),
( 'caom2.Plane', 'time_exposureBounds', 'range of exposure on the time axis [new in 2.4]', 'caom2:Time.exposureBounds', NULL, 's', 'double', '2', 'interval', 0,0,0 , 86),
( 'caom2.Plane', 'time_sampleSize', 'median sample (pixel) size on time axis', 'caom2:Time.sampleSize', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 87),
( 'caom2.Plane', 'time_calibration', 'calibration of the time axis', 'caom2:Time.calibration', NULL, NULL, 'char', '64*', NULL, 0,0,0 , 88),

( 'caom2.Plane', 'polarization_states', 'polarization letter codes in canonical order, separated by /', 'caom2:Polarization.states', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 90),
( 'caom2.Plane', 'polarization_dimension', 'number of samples (pixels) along polarization axis', 'caom2:Polarization.dimension', NULL, NULL, 'int', NULL, NULL, 0,0,0 , 91),

( 'caom2.Plane', 'custom_ctype', 'coordinate type for custom axis', 'caom2:CustomAxis.ctype', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 100),
( 'caom2.Plane', 'custom_bounds', 'queryable custom axis coverage', 'caom2:CustomAxis.bounds', NULL, NULL, 'double', '2', 'interval', 0,0,0 , 101),
( 'caom2.Plane', 'custom_samples', 'detailed custom axis coverage', 'caom2:CustomAxis.samples', NULL, NULL, 'double', '*', 'multiinterval', 0,0,0 , 102),
( 'caom2.Plane', 'custom_dimension', 'dimension (number of pixels) along custom axis', 'caom2:CustomAxis.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 106),

( 'caom2.Plane', 'uv_distance', 'UV plane coverage', 'caom2:Visibility.distance', NULL, NULL, 'double', '2', 'interval', 0,0,0 , 110),
( 'caom2.Plane', 'uv_distributionEccentricity', 'TBD [new in 2.5]', 'caom2:Visibility.distributionEccentricity', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 111),
( 'caom2.Plane', 'uv_distributionFill', 'TBD [new in 2.5]', 'caom2:Visibility.distributionFill', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 112),

( 'caom2.Plane', 'planeID', 'unique plane identifier', 'caom2:Entity.id', NULL, NULL,    'char','36','uuid', 0,1,0 , 120),
( 'caom2.Plane', 'metaProducer', 'identifier for the producer of this entity metadata (URI of the form {organisation}:{software}-{version}) [new in 2.4]', 'caom2:Entity.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 121),
( 'caom2.Plane', 'lastModified', 'timestamp of last modification of this row', 'caom2:Entity.lastModified', NULL, NULL, 'char', '23*','timestamp', 1,1,0 , 122),
( 'caom2.Plane', 'maxLastModified', 'timestamp of last modification of this entity+children', 'caom2:CaomEntity.maxLastModified', NULL, NULL, 'char', '23*','timestamp', 1,1,0 , 123),
( 'caom2.Plane', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Entity.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 124),
( 'caom2.Plane', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form {algorithm}:{hex value})', 'caom2:CaomEntity.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 125)
;

-- Artifact
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,
    principal,indexed,std,column_index,column_id) values
( 'caom2.Artifact', 'uri', 'external URI for the physical artifact', 'caom2:Artifact.uri', NULL, NULL, 'char', '*','uri', 1,1,0 , 1, 'caomArtifactURI')
;
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Artifact', 'uriBucket', 'short hex string', 'caom2:Artifact.uriBucket', NULL, NULL, 'char','3',NULL, 0,1,0 , 2),
( 'caom2.Artifact', 'planeID',       'foreign key', NULL, NULL, NULL, 'char','36','uuid', 0,1,0 , 2),

( 'caom2.Artifact', 'productType',   'product type (science, calibration, auxiliary, preview, info)', 'caom2:Artifact.productType', NULL, NULL, 'char', '64*', NULL, 1,0,0 , 4),
( 'caom2.Artifact', 'releaseType',   'release type (data, meta)', 'caom2:Artifact.releaseType', NULL, NULL, 'char', '16*', NULL, 1,0,0 , 5),
( 'caom2.Artifact', 'contentType',   'content-type of the representation at uri', 'caom2:Artifact.contentType', NULL, NULL, 'char', '128*', NULL, 1,0,0 , 6),
( 'caom2.Artifact', 'contentLength', 'size of the representation at uri', 'caom2:Artifact.contentLength', NULL, 'byte', 'long', NULL, NULL, 1,0,0 , 7),
( 'caom2.Artifact', 'contentChecksum', 'checksum of the content (URI of the form {algorithm}:{hex value})', 'caom2:Artifact.contentChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 8),
( 'caom2.Artifact', 'contentRelease', 'date the data for an artifact is public (UTC) (default: inherit from Plane) [new in 2.4]', 'caom2:Artifact.contentRelease', NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 , 20),
( 'caom2.Artifact', 'contentReadGroups', 'GMS groups that are authorized to retrieve the artifact (default: inherit from Plane) [new in 2.4]', 'caom2:Artifact.contentReadGroups', NULL, NULL, 'char', '*',NULL, 0,0,0, 21),
( 'caom2.Artifact', 'descriptionID', 'reference to an ArtifactDescription [new in 2.5]', 'caom2:Artifact.descriptionID', NULL, NULL, 'char', '*','uri', 0,0,0, 22),

( 'caom2.Artifact', 'artifactID',   'primary key', 'caom2:Entity.id', NULL, NULL, 'char','36','uuid', 0,1,0 , 40),
( 'caom2.Artifact', 'metaProducer', 'identifier for the producer of this entity metadata (URI of the form {organisation}:{software}-{version}) [new in 2.4]', 'caom2:Entity.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 41),
( 'caom2.Artifact', 'lastModified',  'timestamp of last modification of this row', 'caom2:Entity.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0, 42),
( 'caom2.Artifact', 'maxLastModified',  'timestamp of last modification of this entity+children', 'caom2:CaomEntity.maxLastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0, 43),
( 'caom2.Artifact', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Entity.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 44),
( 'caom2.Artifact', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form {algorithm}:{hex value})', 'caom2:CaomEntity.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 45)
;

insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.ArtifactDescription', 'uri', 'logical identifier', 'caom2:ArtifactDescription.uri', NULL, NULL, 'char','*','uri', 0,1,0 , 1),
( 'caom2.ArtifactDescription', 'description', 'human-readable description of a class of artifacts', 'caom2:ArtifactDescription.description', NULL, NULL, 'char','*',NULL, 0,1,0 , 2),
( 'caom2.ArtifactDescription', 'id',   'primary key', 'caom2:Entity.id', NULL, NULL, 'char','36','uuid', 0,1,0 , 3),
( 'caom2.ArtifactDescription', 'metaProducer', 'identifier for the producer of this entity metadata (URI of the form {organisation}:{software}-{version}) [new in 2.4]', 'caom2:Entity.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 4),
( 'caom2.ArtifactDescription', 'lastModified',  'timestamp of last modification of this row', 'caom2:Entity.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0, 5),
( 'caom2.ArtifactDescription', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Entity.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 6)
;

insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.DeletedObservationEvent', 'uri', 'URI of the deleted Observation', 'caom2:DeletedObservationEvent.uri', NULL, NULL, 'char','*','uri', 0,1,0 , 1),
( 'caom2.DeletedObservationEvent', 'id', 'primary key == the uuid of the deleted entity', 'caom2:Entity.id', NULL, NULL, 'char','36','uuid', 0,1,0 , 2),
( 'caom2.DeletedObservationEvent', 'metaProducer', 'identifier for the producer of this entity; usually null in practice', 'caom2:Entity.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 3),
( 'caom2.DeletedObservationEvent', 'lastModified',  'timestamp of last modification of this row', 'caom2:Entity.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0, 4),
( 'caom2.DeletedObservationEvent', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Entity.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 5)
;

insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.DeletedArtifactDescriptionEvent', 'uri', 'URI of the deleted ArtifactDescription', 'caom2:DeletedObservationEvent.uri', NULL, NULL, 'char','*','uri', 0,1,0 , 1),
( 'caom2.DeletedArtifactDescriptionEvent', 'id', 'primary key == the uuid of the deleted entity', 'caom2:Entity.id', NULL, NULL, 'char','36','uuid', 0,1,0 , 2),
( 'caom2.DeletedArtifactDescriptionEvent', 'metaProducer', 'identifier for the producer of this entity; usually null in practice', 'caom2:Entity.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 3),
( 'caom2.DeletedArtifactDescriptionEvent', 'lastModified',  'timestamp of last modification of this row', 'caom2:Entity.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0, 4),
( 'caom2.DeletedArtifactDescriptionEvent', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Entity.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 5)
;

-- join tables
insert into tap_schema.columns11 (table_name,column_name,description,datatype,arraysize,xtype,principal,indexed,std) values
('caom2.ObservationMember', 'parentID', 'parent observation identifier [changed in 2.4]', 'char', '36','uuid', 0,1,0),
('caom2.ObservationMember', 'memberID', 'member observation identifier [changed in 2.4]', 'char', '*', 'uri', 0,1,0),

('caom2.ProvenanceInput', 'outputID', 'output plane identifier', 'char', '36','uuid', 0,1,0),
('caom2.ProvenanceInput', 'inputID', 'input plane identifier', 'char', '*', 'uri', 0,1,0)
;

insert into tap_schema.keys11 (key_id,from_table,target_table,description) values
('caom2-p-o', 'caom2.Plane', 'caom2.Observation','standard way to join caom2.Observation and caom2.Plane'),
('caom2-a-p', 'caom2.Artifact', 'caom2.Plane', 'standard way to join caom2.Plane and caom2.Artifact'),
('caom2-a-ad', 'caom2.Artifact', 'caom2.ArtifactDescription', 'standard way to join caom2.Artifact and caom2.ArtifactDescription'),

('caom2-composite-member', 'caom2.Observation', 'caom2.ObservationMember', 
    'standard way to join caom2.Observation (as parent aka DerivedObservation) and caom2.ObservationMember [join table]'),
('caom2-member-obs', 'caom2.ObservationMember', 'caom2.Observation',
    'standard way to join caom2.ObservationMember and caom2.Observation (as member) [join table]'),

('caom2-plane-prov', 'caom2.Plane', 'caom2.ProvenanceInput',
    'standard way to join caom2.Plane (product) and caom2.ProvenanceInput [join table]'),
('caom2-prov-input', 'caom2.ProvenanceInput', 'caom2.Plane',
    'standard way to join caom2.ProvenanceInput and caom2.Plane (input) [join table]')
;

insert into tap_schema.key_columns11 (key_id,from_column,target_column) values
('caom2-p-o', 'obsID', 'obsID'),
('caom2-a-p', 'planeID', 'planeID'),
('caom2-a-ad', 'descriptionID', 'uri'),

('caom2-composite-member', 'obsID', 'parentID' ),
('caom2-member-obs', 'memberID', 'uri'),

('caom2-plane-prov', 'planeID', 'outputID' ),
('caom2-prov-input', 'inputID', 'uri')
;

insert into tap_schema.tables11 (schema_name,table_name,table_type,description) values
('caom2', 'caom2.HarvestState', 'table', 'observation sync state'),
('caom2', 'caom2.HarvestSkip', 'table', 'list of observations to retry or artifacts to sync');

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.HarvestState', 'source',           'caom2:HarvestState.source',           'ID of the remote luskan (URI)', NULL, 'char','512*', 'uri', 1, 1, 1, 1 ),
( 'caom2.HarvestState', 'cname',            'caom2:HarvestState.cname',            'classname of the harvested entity', NULL, 'char','64*', NULL, 1, 1, 1, 2 ),
( 'caom2.HarvestState', 'curLastModified',  'caom2:HarvestState.curLastModified',  'lastModified timestamp of the last entity harvested', NULL, 'char','23*','timestamp', 1, 1, 1, 3 ),
( 'caom2.HarvestState', 'curID',            'caom2:HarvestState`.curID',           'id of the last entity harvested', NULL, 'char','36','uuid', 1, 1, 1, 4 ),
( 'caom2.HarvestState', 'lastModified',     'caom2:HarvestState.lastModified',     'timestamp of the event', NULL, 'char','23*','timestamp', 1, 1, 1, 5 ),
( 'caom2.HarvestState', 'stateID',          'caom2:HarvestState`.stateID',         'primary key', NULL, 'char','36','uuid', 1, 1, 1, 6 );

insert into tap_schema.columns11 (table_name,column_name,description,datatype,arraysize,xtype,principal,indexed,std) values
( 'caom2.HarvestSkip', 'source', 'harvesting source', 'char', '*', NULL, 1,1,1),
( 'caom2.HarvestSkip', 'cname', 'entity (class name)', 'char', '*', NULL, 1,1,1),
( 'caom2.HarvestSkip', 'bucket', 'random bucket code', 'char', '3', NULL, 1,1,1),
( 'caom2.HarvestSkip', 'skipID', 'URI of skipped entity instance', 'char', '*', 'uri', 1,1,1),
( 'caom2.HarvestSkip', 'tryAfter', 'timestamp for next (re)try', 'char', '23*', 'timestamp', 1,1,1),
( 'caom2.HarvestSkip', 'errorMessage', 'reason for harvest failure', 'char', '*', NULL, 1,0,1),
( 'caom2.HarvestSkip', 'lastModified', 'last modification of this entry', 'char', '23*', 'timestamp', 1,0,1),
( 'caom2.HarvestSkip', 'id', 'primary key', 'char', '36', 'uuid', 1,1,1);
