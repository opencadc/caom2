
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
('caom2', 'Common Archive Observation Model, version 2.4', 'caom2')
;

-- index start at 20
insert into tap_schema.tables11 (schema_name,table_name,table_type,description,utype,table_index) values
( 'caom2', 'caom2.Observation', 'table', 'the main CAOM Observation table', 'caom2:Observation' , 20),
( 'caom2', 'caom2.Plane', 'table', 'the products of the observation', 'caom2:Plane' , 21),
( 'caom2', 'caom2.Artifact', 'table', 'physical data artifacts (e.g. files)', 'caom2:Artifact' , 22),
( 'caom2', 'caom2.Part', 'table', 'parts of artifacts (e.g. FITS extensions)', 'caom2:Part' , 23),
( 'caom2', 'caom2.Chunk', 'table', 'description of the data array in a part', 'caom2:Chunk' , 24),
( 'caom2', 'caom2.ObservationMember', 'table', 'composite to simple observation join table', NULL, 25),
( 'caom2', 'caom2.ProvenanceInput', 'table', 'plane.provenance to input plane join table', NULL, 26)
;

-- Observation
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index,id) values
( 'caom2.Observation', 'observationURI', 'unique URI for this observation', 'caom2:Observation.uri', NULL, NULL, 'char', '*','uri', 1,1,1,1, 'caomObservationURI')
;

insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Observation', 'obsID', 'unique observation identifier', 'caom2:Observation.id', NULL, NULL, 'char','36','uuid', 0,1,0 , 2),
( 'caom2.Observation', 'collection', 'data collection this observation belongs to', 'caom2:Observation.collection', NULL, NULL, 'char','32*',NULL, 1,1,1 , 3),
( 'caom2.Observation', 'observationID', 'collection-specific unique observation identifier', 'caom2:Observation.observationID', NULL, NULL, 'char','128*',NULL, 1,1,1 , 4),
( 'caom2.Observation', 'algorithm_name', 'algorithm that defines the observation( exposure for simple, grouping algorithm for composites)', 'caom2:Observation.algorithm.name', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 5),

( 'caom2.Observation', 'type', 'type of data collected (e.g. FITS OBSTYPE header)', 'caom2:Observation.type', NULL, NULL, 'char', '32*', NULL, 1,0,1 , 6),
( 'caom2.Observation', 'intent', 'intended purpose of data (one of: science, calibration)', 'caom2:Observation.intent', NULL, NULL, 'char', '32*', NULL, 0,0,1, 7),
( 'caom2.Observation', 'sequenceNumber', 'sequence number assigned by the observatory', 'caom2:Observation.sequenceNumber', NULL, NULL, 'int', NULL, NULL, 0,0,0, 8),
( 'caom2.Observation', 'metaRelease', 'date the metadata for an observation is public (UTC)', 'caom2:Observation.metaRelease', NULL, NULL, 'char', '23*','timestamp', 0,0,0, 9),
( 'caom2.Observation', 'metaReadGroups', 'GMS groups that are authorized to see metadata', 'caom2:Observation.metaReadGroups', NULL, NULL, 'char', '*',NULL, 0,0,0, 10),

( 'caom2.Observation', 'proposal_id', 'collection-specific unique proposal identifier', 'caom2:Observation.proposal.id', NULL, NULL, 'char', '128*',NULL, 0,1,0 , 20),
( 'caom2.Observation', 'proposal_pi', 'proposal principal investigator', 'caom2:Observation.proposal.pi', NULL, NULL, 'char', '128*',NULL, 0,0,0 , 21),
( 'caom2.Observation', 'proposal_project', 'project that created/acquired the observation', 'caom2:Observation.proposal.project', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 22),
( 'caom2.Observation', 'proposal_title', 'proposal title', 'caom2:Observation.proposal.title', NULL, NULL, 'char', '256*',NULL, 0,0,0 , 23),
( 'caom2.Observation', 'proposal_keywords', 'proposal keywords (separated by |)', 'caom2:Observation.proposal.keywords', NULL, NULL, 'char', '*',NULL, 0,0,0 , 24),

( 'caom2.Observation', 'target_name', 'name of intended target', 'caom2:Observation.target.name', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 30),
( 'caom2.Observation', 'target_targetID', 'identifier of intended target [new in 2.4]', 'caom2:Observation.target.targetID', NULL, NULL, 'char', '32*','uri', 1,0,0 , 31),
( 'caom2.Observation', 'target_type', 'classification of intended target', 'caom2:Observation.target.type', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 32),
( 'caom2.Observation', 'target_standard', 'intended target is a standard (0=false, 1=true)', 'caom2:Observation.target.standard', NULL, NULL, 'int', NULL,NULL, 0,0,0 , 33),
( 'caom2.Observation', 'target_redshift', 'redshift of intended target', 'caom2:Observation.target.redshift', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 34),
( 'caom2.Observation', 'target_moving', 'flag for moving target', 'caom2:Observation.target.moving', NULL, NULL, 'int', NULL,NULL, 0,0,0 , 35),
( 'caom2.Observation', 'target_keywords', 'target keywords (separated by |)', 'caom2:Observation.target.keywords', NULL, NULL, 'char', '*',NULL, 0,0,0 , 36),

( 'caom2.Observation', 'targetPosition_coordsys', 'coordinate system for target position', 'caom2:Observation.targetPosition.coordsys', NULL, NULL, 'char', '16*',NULL, 0,0,0 , 40),
( 'caom2.Observation', 'targetPosition_equinox', 'equinox of target position coordinate system', 'caom2:Observation.targetPosition.equinox', NULL, NULL, 'double', NULL,NULL, 0,0,0 , 41),
( 'caom2.Observation', 'targetPosition_coordinates_cval1', 'longitude of target position', 'caom2:Observation.targetPosition.coordinates.cval1', NULL, 'deg', 'double', NULL,NULL, 0,0,0 , 41),
( 'caom2.Observation', 'targetPosition_coordinates_cval2', 'latitude of target position', 'caom2:Observation.targetPosition.coordinates.cval2', NULL, 'deg', 'double', NULL,NULL, 0,0,0 , 43),

( 'caom2.Observation', 'telescope_name', 'name of telescope used to acquire observation', 'caom2:Observation.telescope.name', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 45),
( 'caom2.Observation', 'telescope_geoLocationX', 'x component of geocentric location of telescope', 'caom2:Observation.telescope.geoLocationX', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 46),
( 'caom2.Observation', 'telescope_geoLocationY', 'y component of geocentric location of telescope', 'caom2:Observation.telescope.geoLocationY', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 47),
( 'caom2.Observation', 'telescope_geoLocationZ', 'z component of geocentric location of telescope', 'caom2:Observation.telescope.geoLocationZ', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 48),
( 'caom2.Observation', 'telescope_keywords', 'telescope keywords (separated by |)', 'caom2:Observation.telescope.keywords', NULL, NULL, 'char', '*', NULL, 0,0,0 , 49),

( 'caom2.Observation', 'requirements_flag', 'flag describing satisfied proposal requirements (possible values: fail)', 'caom2:Observation.requirements.flag', NULL, NULL, 'char', '16',NULL, 1,0,0 , 50),

( 'caom2.Observation', 'instrument_name', 'name of instrument used to acquire observation', 'caom2:Observation.instrument.name', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 60),
( 'caom2.Observation', 'instrument_keywords', 'instrument keywords (separated by |)', 'caom2:Observation.instrument.keywords', NULL, NULL, 'char', '*',NULL, 0,0,0 , 61),

( 'caom2.Observation', 'environment_seeing', 'atmospheric seeing (FWHM)', 'caom2:Observation.environment.seeing', NULL, 'arcsec', 'double', NULL, NULL, 0,0,0 , 70),
( 'caom2.Observation', 'environment_humidity', 'humidity at telescope during observation', 'caom2:Observation.environment.humidity', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 71),
( 'caom2.Observation', 'environment_elevation', 'elevation above horizon (0 to 90)', 'caom2:Observation.environment.elevation', NULL, 'deg', 'double', NULL, NULL, 0,0,0 , 72),
( 'caom2.Observation', 'environment_tau', 'zenith optical depth', 'caom2:Observation.environment.tau', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 72),
( 'caom2.Observation', 'environment_wavelengthTau', 'wavelength at which Tau has been measured', 'caom2:Observation.environment.wavelengthTau', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 74),
( 'caom2.Observation', 'environment_ambientTemp', 'ambient temperature telescope during observation', 'caom2:Observation.environment.ambientTemp', NULL, 'C', 'double', NULL, NULL, 0,0,0 , 75),
( 'caom2.Observation', 'environment_photometric', 'conditions were photometric (0=false, 1=true)', 'caom2:Observation.environment.photometric', NULL, NULL, 'int', NULL, NULL, 0,0,0 , 76),

( 'caom2.Observation', 'members', 'members of a composite observation (space-separated list of Observation URIs)', 'caom2:Observation.members', NULL, NULL, 'char','*',NULL, 0,0,0 , 80),
( 'caom2.Observation', 'typeCode', 'single character code to denote type: S(impleObservation) or C(ompositeOvservation)', 'caom2:Observation.typeCode', NULL, NULL, 'char',NULL,NULL, 0,0,0 , 81),

( 'caom2.Observation', 'metaProducer', 'identifier for the producer of this entity metadata (URI of the form {organisation}:{software}-{version}) [new in 2.4]', 'caom2:Observation.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 120),
( 'caom2.Observation', 'lastModified', 'timestamp of last modification of this row', 'caom2:Observation.lastModified', NULL, NULL, 'char', '23*','timestamp', 1,0,0 , 121),
( 'caom2.Observation', 'maxLastModified', 'timestamp of last modification of this entity+children', 'caom2:Observation.maxLastModified', NULL, NULL, 'char', '23*','timestamp', 1,0,0 , 122),
( 'caom2.Observation', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Observation.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 123),
( 'caom2.Observation', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form {algorithm}:{hex value})', 'caom2:Observation.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 124)
;

-- Plane
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index,id) values
( 'caom2.Plane', 'publisherID', 'unique publisher identifier for this product', 'caom2:Plane.publisherID', NULL, NULL, 'char', '*', 'uri', 1,1,1,1, 'caomPublisherID')
;
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Plane', 'obsID', 'foreign key', NULL, NULL, NULL,                              'char','36','uuid', 0,1,0 , 2),
( 'caom2.Plane', 'planeID', 'unique plane identifier', 'caom2:Plane.id', NULL, NULL,    'char','36','uuid', 0,1,0 , 3),
( 'caom2.Plane', 'planeURI', 'unique internal URI for this product', 'caom2:Plane.uri', NULL, NULL, 'char', '*', 'uri', 1,1,1 , 4),
( 'caom2.Plane', 'creatorID', 'unique creator identifier for this product', 'caom2:Plane.creatorID', NULL, NULL, 'char', '*', 'uri', 1,1,1 , 5),
( 'caom2.Plane', 'productID', 'name of this product', 'caom2:Plane.productID', NULL, NULL, 'char', '64*', NULL, 1,1,1 , 6),
( 'caom2.Plane', 'metaRelease', 'date the metadata for a plane is public (UTC)', 'caom2:Plane.metaRelease', NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 , 7),
( 'caom2.Plane', 'metaReadGroups', 'GMS groups that are authorized to see metadata [new in 2.4]', 'caom2:Plane.metaReadGroups', NULL, NULL, 'char', '*',NULL, 0,0,0, 8),
( 'caom2.Plane', 'dataRelease', 'date the data for a plane is public (UTC)', 'caom2:Plane.dataRelease', NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 , 9),
( 'caom2.Plane', 'dataReadGroups', 'GMS groups that are authorized to see data [new in 2.4]', 'caom2:Plane.dataReadGroups', NULL, NULL, 'char', '*',NULL, 0,0,0, 10),
( 'caom2.Plane', 'dataProductType', 'IVOA ObsCore data product type + extensions', 'caom2:Plane.dataProductType', NULL, NULL, 'char', '128*', NULL, 1,0,1 , 11),
( 'caom2.Plane', 'calibrationLevel', 'IVOA ObsCore calibration level + extensions (-1,0,1,2,3,4)', 'caom2:Plane.calibrationLevel', NULL, NULL, 'int', NULL, NULL, 1,0,1 , 12),

( 'caom2.Plane', 'provenance_name', 'name of the process that created this plane', 'caom2:Plane.provenance.name', NULL, NULL, 'char', '128*', NULL, 0,0,1 , 20),
( 'caom2.Plane', 'provenance_version', 'version of the process/software', 'caom2:Plane.provenance.version', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 21),
( 'caom2.Plane', 'provenance_reference', 'external reference (URL)', 'caom2:Plane.provenance.reference', NULL, NULL, 'char', '256*', NULL, 0,0,0 , 22),
( 'caom2.Plane', 'provenance_producer', 'responsible entity (e.g. person)', 'caom2:Plane.provenance.producer', NULL, NULL, 'char', '128*', NULL, 0,0,0, 23),
( 'caom2.Plane', 'provenance_project', 'responsible entity (e.g. person)', 'caom2:Plane.provenance.project', NULL, NULL, 'char', '256*', NULL, 0,0,0, 24),
( 'caom2.Plane', 'provenance_runID', 'responsible entity (e.g. person)', 'caom2:Plane.provenance.runID', NULL, NULL, 'char', '64*', NULL, 0,1,0, 25),
( 'caom2.Plane', 'provenance_lastExecuted', 'date this process was last executed', 'caom2:Plane.provenance.lastExecuted', NULL, NULL, 'char', '23*', 'timestamp', 0,0,0 , 26),
( 'caom2.Plane', 'provenance_keywords', 'provenance keywords (separated by |)', 'caom2:Plane.provenance.keywords', NULL, NULL, 'char', '*', NULL, 0,0,0 , 27),
( 'caom2.Plane', 'provenance_inputs', 'inputs of the process that created this plane (space-separated list of Plane URIs)', 'caom2:Plane.provenance.inputs', NULL, NULL, 'char','*','clob', 0,0,0 , 28),

( 'caom2.Plane', 'metrics_sourceNumberDensity', 'number density of sources', 'caom2:Plane.metrics.sourceNumberDensity', NULL, 'deg-2', 'double', NULL, NULL, 0,0,0 , 30),
( 'caom2.Plane', 'metrics_background', 'background intensity', 'caom2:Plane.metrics.background', NULL, 'Jy/pix', 'double', NULL,NULL, 0,0,0 , 31),
( 'caom2.Plane', 'metrics_backgroundStddev', 'standard deviation in background', 'caom2:Plane.metrics.backgroundStddev', NULL, 'Jy/pix', 'double', NULL, NULL, 0,0,0 , 32),
( 'caom2.Plane', 'metrics_fluxDensityLimit', 'flux density limit where S:N=5 for point source', 'caom2:Plane.metrics.fluxDensityLimit', NULL, 'Jy', 'double', NULL, NULL, 0,0,0 , 33),
( 'caom2.Plane', 'metrics_magLimit', 'AB magnitude limit where S:N=5 for point source', 'caom2:Plane.metrics.magLimit', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 34),

( 'caom2.Plane', 'observable_ucd', 'UCD (Universal Content Descriptor) for the observed/measure quantity', 'caom2:Plane.observable.ucd', NULL, NULL, 'char', '64*', NULL, 0,0,0 , 40),
( 'caom2.Plane', 'quality_flag', 'flag describing the data quality (possible values: junk)', 'caom2:Plane.quality.flag', NULL, NULL, 'char', '16*', NULL, 0,0,0 , 41),

( 'caom2.Plane', 'position_bounds', 'positional coverage of the data', 'caom2:Plane.position.bounds', NULL, 'deg', 'char', '*', 'shape', 0,0,0 , 50),
( 'caom2.Plane', 'position_bounds_samples', 'positional coverage of the data', 'caom2:Plane.position.bounds.samples', NULL, 'deg', 'double', '*', 'multipolygon', 0,0,0 , 50),
( 'caom2.Plane', 'position_bounds_size', 'size of the polygon bounds (diameter of minimum spanning circle)', 'caom2:Plane.position.bounds.size', NULL, 'deg', 'double', NULL, NULL, 0,0,0 , 51),
( 'caom2.Plane', 'position_resolution', 'median spatial resolution (FWHM)', 'caom2:Plane.position.resolution', NULL, 'arcsec', 'double', NULL, NULL, 0,0,0 , 52),
( 'caom2.Plane', 'position_resolutionBounds', 'range of spatial resolution (FWHM) [new in 2.4]', 'caom2:Plane.position.resolutionBounds', NULL, 'arcsec', 'double', '2', 'interval', 0,0,0 , 52),
( 'caom2.Plane', 'position_sampleSize', 'median sample (pixel) size on spatial axes', 'caom2:Plane.position.sampleSize', NULL, 'arcsec', 'double', NULL, NULL, 0,0,0 , 53),
( 'caom2.Plane', 'position_dimension_naxis1', 'dimensions (number of pixels) along one spatial axis', 'caom2:Plane.position.dimension.naxis1', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 54),
( 'caom2.Plane', 'position_dimension_naxis2', 'dimensions (number of pixels) along other spatial axis', 'caom2:Plane.position.dimension.naxis2', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 55),
( 'caom2.Plane', 'position_timeDependent', 'flag indicating that the position is time-dependent (0=false, 1=true)', 'caom2:Plane.position.timeDependent', NULL, NULL, 'int', NULL, NULL, 0,0,0 , 56),

( 'caom2.Plane', 'energy_bounds', 'energy coverage (barycentric wavelength)', 'caom2:Plane.energy.bounds', NULL, 'm', 'double', '2','interval', 0,1,0 , 61),
( 'caom2.Plane', 'energy_bounds_samples', 'detailed energy coverage (barycentric wavelength)', 'caom2:Plane.energy.bounds.samples', NULL, 'm', 'double', '2x*', 'interval', 0,0,0 , 61),
( 'caom2.Plane', 'energy_bounds_lower', 'lower bound on energy axis (barycentric wavelength)', 'caom2:Plane.energy.bounds.lower', NULL, 'm', 'double', NULL, NULL, 0,1,0 , 62),
( 'caom2.Plane', 'energy_bounds_upper', 'upper bound on energy axis (barycentric wavelength)', 'caom2:Plane.energy.bounds.upper', NULL, 'm', 'double', NULL, NULL, 0,1,0 , 63),
( 'caom2.Plane', 'energy_bounds_width', 'width of the energy bounds', 'caom2:Plane.energy.bounds.width', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 64),
( 'caom2.Plane', 'energy_dimension', 'dimension (number of pixels) along energy axis', 'caom2:Plane.energy.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 65),
( 'caom2.Plane', 'energy_resolvingPower', 'median spectral resolving power (R)', 'caom2:Plane.energy.resolvingPower', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 66),
( 'caom2.Plane', 'energy_resolvingPowerBounds', 'range of spectral resolving power (R) [new in 2.4]', 'caom2:Plane.energy.resolvingPowerBounds', NULL, NULL, 'double', '2', 'interval', 0,0,0 , 67),
( 'caom2.Plane', 'energy_sampleSize', 'median sample (pixel) size on energy axis', 'caom2:Plane.energy.sampleSize', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 68),
-- cardinality and name change: temporarily allow old name
( 'caom2.Plane', 'energy_emBand', 'energy band names (Radio,Millimeter,Infrared,Optical,UV,EUV,X-ray,Gamma-ray), separated by | [deprecated]', 'caom2:Plane.energy.emBand', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 69),
( 'caom2.Plane', 'energy_energyBands', 'energy band names (Radio,Millimeter,Infrared,Optical,UV,EUV,X-ray,Gamma-ray), separated by | [new in 2.4]', 'caom2:Plane.energy.energyBands', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 69),

( 'caom2.Plane', 'energy_bandpassName', 'collection-specific name for energy band (e.g. filter name)', 'caom2:Plane.energy.bandpassName', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 70),
( 'caom2.Plane', 'energy_transition_species', 'atom or molecule', 'caom2:Plane.energy.transition.species', NULL, NULL, 'char', '32*',NULL, 0,0,0 , 71),
( 'caom2.Plane', 'energy_transition_transition', 'specific energy transition of species', 'caom2:Plane.energy.transition.transition', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 72),
( 'caom2.Plane', 'energy_freqWidth', 'width of the energy bounds (convenience: in frequency)', 'caom2:Plane.energy.freqWidth', NULL, 'Hz', 'double', NULL, NULL, 0,0,0 , 73),
( 'caom2.Plane', 'energy_freqSampleSize', 'median sample (pixel) size on energy axis (convenience: in frequency)', 'caom2:Plane.energy.freqSampleSize', NULL, 'Hz', 'double', NULL, NULL, 0,0,0 , 74),
( 'caom2.Plane', 'energy_restwav', 'rest wavelength of target spectral feature (barycentric)', 'caom2:Plane.energy.restwav', NULL, 'm', 'double', NULL, NULL, 0,1,0 , 75),

( 'caom2.Plane', 'time_bounds', 'time coverage (Modified Julian Day)', 'caom2:Plane.time.bounds', NULL, 'd', 'double', '2', 'interval', 0,1,0 , 80),
( 'caom2.Plane', 'time_bounds_samples', 'detailed time coverage (Modified Julian Day)', 'caom2:Plane.time.bounds.samples', NULL, 'd', 'double', '2x*', 'interval', 0,0,0 , 81),
( 'caom2.Plane', 'time_bounds_lower', 'lower bound on time axis (Modified Julian Day)', 'caom2:Plane.time.bounds.lower', NULL, 'd', 'double', NULL, NULL, 0,1,0 , 81),
( 'caom2.Plane', 'time_bounds_upper', 'upper bound on time axis (Modified Julian Day)', 'caom2:Plane.time.bounds.upper', NULL, 'd', 'double', NULL, NULL, 0,1,0 , 82),
( 'caom2.Plane', 'time_bounds_width', 'width of the time bounds', 'caom2:Plane.time.bounds.width', NULL, 'd', 'double', NULL, NULL, 0,1,0 , 83),
( 'caom2.Plane', 'time_dimension', 'dimension (number of pixels) along time axis', 'caom2:Plane.time.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 84),
( 'caom2.Plane', 'time_resolution', 'median resolution on the time axis', 'caom2:Plane.time.resolution', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 85),
( 'caom2.Plane', 'time_resolutionBounds', 'range of resolution on the time axis [new in 2.4]', 'caom2:Plane.time.resolutionBounds', NULL, 'd', 'double', '2', 'interval', 0,0,0 , 86),
( 'caom2.Plane', 'time_sampleSize', 'median sample (pixel) size on time axis', 'caom2:Plane.time.sampleSize', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 87),
( 'caom2.Plane', 'time_exposure', 'median exposure time per pixel', 'caom2:Plane.time.exposure', NULL, 's', 'double', NULL, NULL, 0,0,0 , 88),

( 'caom2.Plane', 'polarization_states', 'polarization letter codes in canonical order, separated by /', 'caom2:Plane.polarization.states', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 90),
( 'caom2.Plane', 'polarization_dimension', 'number of samples (pixels) along polarization axis', 'caom2:Plane.polarization.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 91),

( 'caom2.Plane', 'custom_ctype', 'coordinate type for custom axis [new in 2.4]', 'caom2:Plane.custom.ctype', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 100),
( 'caom2.Plane', 'custom_bounds', 'custom axis coverage [new in 2.4]', 'caom2:Plane.custom.bounds', NULL, NULL, 'double', '2', 'interval', 0,0,0 , 101),
( 'caom2.Plane', 'custom_bounds_samples', 'detailed custom axis coverage [new in 2.4]', 'caom2:Plane.custom.bounds.samples', NULL, NULL, 'double', '2x*', 'interval', 0,0,0 , 102),
( 'caom2.Plane', 'custom_bounds_lower', 'lower bound on custom axis [new in 2.4]', 'caom2:Plane.custom.bounds.lower', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 103),
( 'caom2.Plane', 'custom_bounds_upper', 'upper bound on custom axis [new in 2.4]', 'caom2:Plane.custom.bounds.upper', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 104),
( 'caom2.Plane', 'custom_bounds_width', 'width of the custom bounds [new in 2.4]', 'caom2:Plane.custom.bounds.width', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 105),
( 'caom2.Plane', 'custom_dimension', 'dimension (number of pixels) along custom axis [new in 2.4]', 'caom2:Plane.custom.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 106),

( 'caom2.Plane', 'metaProducer', 'identifier for the producer of this entity metadata (URI of the form {organisation}:{software}-{version}) [new in 2.4]', 'caom2:Plane.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 120),
( 'caom2.Plane', 'lastModified', 'timestamp of last modification of this row', 'caom2:Plane.lastModified', NULL, NULL, 'char', '23*','timestamp', 1,1,0 , 121),
( 'caom2.Plane', 'maxLastModified', 'timestamp of last modification of this entity+children', 'caom2:Plane.maxLastModified', NULL, NULL, 'char', '23*','timestamp', 1,1,0 , 122),
( 'caom2.Plane', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Plane.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 123),
( 'caom2.Plane', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form {algorithm}:{hex value})', 'caom2:Plane.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 124)
;

-- Artifact
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,
    principal,indexed,std,column_index,id) values
( 'caom2.Artifact', 'uri',           'external URI for the physical artifact', 'caom2:Artifact.uri', NULL, NULL, 'char', '*','uri', 
    1,1,0 , 1, 'caomArtifactURI')
;
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Artifact', 'planeID',       'foreign key', NULL, NULL, NULL, 'char','36','uuid', 0,1,0 , 2),
( 'caom2.Artifact', 'artifactID',    'unique artifact identifier', 'caom2:Artifact.id', NULL, NULL, 'char','36','uuid', 0,1,0 , 3),
( 'caom2.Artifact', 'productType',   'product type (science, calibration, auxiliary, preview, info)', 'caom2:Artifact.productType', NULL, NULL, 'char', '32*', NULL, 1,0,0 , 4),
( 'caom2.Artifact', 'releaseType',   'release type (data, meta)', 'caom2:Artifact.releaseType', NULL, NULL, 'char', '16*', NULL, 1,0,0 , 5),
( 'caom2.Artifact', 'contentType',   'content-type of the representation at uri', 'caom2:Artifact.contentType', NULL, NULL, 'char', '128*', NULL, 1,0,0 , 6),
( 'caom2.Artifact', 'contentLength', 'size of the representation at uri', 'caom2:Artifact.contentLength', NULL, 'byte', 'long', NULL, NULL, 1,0,0 , 7),
( 'caom2.Artifact', 'contentChecksum', 'checksum of the content (URI of the form {algorithm}:{hex value})', 'caom2:Artifact.contentChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 8),
( 'caom2.Artifact', 'contentRelease', 'date the data for an artifact is public (UTC) (default: inherit from Plane) [new in 2.4]', 'caom2:Artifact.contentRelease', NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 , 20),
( 'caom2.Artifact', 'contentReadGroups', 'GMS groups that are authorized to retrieve the artifact (default: inherit from Plane) [new in 2.4]', 'caom2:Artifact.contentReadGroups', NULL, NULL, 'char', '*',NULL, 0,0,0, 21),

( 'caom2.Artifact', 'metaProducer', 'identifier for the producer of this entity metadata (URI of the form {organisation}:{software}-{version}) [new in 2.4]', 'caom2:Artifact.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 40),
( 'caom2.Artifact', 'lastModified',  'timestamp of last modification of this row', 'caom2:Artifact.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0, 41),
( 'caom2.Artifact', 'maxLastModified',  'timestamp of last modification of this entity+children', 'caom2:Artifact.maxLastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0, 42),
( 'caom2.Artifact', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Artifact.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 43),
( 'caom2.Artifact', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form {algorithm}:{hex value})', 'caom2:Artifact.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 44)
;

-- Part
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Part', 'artifactID',    'foreign key', NULL, NULL, NULL, 'char', '36','uuid', 0,1,0 , 1),
( 'caom2.Part', 'partID',        'unique part identifier', 'caom2:Part.id', NULL, NULL, 'char', '36','uuid', 0,1,0 , 2),

( 'caom2.Part', 'name',          'name of this part', 'caom2:Part.name', NULL, NULL, 'char', '128*', NULL, 1,0,0 , 3),
( 'caom2.Part', 'productType',   'product type (science, calibration, auxiliary, preview, info)', 'caom2:Part.productType', NULL, NULL, 'char', '32*', NULL, 1,0,0 , 4),

( 'caom2.Part', 'metaProducer', 'identifier for the producer of this entity metadata (URI of the form {organisation}:{software}-{version}) [new in 2.4]', 'caom2:Part.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 10),
( 'caom2.Part', 'lastModified',  'timestamp of last modification of this row', 'caom2:Part.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0 , 20),
( 'caom2.Part', 'maxLastModified',  'timestamp of last modification of this entity+children', 'caom2:Part.maxLastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0 , 21),
( 'caom2.Part', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Part.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 22),
( 'caom2.Part', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form {algorithm}:{hex value})', 'caom2:Part.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 23)
;

-- Chunk
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Chunk', 'partID',       'parent identifier', NULL, NULL, NULL, 'char', '36','uuid', 0,1,0 , 1),
( 'caom2.Chunk', 'chunkID',    'unique chunk identifier', 'caom2:Chunk.id', NULL, NULL, 'char', '36','uuid', 0,1,0 , 2),

( 'caom2.Chunk', 'productType',   'deprecated', 'caom2:Chunk.productType', NULL, NULL, 'char', '32*', NULL, 1,0,0 , 999),

( 'caom2.Chunk', 'naxis', 'number of data axes', 'caom2:Chunk.naxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 10),
( 'caom2.Chunk', 'positionAxis1', 'axis for the first position coordinate', 'caom2:Chunk.positionAxis1', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 11),
( 'caom2.Chunk', 'positionAxis2', 'axis for the second position coordinate', 'caom2:Chunk.positionAxis2', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 12),
( 'caom2.Chunk', 'energyAxis', 'axis for the energy coordinate', 'caom2:Chunk.energyAxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 13),
( 'caom2.Chunk', 'timeAxis', 'axis for the time coordinate', 'caom2:Chunk.timeAxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 14),
( 'caom2.Chunk', 'polarizationAxis', 'axis for the polarization coordinate', 'caom2:Chunk.polarizationAxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 15),
( 'caom2.Chunk', 'customAxis', 'axis for a custom coordinate [new in 2.4]', 'caom2:Chunk.customAxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 16),
( 'caom2.Chunk', 'observableAxis', 'axis for the observable quantity', 'caom2:Chunk.observableAxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 17),

( 'caom2.Chunk', 'position_coordsys', 'coordinate system name', 'caom2:Chunk.position.coordsys', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 20),
( 'caom2.Chunk', 'position_equinox', 'coordinate equinox', 'caom2:Chunk.position.equinox', NULL, 'a', 'double', NULL, NULL, 1,0,0 , NULL),
( 'caom2.Chunk', 'position_resolution', 'median spatial resolution (FWHM) ', 'caom2:Chunk.position.resolution', NULL, 'arcsec', 'double', NULL, NULL, 1,0,0 , 21),
( 'caom2.Chunk', 'position_axis_axis1_ctype', 'coordinate type for first position axis', 'caom2:Chunk.position.axis.axis1.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 22),
( 'caom2.Chunk', 'position_axis_axis1_cunit', 'coordinate unit for first position axis', 'caom2:Chunk.position.axis.axis1.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 23),
( 'caom2.Chunk', 'position_axis_error1_syser', 'systematic coordinate error for first position axis', 'caom2:Chunk.position.axis.error1.syser', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 24),
( 'caom2.Chunk', 'position_axis_error1_rnder', 'random coordinate error for first position axis', 'caom2:Chunk.position.axis.error1.rnder', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 25),
( 'caom2.Chunk', 'position_axis_axis2_ctype', 'coordinate type for second position axis', 'caom2:Chunk.position.axis.axis2.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 26),
( 'caom2.Chunk', 'position_axis_axis2_cunit', 'coordinate unit for second position axis', 'caom2:Chunk.position.axis.axis2.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 27),
( 'caom2.Chunk', 'position_axis_error2_syser', 'systematic coordinate error for second position axis', 'caom2:Chunk.position.axis.error2.syser', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 28),
( 'caom2.Chunk', 'position_axis_error2_rnder', 'random coordinate error for second position axis', 'caom2:Chunk.position.axis.error2.rnder', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 29),
( 'caom2.Chunk', 'position_axis_range_start_coord1_pix', 'coordinate range for position axis', 'caom2:Chunk.position.axis.range.start.coord1.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 30),
( 'caom2.Chunk', 'position_axis_range_start_coord1_val', 'coordinate range for position axis', 'caom2:Chunk.position.axis.range.start.coord1.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 31),
( 'caom2.Chunk', 'position_axis_range_start_coord2_pix', 'coordinate range for position axis', 'caom2:Chunk.position.axis.range.start.coord2.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 32),
( 'caom2.Chunk', 'position_axis_range_start_coord2_val', 'coordinate range for position axis', 'caom2:Chunk.position.axis.range.start.coord2.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 33),
( 'caom2.Chunk', 'position_axis_range_end_coord1_pix', 'coordinate range for position axis', 'caom2:Chunk.position.axis.range.end.coord1.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 34),
( 'caom2.Chunk', 'position_axis_range_end_coord1_val', 'coordinate range for position axis', 'caom2:Chunk.position.axis.range.end.coord1.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 35),
( 'caom2.Chunk', 'position_axis_range_end_coord2_pix', 'coordinate range for position axis', 'caom2:Chunk.position.axis.range.end.coord2.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 36),
( 'caom2.Chunk', 'position_axis_range_end_coord2_val', 'coordinate range for position axis', 'caom2:Chunk.position.axis.range.end.coord2.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 37),
( 'caom2.Chunk', 'position_axis_bounds', 'coordinate bounds for position axis (string-encoded)', 'caom2:Chunk.position.axis.bounds', NULL, NULL, 'char', NULL, NULL, 1,0,0 , 38),
( 'caom2.Chunk', 'position_axis_function_dimension_naxis1', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.dimension.naxis1', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 39),
( 'caom2.Chunk', 'position_axis_function_dimension_naxis2', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.dimension.naxis2', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 40),
( 'caom2.Chunk', 'position_axis_function_refCoord_coord1_pix', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.refCoord.coord1.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 41),
( 'caom2.Chunk', 'position_axis_function_refCoord_coord1_val', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.refCoord.coord1.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 42),
( 'caom2.Chunk', 'position_axis_function_refCoord_coord2_pix', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.refCoord.coord2.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 43),
( 'caom2.Chunk', 'position_axis_function_refCoord_coord2_val', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.refCoord.coord2.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 44),
( 'caom2.Chunk', 'position_axis_function_cd11', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.cd11', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 45), 
( 'caom2.Chunk', 'position_axis_function_cd12', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.cd12', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 46),
( 'caom2.Chunk', 'position_axis_function_cd21', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.cd21', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 47),
( 'caom2.Chunk', 'position_axis_function_cd22', 'coordinate function for position axis', 'caom2:Chunk.position.axis.function.cd22', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 48),

( 'caom2.Chunk', 'energy_axis_axis_ctype', 'coordinate type for energy axis', 'caom2:Chunk.energy.axis.axis.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 60),
( 'caom2.Chunk', 'energy_axis_axis_cunit', 'coordinate unit for energy axis', 'caom2:Chunk.energy.axis.axis.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 61),
( 'caom2.Chunk', 'energy_axis_error_syser', 'systematic coordinate error for energy axis', 'caom2:Chunk.energy.axis.error.syser', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 62),
( 'caom2.Chunk', 'energy_axis_error_rnder', 'random coordinate error for energy axis', 'caom2:Chunk.energy.axis.error.rnder', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 63),
( 'caom2.Chunk', 'energy_axis_range_start_pix', 'coordinate range for energy axis', 'caom2:Chunk.energy.axis.range.start.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 64),
( 'caom2.Chunk', 'energy_axis_range_start_val', 'coordinate range for energy axis', 'caom2:Chunk.energy.axis.range.start.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 65),
( 'caom2.Chunk', 'energy_axis_range_end_pix', 'coordinate range for energy axis', 'caom2:Chunk.energy.axis.range.end.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 66),
( 'caom2.Chunk', 'energy_axis_range_end_val', 'coordinate range for energy axis', 'caom2:Chunk.energy.axis.range.end.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 67),
( 'caom2.Chunk', 'energy_axis_bounds', 'coordinate bounds for energy axis (string-encoded)', 'caom2:Chunk.energy.axis.bounds', NULL, NULL, 'char', NULL, NULL, 1,0,0 , 68),
( 'caom2.Chunk', 'energy_axis_function_naxis', 'coordinate function for energy axis', 'caom2:Chunk.energy.axis.function.naxis', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 69),
( 'caom2.Chunk', 'energy_axis_function_refCoord_pix', 'coordinate function for energy axis', 'caom2:Chunk.energy.axis.function.refCoord.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 70),
( 'caom2.Chunk', 'energy_axis_function_refCoord_val', 'coordinate function for energy axis', 'caom2:Chunk.energy.axis.function.refCoord.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 71),
( 'caom2.Chunk', 'energy_axis_function_delta', 'coordinate function for energy axis', 'caom2:Chunk.energy.axis.function.delta', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 72),
( 'caom2.Chunk', 'energy_specsys', 'SPECSYS', 'caom2:Chunk.energy.specsys', NULL, NULL, 'char', '8*',NULL, 1,0,0 , 73),
( 'caom2.Chunk', 'energy_ssysobs', 'SSYSOBS', 'caom2:Chunk.energy.ssysobs', NULL, NULL, 'char', '8*',NULL, 1,0,0 , 74),
( 'caom2.Chunk', 'energy_ssyssrc', 'SSYSSRC', 'caom2:Chunk.energy.ssyssrc', NULL, NULL, 'char', '8*',NULL, 1,0,0 , 75),
( 'caom2.Chunk', 'energy_restfrq', 'RESTFRQ', 'caom2:Chunk.energy.restfrq', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 76),
( 'caom2.Chunk', 'energy_restwav', 'RESTWAV', 'caom2:Chunk.energy.restwav', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 77),
( 'caom2.Chunk', 'energy_velosys', 'VELOSYS', 'caom2:Chunk.energy.velosys', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 78),
( 'caom2.Chunk', 'energy_zsource', 'ZSOURCE', 'caom2:Chunk.energy.zsource', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 79),
( 'caom2.Chunk', 'energy_velang', 'VELANG', 'caom2:Chunk.energy.velang', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 80),
( 'caom2.Chunk', 'energy_bandpassName', 'observatory name for the band (e.g. filter name)', 'caom2:Chunk.energy.bandpassName', NULL, NULL, 'char', '64*', NULL, 1,0,0 , 81),
( 'caom2.Chunk', 'energy_resolvingPower', 'resolving power R', 'caom2:Chunk.energy.resolvingPower', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 82),
( 'caom2.Chunk', 'energy_transition_species', 'target element/molecule species', 'caom2:Chunk.energy.transition.species', NULL, NULL, 'char', '128*', NULL, 1,0,0 , 83),
( 'caom2.Chunk', 'energy_transition_transition', 'target energy transition', 'caom2:Chunk.energy.transition.transition', NULL, NULL, 'char', '128*', NULL, 1,0,0 , 84),

( 'caom2.Chunk', 'time_axis_axis_ctype', 'coordinate type for time axis', 'caom2:Chunk.time.axis.axis.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 100),
( 'caom2.Chunk', 'time_axis_axis_cunit', 'coordinate unit for time axis', 'caom2:Chunk.time.axis.axis.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , NULL),
( 'caom2.Chunk', 'time_axis_error_syser', 'systematic coordinate error for time axis', 'caom2:Chunk.time.axis.error.syser', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 101),
( 'caom2.Chunk', 'time_axis_error_rnder', 'random coordinate error for time axis', 'caom2:Chunk.time.axis.error.rnder', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 102),
( 'caom2.Chunk', 'time_axis_range_start_pix', 'coordinate range for time axis', 'caom2:Chunk.time.axis.range.start.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 103),
( 'caom2.Chunk', 'time_axis_range_start_val', 'coordinate range for time axis', 'caom2:Chunk.time.axis.range.start.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 104),
( 'caom2.Chunk', 'time_axis_range_end_pix', 'coordinate range for time axis', 'caom2:Chunk.time.axis.range.end.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 105),
( 'caom2.Chunk', 'time_axis_range_end_val', 'coordinate range for time axis', 'caom2:Chunk.time.axis.range.end.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 106),
( 'caom2.Chunk', 'time_axis_bounds', 'coordinate bounds for time axis (string-encoded)', 'caom2:Chunk.time.axis.bounds', NULL, NULL, 'char', NULL, NULL, 1,0,0 , 107),
( 'caom2.Chunk', 'time_axis_function_naxis', 'coordinate function for time axis', 'caom2:Chunk.time.axis.function.naxis', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 108),
( 'caom2.Chunk', 'time_axis_function_refCoord_pix', 'coordinate function for time axis', 'caom2:Chunk.time.axis.function.refCoord.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 109),
( 'caom2.Chunk', 'time_axis_function_refCoord_val', 'coordinate function for time axis', 'caom2:Chunk.time.axis.function.refCoord.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 110),
( 'caom2.Chunk', 'time_axis_function_delta', 'coordinate function for time axis', 'caom2:Chunk.time.axis.function.delta', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 111),
( 'caom2.Chunk', 'time_timesys', 'time system', 'caom2:Chunk.time.timesys', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 112),
( 'caom2.Chunk', 'time_trefpos', 'time reference position', 'caom2:Chunk.time.trefpos', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 113),
( 'caom2.Chunk', 'time_mjdref', 'reference MJD value', 'caom2:Chunk.time.mjdref', NULL, 'd', 'double', NULL, NULL, 1,0,0 , 114),
( 'caom2.Chunk', 'time_exposure', 'median exposure time per pixel', 'caom2:Chunk.time.exposure', NULL, 's', 'double', NULL, NULL, 1,0,0 , 115),
( 'caom2.Chunk', 'time_resolution', 'resolution on time axis', 'caom2:Chunk.time.resolution', NULL, 's', 'double', NULL, NULL, 1,0,0 , 116),

( 'caom2.Chunk', 'polarization_axis_axis_ctype', 'coordinate type for polarization axis', 'caom2:Chunk.polarization.axis.axis.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 140),
( 'caom2.Chunk', 'polarization_axis_axis_cunit', 'coordinate unit for polarization axis', 'caom2:Chunk.polarization.axis.axis.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 141),
( 'caom2.Chunk', 'polarization_axis_error_syser', 'systematic coordinate error for polarization axis', 'caom2:Chunk.polarization.axis.error.syser', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 142),
( 'caom2.Chunk', 'polarization_axis_error_rnder', 'random coordinate error for polarization axis', 'caom2:Chunk.polarization.axis.error.rnder', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 143),
( 'caom2.Chunk', 'polarization_axis_range_start_pix', 'coordinate range for time axis', 'caom2:Chunk.polarization.axis.range.start.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 144),
( 'caom2.Chunk', 'polarization_axis_range_start_val', 'coordinate range for time axis', 'caom2:Chunk.polarization.axis.range.start.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 145),
( 'caom2.Chunk', 'polarization_axis_range_end_pix', 'coordinate range for time axis', 'caom2:Chunk.polarization.axis.range.end.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 146),
( 'caom2.Chunk', 'polarization_axis_range_end_val', 'coordinate range for time axis', 'caom2:Chunk.polarization.axis.range.end.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 147),
( 'caom2.Chunk', 'polarization_axis_bounds', 'coordinate bounds for polarization axis (string-encoded)', 'caom2:Chunk.polarization.axis.bounds', NULL, NULL, 'char', NULL, NULL, 1,0,0 , 148),
( 'caom2.Chunk', 'polarization_axis_function_naxis', 'coordinate function for polarization axis', 'caom2:Chunk.polarization.axis.function.naxis', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 149),
( 'caom2.Chunk', 'polarization_axis_function_refCoord_pix', 'coordinate function for polarization axis', 'caom2:Chunk.polarization.axis.function.refCoord.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 150),
( 'caom2.Chunk', 'polarization_axis_function_refCoord_val', 'coordinate function for polarization axis', 'caom2:Chunk.polarization.axis.function.refCoord.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 151),
( 'caom2.Chunk', 'polarization_axis_function_delta', 'coordinate function for polarization axis', 'caom2:Chunk.polarization.axis.function.delta', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 152),

( 'caom2.Chunk', 'custom_axis_axis_ctype', 'coordinate type for custom axis [new in 2.4]', 'caom2:Chunk.custom.axis.axis.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 160),
( 'caom2.Chunk', 'custom_axis_axis_cunit', 'coordinate unit for custom axis [new in 2.4]', 'caom2:Chunk.custom.axis.axis.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 161),
( 'caom2.Chunk', 'custom_axis_error_syser', 'systematic coordinate error for custom axis [new in 2.4]', 'caom2:Chunk.custom.axis.error.syser', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 162),
( 'caom2.Chunk', 'custom_axis_error_rnder', 'random coordinate error for custom axis [new in 2.4]', 'caom2:Chunk.custom.axis.error.rnder', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 163),
( 'caom2.Chunk', 'custom_axis_range_start_pix', 'coordinate range for time axis [new in 2.4]', 'caom2:Chunk.custom.axis.range.start.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 164),
( 'caom2.Chunk', 'custom_axis_range_start_val', 'coordinate range for time axis [new in 2.4]', 'caom2:Chunk.custom.axis.range.start.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 165),
( 'caom2.Chunk', 'custom_axis_range_end_pix', 'coordinate range for time axis [new in 2.4]', 'caom2:Chunk.custom.axis.range.end.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 166),
( 'caom2.Chunk', 'custom_axis_range_end_val', 'coordinate range for time axis [new in 2.4]', 'caom2:Chunk.custom.axis.range.end.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 167),
( 'caom2.Chunk', 'custom_axis_bounds', 'coordinate bounds for custom axis (string-encoded) [new in 2.4]', 'caom2:Chunk.custom.axis.bounds', NULL, NULL, 'char', NULL, NULL, 1,0,0 , 168),
( 'caom2.Chunk', 'custom_axis_function_naxis', 'coordinate function for custom axis [new in 2.4]', 'caom2:Chunk.custom.axis.function.naxis', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 169),
( 'caom2.Chunk', 'custom_axis_function_refCoord_pix', 'coordinate function for custom axis [new in 2.4]', 'caom2:Chunk.custom.axis.function.refCoord.pix', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 170),
( 'caom2.Chunk', 'custom_axis_function_refCoord_val', 'coordinate function for custom axis [new in 2.4]', 'caom2:Chunk.custom.axis.function.refCoord.val', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 171),
( 'caom2.Chunk', 'custom_axis_function_delta', 'coordinate function for custom axis [new in 2.4]', 'caom2:Chunk.custom.axis.function.delta', NULL, NULL, 'double', NULL, NULL, 1,0,0 , 172),

( 'caom2.Chunk', 'observable_independent_axis_ctype', 'independent observable axis coordinate type', 'caom2:Chunk.observable.independent.axis.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 183),
( 'caom2.Chunk', 'observable_independent_axis_cunit', 'independent observable axis coordinate unit', 'caom2:Chunk.observable.independent.axis.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 184),
( 'caom2.Chunk', 'observable_independent_bin', 'independent observable pixel value', 'caom2:Chunk.observable.independent.bin', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 185),
( 'caom2.Chunk', 'observable_dependent_axis_ctype', 'dependent observable axis coordinate type', 'caom2:Chunk.observable.dependent.axis.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 186),
( 'caom2.Chunk', 'observable_dependent_axis_cunit', 'dependent observable axis coordinate unit', 'caom2:Chunk.observable.dependent.axis.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 187),
( 'caom2.Chunk', 'observable_dependent_bin', 'dependent observable pixel value', 'caom2:Chunk.observable.dependent.bin', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 188),

( 'caom2.Chunk', 'metaProducer', 'identifier for the producer of this entity metadata (URI of the form {organisation}:{software}-{version}) [new in 2.4]', 'caom2:Chunk.metaProducer', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 200),
( 'caom2.Chunk', 'lastModified',  'timestamp of last modification of this row', 'caom2:Chunk.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0 , 201),
( 'caom2.Chunk', 'maxLastModified',  'timestamp of last modification of this entity+children', 'caom2:Chunk.maxLastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0 , 202),
( 'caom2.Chunk', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form {algorithm}:{hex value})', 'caom2:Chunk.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 203),
( 'caom2.Chunk', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form {algorithm}:{hex value})', 'caom2:Chunk.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 204)
;

-- join tables
insert into tap_schema.columns11 (table_name,column_name,description,datatype,arraysize,xtype,principal,indexed,std) values
--('caom2.ObservationMember', 'compositeID', 'composite observation identifier', 'char', '36','uuid', 0,1,0),
--('caom2.ObservationMember', 'simpleID', 'simple observation identifier', 'char', '*', 'uri', 0,1,0),
('caom2.ObservationMember', 'parentID', 'parent observation identifier [changed in 2.4]', 'char', '36','uuid', 0,1,0),
('caom2.ObservationMember', 'memberID', 'member observation identifier [changed in 2.4]', 'char', '*', 'uri', 0,1,0),

('caom2.ProvenanceInput', 'outputID', 'output plane identifier', 'char', '36','uuid', 0,1,0),
('caom2.ProvenanceInput', 'inputID', 'input plane identifier', 'char', '*', 'uri', 0,1,0)
;

insert into tap_schema.keys11 (key_id,from_table,target_table,description) values
('caom2-p-o', 'caom2.Plane', 'caom2.Observation','standard way to join the caom2.Observation and caom2.Plane tables'),
('caom2-a-p', 'caom2.Artifact', 'caom2.Plane', 'standard way to join the caom2.Plane and caom2.Artifact tables'),
('caom2-p-a', 'caom2.Part', 'caom2.Artifact','standard way to join the caom2.Artifact and caom2.Part tables'),
('caom2-c-p', 'caom2.Chunk', 'caom2.Part','standard way to join the caom2.Part and caom2. tables'),

('caom2-composite-member', 'caom2.Observation', 'caom2.ObservationMember', 
    'standard way to join caom2.Observation (CompositeObservation) and caom2.ObservationMember [join table]'),
('caom2-member-simple', 'caom2.ObservationMember', 'caom2.Observation',
    'standard way to join caom2.ObservationMember and caom2.Observation (SimpleObservation) [join table]'),

('caom2-plane-prov', 'caom2.Plane', 'caom2.ProvenanceInput',
    'standard way to join caom2.Plane (product) and caom2.ProvenanceInput [join table]'),
('caom2-prov-input', 'caom2.ProvenanceInput', 'caom2.Plane',
    'standard way to join caom2.ProvenanceInput and caom2.Plane (input) [join table]')
;

insert into tap_schema.key_columns11 (key_id,from_column,target_column) values
('caom2-p-o', 'obsID', 'obsID'),
('caom2-a-p', 'planeID', 'planeID'),
('caom2-p-a', 'artifactID', 'artifactID'),
('caom2-c-p', 'partID', 'partID'),

--('caom2-composite-member', 'obsID', 'compositeID' ),
--('caom2-member-simple', 'simpleID', 'observationURI'),
('caom2-composite-member', 'obsID', 'parentID' ),
('caom2-member-simple', 'memberID', 'observationURI'),

('caom2-plane-prov', 'planeID', 'outputID' ),
('caom2-prov-input', 'inputID', 'planeURI')
;

insert into tap_schema.tables11 (schema_name,table_name,table_type,description) values
('caom2', 'caom2.HarvestState', 'table', 'observation sync state'),
('caom2', 'caom2.HarvestSkipURI', 'table', 'list of observations to retry or artifacts to sync');

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.HarvestState', 'source',           'caom2:HarvestState.source',           'ID of the remote luskan (URI)', NULL, 'char','512*', 'uri', 1, 1, 1, 1 ),
( 'caom2.HarvestState', 'cname',            'caom2:HarvestState.cname',            'classname of the harvested entity', NULL, 'char','64*', NULL, 1, 1, 1, 2 ),
( 'caom2.HarvestState', 'curLastModified',  'caom2:HarvestState.curLastModified',  'lastModified timestamp of the last entity harvested', NULL, 'char','23*','timestamp', 1, 1, 1, 3 ),
( 'caom2.HarvestState', 'curID',            'caom2:HarvestState`.curID',           'id of the last entity harvested', NULL, 'char','36','uuid', 1, 1, 1, 4 ),
( 'caom2.HarvestState', 'lastModified',     'caom2:HarvestState.lastModified',     'timestamp of the event', NULL, 'char','23*','timestamp', 1, 1, 1, 5 ),
( 'caom2.HarvestState', 'stateID',          'caom2:HarvestState`.stateID',         'primary key', NULL, 'char','36','uuid', 1, 1, 1, 6 );

insert into tap_schema.columns11 (table_name,column_name,description,datatype,arraysize,xtype,principal,indexed,std) values
( 'caom2.HarvestSkipURI', 'source', 'harvesting source', 'char', '*', NULL, 1,1,1),
( 'caom2.HarvestSkipURI', 'cname', 'entity (class name)', 'char', '*', NULL, 1,1,1),
( 'caom2.HarvestSkipURI', 'bucket', 'random bucket code', 'char', '3', NULL, 1,1,1),
( 'caom2.HarvestSkipURI', 'skipID', 'URI of skipped entity instance', 'char', '*', 'uri', 1,1,1),
( 'caom2.HarvestSkipURI', 'tryAfter', 'timestamp for next (re)try', 'char', '23*', 'timestamp', 1,1,1),
( 'caom2.HarvestSkipURI', 'errorMessage', 'reason for harvest failure', 'char', '*', NULL, 1,0,1),
( 'caom2.HarvestSkipURI', 'lastModified', 'last modification of this entry', 'char', '23*', 'timestamp', 1,0,1),
( 'caom2.HarvestSkipURI', 'id', 'primary key', 'char', '36', 'uuid', 1,1,1);


-- caom2.SIAv1 view
insert into tap_schema.tables11 (schema_name,table_name,table_type,description,utype) values
( 'caom2', 'caom2.SIAv1', 'view', 'SIAv1 view on CAOM-2.0: caom.Observation JOIN caom.Plane JOIN caom2.Artifact, limited to calibrated science images', NULL );

insert into tap_schema.columns11 (table_name,column_name,description,ucd,utype,unit,datatype,arraysize,xtype,principal,indexed,std) values
( 'caom2.SIAv1', 'collection', 		'data collection this observation belongs to', 		NULL, NULL, NULL, 			'char', '32*',NULL, 1,1,1 ),
( 'caom2.SIAv1', 'publisherDID', 	'unique product identifier', 	'VOX:Image_Title', NULL, NULL, 	'char', '128*', NULL, 1,1,1 ),
( 'caom2.SIAv1', 'instrument_name', 	'name of the instrument used to collect the data', 	'INST_ID', NULL, NULL, 	'char', '128*',NULL, 1,1,1 ),

( 'caom2.SIAv1', 'position_bounds', 'positional coverage of the data', NULL, 'caom2:Plane.position.bounds', 'deg', 'char', '*', 'caom2:shape', 0,0,0),

( 'caom2.SIAv1', 'position_center_ra', 	'RA of central coordinates', 				'POS_EQ_RA_MAIN', NULL, 'deg', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'position_center_dec', 'DEC of central coordinates', 				'POS_EQ_DEC_MAIN', NULL, 'deg', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'position_naxes', 	'number of axes', 					'VOX:Image_Naxes', NULL, NULL, 	'int', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'position_naxis',	'dimensions (number of pixels) along spatial axes', 	'VOX:Image_Naxis', NULL, NULL, 	'long', 2, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'position_scale', 	'pixel size along spatial axes', 			'VOX:Image_Scale', NULL, NULL, 	'double', 2, NULL, 1,1,1 ),

( 'caom2.SIAv1', 'energy_bounds_center', 'medianvalue on  energy axis (barycentric wavelength)',  'VOX:BandPass_RefValue', NULL, 'm', 'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'energy_bounds_cval1', 'lower bound on energy axis (barycentric wavelength)', 	'VOX:BandPass_LoLimit', NULL, 'm', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'energy_bounds_cval2', 'upper bound on energy axis (barycentric wavelength)', 	'VOX:BandPass_HiLimit', NULL, 'm', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'energy_units', 	'units used for energy values', 			'VOX:BandPass_Unit', NULL, NULL, 	'char', '1*',NULL, 1,1,1 ),
( 'caom2.SIAv1', 'energy_bandpassName', 'collection-specific name for energy band (e.g. filter name)', 'VOX:BandPass_ID', NULL, NULL, 'char', '32*',NULL, 1,1,1 ),

( 'caom2.SIAv1', 'time_bounds_center', 	'central value on time axis (Modified Julian Day)', 	'VOX:Image_MJDateObs', NULL, 'd', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'time_bounds_cval1', 	'lower bound on time axis (Modified Julian Day)', 	'time.start;obs.exposure', NULL, 'd', 'double', NULL, NULL, 1,1,0 ),
( 'caom2.SIAv1', 'time_bounds_cval2', 	'upper bound on time axis (Modified Julian Day)', 	'time.end;obs.exposure', NULL, 'd', 	'double', NULL, NULL, 1,1,0 ),
( 'caom2.SIAv1', 'time_exposure', 	'actual exposure time', 				'time.duration;obs.exposure', NULL, 'sec', 'double', NULL, NULL, 1,1,0 ),

( 'caom2.SIAv1', 'imageFormat', 	'mimetype of the data file(s)', 'VOX:Image_Format', NULL, NULL, 'char', '128*', NULL, 1,0,1 ),
( 'caom2.SIAv1', 'accessURL', 		'access URL for the complete image', 'VOX:Image_AccessReference', NULL, NULL, 'char', '*', 'clob', 1,0,1 ),

( 'caom2.SIAv1', 'metaRelease',  'UTC timestamp when metadata is publicly visible',              NULL, NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 ),
( 'caom2.SIAv1', 'dataRelease',  'UTC timestamp when data is publicly available',                NULL, NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 )
;


-- backwards compatible: fill "size" column with values from arraysize set above
-- where arraysize is a possibly variable-length 1-dimensional value
update tap_schema.columns11 SET "size" = replace(arraysize::varchar,'*','')::int 
WHERE table_name LIKE 'caom2.%'
  AND arraysize IS NOT NULL
  AND arraysize NOT LIKE '%x%'
  AND arraysize != '*';
