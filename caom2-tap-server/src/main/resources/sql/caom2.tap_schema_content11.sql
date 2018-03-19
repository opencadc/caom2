
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

-- To swap proprietary <-> public: "1000)" <-> "NULL)"

insert into tap_schema.schemas11 (schema_name,description,utype) values
('caom2', 'Common Archive Observation Model, version 2.1', 'caom2')
;

-- index start at 20
insert into tap_schema.tables11 (schema_name,table_name,table_type,description,utype,table_index) values
( 'caom2', 'caom2.Observation', 'table', 'the main CAOM Observation table', 'caom2:Observation' , 20),
( 'caom2', 'caom2.Plane', 'table', 'the products of the observation', 'caom2:Plane' , 21),
( 'caom2', 'caom2.Artifact', 'table', 'physical data artifacts (e.g. files)', 'caom2:Artifact' , 22),
( 'caom2', 'caom2.Part', 'table', 'parts of artifacts (e.g. FITS extensions)', 'caom2:Part' , 23),
( 'caom2', 'caom2.Chunk', 'table', 'description of the data array in a part', 'caom2:Chunk' , 24),
( 'caom2', 'caom2.ObservationMember', 'table', 'composite to simple observation join table', NULL, 25),
( 'caom2', 'caom2.ProvenanceInput', 'table', 'plane.provenance to input plane join table', NULL, 26),

-- index start at 30
( 'caom2', 'caom2.EnumField', 'table', 'pre-computed aggregate (group by) table built from enumerated types in CAOM model', NULL , 30),
( 'caom2', 'caom2.ObsCoreEnumField', 'table', 'pre-computed aggregate (group by) table built from enumerated types in ObsCore-1.1 model', NULL, 31),
( 'caom2', 'caom2.distinct_proposal_id', 'table', 'pre-computed list of distinct caom2.Observation.proposal_id values', NULL , 32),
( 'caom2', 'caom2.distinct_proposal_pi', 'table', 'pre-computed list of distinct caom2.Observation.proposal_pi values', NULL , 33),
( 'caom2', 'caom2.distinct_proposal_title', 'table', 'pre-computed list of distinct caom2.Observation.proposal_title values', NULL , 34)
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
( 'caom2.Observation', 'intent', 'intended purpose of data (one of: science, calibration)', 'caom2:Observation.intent', NULL, NULL, 'char', '32*', NULL, 0,0,1 , 7),
( 'caom2.Observation', 'sequenceNumber', 'sequence number assigned by the observatory', 'caom2:Observation.sequenceNumber', NULL, NULL, 'int', NULL, NULL, 0,0,0 , NULL),
( 'caom2.Observation', 'metaRelease', 'date the metadata for an observation is public (UTC)', 'caom2:Observation.metaRelease', NULL, NULL, 'char', '23*','timestamp', 0,0,8 , 9),

( 'caom2.Observation', 'proposal_id', 'collection-specific unique proposal identifier', 'caom2:Observation.proposal.id', NULL, NULL, 'char', '128*',NULL, 0,1,0 , 20),
( 'caom2.Observation', 'proposal_pi', 'proposal principal investigator', 'caom2:Observation.proposal.pi', NULL, NULL, 'char', '128*',NULL, 0,0,0 , 21),
( 'caom2.Observation', 'proposal_project', 'project that created/acquired the observation', 'caom2:Observation.proposal.project', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 22),
( 'caom2.Observation', 'proposal_title', 'proposal title', 'caom2:Observation.proposal.title', NULL, NULL, 'char', '256*',NULL, 0,0,0 , 23),
( 'caom2.Observation', 'proposal_keywords', 'proposal keywords (separated by |)', 'caom2:Observation.proposal.keywords', NULL, NULL, 'char', '*',NULL, 0,0,0 , 24),

( 'caom2.Observation', 'target_name', 'name of intended target', 'caom2:Observation.target.name', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 30),
( 'caom2.Observation', 'target_type', 'classification of intended target', 'caom2:Observation.target.type', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 31),
( 'caom2.Observation', 'target_standard', 'intended target is a standard (0=false, 1=true)', 'caom2:Observation.target.standard', NULL, NULL, 'int', NULL,NULL, 0,0,0 , 32),
( 'caom2.Observation', 'target_redshift', 'redshift of intended target', 'caom2:Observation.target.redshift', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 33),
( 'caom2.Observation', 'target_moving', 'flag for moving target', 'caom2:Observation.target.moving', NULL, NULL, 'int', NULL,NULL, 0,0,0 , 34),
( 'caom2.Observation', 'target_keywords', 'target keywords (separated by |)', 'caom2:Observation.target.keywords', NULL, NULL, 'char', '*',NULL, 0,0,0 , 35),

( 'caom2.Observation', 'telescope_name', 'name of telescope used to acquire observation', 'caom2:Observation.telescope.name', NULL, NULL, 'char', '32*',NULL, 1,0,0 , 40),
( 'caom2.Observation', 'telescope_geoLocationX', 'x component of geocentric location of telescope', 'caom2:Observation.telescope.geoLocationX', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 41),
( 'caom2.Observation', 'telescope_geoLocationY', 'y component of geocentric location of telescope', 'caom2:Observation.telescope.geoLocationY', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 42),
( 'caom2.Observation', 'telescope_geoLocationZ', 'z component of geocentric location of telescope', 'caom2:Observation.telescope.geoLocationZ', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 43),
( 'caom2.Observation', 'telescope_keywords', 'telescope keywords (separated by |)', 'caom2:Observation.telescope.keywords', NULL, NULL, 'char', '*', NULL, 0,0,0 , 44),

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

( 'caom2.Observation', 'members', 'members of a composite observation (space-separated list of Observation URIs)', 'caom2:Observation.members', NULL, NULL, 'char','*','clob', 0,0,0 , 80),

( 'caom2.Observation', 'lastModified', 'timestamp of last modification of this row', 'caom2:Observation.lastModified', NULL, NULL, 'char', '23*','timestamp', 1,0,0 , 100),
( 'caom2.Observation', 'maxLastModified', 'timestamp of last modification of this observation', 'caom2:Observation.maxLastModified', NULL, NULL, 'char', '23*','timestamp', 1,0,0 , 101),
( 'caom2.Observation', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form <algorithm>:<value>)', 'caom2:Observation.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 102),
( 'caom2.Observation', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form <algorithm>:<value>)', 'caom2:Observation.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 103)
;

-- Plane
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index,id) values
( 'caom2.Plane', 'planeURI', 'unique internal URI for this product', 'caom2:Plane.uri', NULL, NULL, 'char', '*', 'uri', 1,1,1 , 1, 'caomPlaneURI'),
( 'caom2.Plane', 'publisherID', 'unique global identifier for this product', 'caom2:Plane.publisherID', NULL, NULL, 'char', '*', 'uri', 1,1,1 , 1, 'caomPublisherID')
;
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Plane', 'obsID', 'foreign key', NULL, NULL, NULL,                              'char','36','uuid', 0,1,0 , 2),
( 'caom2.Plane', 'planeID', 'unique plane identifier', 'caom2:Plane.id', NULL, NULL,    'char','36','uuid', 0,1,0 , 3),

( 'caom2.Plane', 'productID', 'name of this product', 'caom2:Plane.productID', NULL, NULL, 'char', '32*', NULL, 1,1,1 , NULL),
( 'caom2.Plane', 'metaRelease', 'date the metadata for a plane is public (UTC)', 'caom2:Plane.metaRelease', NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 , 4),
( 'caom2.Plane', 'dataRelease', 'date the data for a plane is public (UTC)', 'caom2:Plane.dataRelease', NULL, NULL, 'char', '23*', 'timestamp', 0,1,0 , 5),

( 'caom2.Plane', 'dataProductType', 'IVOA ObsCore data product type', 	'caom2:Plane.dataProductType', NULL, NULL, 'char', '128*', NULL, 1,0,1 , 10),
( 'caom2.Plane', 'calibrationLevel', 'IVOA ObsCore calibration level (0,1,2,3,...)', 'caom2:Plane.calibrationLevel', NULL, NULL, 'int', NULL, NULL, 1,0,1 , 11),

( 'caom2.Plane', 'provenance_name', 'name of the process that created this plane', 'caom2:Plane.provenance.name', NULL, NULL, 'char', '128*', NULL, 0,0,1 , 20),
( 'caom2.Plane', 'provenance_version', 'version of the process/software', 'caom2:Plane.provenance.version', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 21),
( 'caom2.Plane', 'provenance_reference', 'external reference (URL)', 'caom2:Plane.provenance.reference', NULL, NULL, 'char', '256*', NULL, 0,0,0 , 22),
( 'caom2.Plane', 'provenance_producer', 'responsible entity (e.g. person)', 'caom2:Plane.provenance.producer', NULL, NULL, 'char', '128*', NULL, 0,0,0, 23),
( 'caom2.Plane', 'provenance_project', 'responsible entity (e.g. person)', 'caom2:Plane.provenance.project', NULL, NULL, 'char', '256*', NULL, 0,0,0, 24),
( 'caom2.Plane', 'provenance_runID', 'responsible entity (e.g. person)', 'caom2:Plane.provenance.runID', NULL, NULL, 'char', '64*', NULL, 0,1,0, 25),
( 'caom2.Plane', 'provenance_lastExecuted', 'date this process was last executed', 'caom2:Plane.provenance.lastExecuted', NULL, NULL, 'char', '23*', 'timestamp', 0,0,0 , 26),
( 'caom2.Plane', 'provenance_keywords', 'provenance keywords (separated by |)', 'caom2:Plane.provenance.keywords', NULL, NULL, 'char', '*', NULL, 0,0,0 , 27),
( 'caom2.Plane', 'provenance_inputs', 'inputs of the process that created this plane (space-separated list of Plane URIs)', 'caom2:Plane.provenance.inputs', NULL, NULL, 'char','*','clob', 0,0,0 , 28),

( 'caom2.Plane', 'metrics_sourceNumberDensity', 'number density of sources', 'caom2:Plane.metrics.sourceNumberDensity', NULL, 'per sq. deg', 'double', NULL, NULL, 0,0,0 , 30),
( 'caom2.Plane', 'metrics_background', 'background intensity', 'caom2:Plane.metrics.background', NULL, 'Jy/pix', 'double', NULL,NULL, 0,0,0 , 31),
( 'caom2.Plane', 'metrics_backgroundStddev', 'standard deviation in background', 'caom2:Plane.metrics.backgroundStddev', NULL, 'Jy/pix', 'double', NULL, NULL, 0,0,0 , 32),
( 'caom2.Plane', 'metrics_fluxDensityLimit', 'flux density limit where S:N=5 for point source', 'caom2:Plane.metrics.fluxDensityLimit', NULL, 'Jy', 'double', NULL, NULL, 0,0,0 , 33),
( 'caom2.Plane', 'metrics_magLimit', 'AB magnitude limit where S:N=5 for point source', 'caom2:Plane.metrics.magLimit', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 34),

( 'caom2.Plane', 'quality_flag', 'flag describing the data quality (possible values: junk)', 'caom2:Plane.quality.flag', NULL, NULL, 'char', '16*', NULL, 1,0,0 , 40),

( 'caom2.Plane', 'position_bounds', 'positional coverage of the data', 'caom2:Plane.position.bounds', NULL, 'deg', 'double', '*', 'polygon', 0,0,0 , 50),
( 'caom2.Plane', 'position_bounds_samples', 'positional coverage of the data', 'caom2:Plane.position.bounds.samples', NULL, 'deg', 'double', '*', 'caom2:MultiPolygon', 0,0,0 , 50),
( 'caom2.Plane', 'position_bounds_size', 'size of the polygon bounds (diameter of minimum spanning circle)', 'caom2:Plane.position.bounds.size', NULL, 'deg', 'double', NULL, NULL, 0,0,0 , 51),
( 'caom2.Plane', 'position_resolution', 'median spatial resolution (FWHM)', 'caom2:Plane.position.resolution', NULL, 'arcsec', 'double', NULL, NULL, 0,0,0 , 52),
( 'caom2.Plane', 'position_sampleSize', 'median sample (pixel) size on spatial axes', 'caom2:Plane.position.sampleSize', NULL, 'arcsec', 'double', NULL, NULL, 0,0,0 , 53),
( 'caom2.Plane', 'position_dimension_naxis1', 'dimensions (number of pixels) along one spatial axis', 'caom2:Plane.position.dimension.naxis1', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 54),
( 'caom2.Plane', 'position_dimension_naxis2', 'dimensions (number of pixels) along other spatial axis', 'caom2:Plane.position.dimension.naxis2', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 55),
( 'caom2.Plane', 'position_timeDependent', 'flag indicating that the position is time-dependent (0=false, 1=true)', 'caom2:Plane.position.timeDependent', NULL, NULL, 'int', NULL, NULL, 0,0,0 , 56),
( 'caom2.Plane', 'position_bounds_points', 'internal polymorphic shape support', 'caom2:Plane.position.bounds.points', NULL, 'deg', 'double', '*', NULL, 0,0,0 , 50),

( 'caom2.Plane', 'energy_emBand', 'generic name for energy band (Radio,Millimeter,Infrared,Optical,UV,EUV,X-ray,Gamma-ray)', 'caom2:Plane.energy.emBand', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 60),
( 'caom2.Plane', 'energy_bounds', 'energy coverage (barycentric wavelength)', 'caom2:Plane.energy.bounds', NULL, 'm', 'double', '2','interval', 0,0,0 , 61),
( 'caom2.Plane', 'energy_bounds_samples', 'detailed energy coverage (barycentric wavelength)', 'caom2:Plane.energy.bounds.samples', NULL, 'm', 'double', '2x*', 'interval', 0,0,0 , 61),
( 'caom2.Plane', 'energy_bounds_lower', 'lower bound on energy axis (barycentric wavelength)', 'caom2:Plane.energy.bounds.lower', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 62),
( 'caom2.Plane', 'energy_bounds_upper', 'upper bound on energy axis (barycentric wavelength)', 'caom2:Plane.energy.bounds.upper', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 63),
( 'caom2.Plane', 'energy_bounds_width', 'width of the energy bounds', 'caom2:Plane.energy.bounds.width', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 64),
( 'caom2.Plane', 'energy_bounds_integrated', 'sum of width of subsamples', 'caom2:Plane.energy.bounds.integrated', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 64),
( 'caom2.Plane', 'energy_dimension', 'dimension (number of pixels) along energy axis', 'caom2:Plane.energy.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 66),
( 'caom2.Plane', 'energy_resolvingPower', 'median spectral resolving power (R)', 'caom2:Plane.energy.resolvingPower', NULL, NULL, 'double', NULL, NULL, 0,0,0 , 67),
( 'caom2.Plane', 'energy_sampleSize', 'median sample (pixel) size on energy axis', 'caom2:Plane.energy.sampleSize', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 68),
( 'caom2.Plane', 'energy_bandpassName', 'collection-specific name for energy band (e.g. filter name)', 'caom2:Plane.energy.bandpassName', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 69),
( 'caom2.Plane', 'energy_transition_species', 'atom or molecule', 'caom2:Plane.energy.transition.species', NULL, NULL, 'char', '32*',NULL, 0,0,0 , 70),
( 'caom2.Plane', 'energy_transition_transition', 'specific energy transition of species', 'caom2:Plane.energy.transition.transition', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 71),
( 'caom2.Plane', 'energy_freqWidth', 'width of the energy bounds (convenience: in frequency)', 'caom2:Plane.energy.freqWidth', NULL, 'Hz', 'double', NULL, NULL, 0,0,0 , 72),
( 'caom2.Plane', 'energy_freqSampleSize', 'median sample (pixel) size on energy axis (convenience: in frequency)', 'caom2:Plane.energy.freqSampleSize', NULL, 'Hz', 'double', NULL, NULL, 0,0,0 , 73),
( 'caom2.Plane', 'energy_restwav', 'rest wavelength of target spectral feature (barycentric)', 'caom2:Plane.energy.restwav', NULL, 'm', 'double', NULL, NULL, 0,0,0 , 74),

( 'caom2.Plane', 'time_bounds', 'time coverage (Modified Julian Day)', 'caom2:Plane.time.bounds', NULL, 'd', 'double', '2', 'interval', 0,0,0 , 80),
( 'caom2.Plane', 'time_bounds_samples', 'detailed time coverage (Modified Julian Day)', 'caom2:Plane.time.bounds.samples', NULL, 'm', 'double', '2x*', 'interval', 0,0,0 , 61),
( 'caom2.Plane', 'time_bounds_lower', 'lower bound on time axis (Modified Julian Day)', 'caom2:Plane.time.bounds.lower', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 81),
( 'caom2.Plane', 'time_bounds_upper', 'upper bound on time axis (Modified Julian Day)', 'caom2:Plane.time.bounds.upper', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 82),
( 'caom2.Plane', 'time_bounds_width', 'width of the time bounds', 'caom2:Plane.time.bounds.width', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 83),
( 'caom2.Plane', 'time_bounds_integrated', 'sum of width of subsamples', 'caom2:Plane.time.bounds.integrated', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 84),
( 'caom2.Plane', 'time_dimension', 'dimension (number of pixels) along time axis', 'caom2:Plane.time.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 85),
( 'caom2.Plane', 'time_resolution', 'median resolution on the time axis', 'caom2:Plane.time.resolution', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 86),
( 'caom2.Plane', 'time_sampleSize', 'median sample (pixel) size on time axis', 'caom2:Plane.time.sampleSize', NULL, 'd', 'double', NULL, NULL, 0,0,0 , 87),
( 'caom2.Plane', 'time_exposure', 'median exposure time per pixel', 'caom2:Plane.time.exposure', NULL, 's', 'double', NULL, NULL, 0,0,0 , 88),

( 'caom2.Plane', 'polarization_states', 'polarization letter codes in canonical order, separated by /', 'caom2:Plane.polarization.states', NULL, NULL, 'char', '32*', NULL, 0,0,0 , 90),
( 'caom2.Plane', 'polarization_dimension', 'number of samples (pixels) along polarization axis', 'caom2:Plane.polarization.dimension', NULL, NULL, 'long', NULL, NULL, 0,0,0 , 91),

( 'caom2.Plane', 'lastModified', 'timestamp of last modification of this row', 'caom2:Plane.lastModified', NULL, NULL, 'char', '23*','timestamp', 1,1,0 , 100),
( 'caom2.Plane', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form <algorithm>:<value>)', 'caom2:Plane.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 101),
( 'caom2.Plane', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form <algorithm>:<value>)', 'caom2:Plane.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 102)
;

-- Artifact
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Artifact', 'planeID',       'foreign key', NULL, NULL, NULL, 'char','36','uuid', 0,1,0 , 1),
( 'caom2.Artifact', 'artifactID',    'unique artifact identifier', 'caom2:Artifact.id', NULL, NULL, 'char','36','uuid', 0,1,0 , 2),

( 'caom2.Artifact', 'uri',           'external URI for the physical artifact', 'caom2:Artifact.uri', NULL, NULL, 'char', '*','uri', 1,1,0 , 3),
( 'caom2.Artifact', 'productType',   'product type (science, calibration, auxiliary, preview, info)', 'caom2:Artifact.productType', NULL, NULL, 'char', '32*', NULL, 1,0,0 , 4),
( 'caom2.Artifact', 'releaseType',   'release type (data, meta), new: not in use', 'caom2:Artifact.releaseType', NULL, NULL, 'char', '16*', NULL, 1,0,0 , 5),
( 'caom2.Artifact', 'contentType',   'content-type of the representation at uri', 'caom2:Artifact.contentType', NULL, NULL, 'char', '128*', NULL, 1,0,0 , 6),
( 'caom2.Artifact', 'contentLength', 'size of the representation at uri', 'caom2:Artifact.contentLength', NULL, 'byte', 'long', NULL, NULL, 1,0,0 , 7),
( 'caom2.Artifact', 'contentChecksum', 'checksum of the content (URI of the form <algorithm>:<value>)', 'caom2:Artifact.contentChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 8),

( 'caom2.Artifact', 'accessURL',     'access URL for the complete file', NULL, NULL, NULL, 'char', '*','clob', 0,0,0, 9),

( 'caom2.Artifact', 'lastModified',  'timestamp of last modification of this row', 'caom2:Artifact.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0, 10),
( 'caom2.Artifact', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form <algorithm>:<value>)', 'caom2:Artifact.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 11),
( 'caom2.Artifact', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form <algorithm>:<value>)', 'caom2:Artifact.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 12)
;

-- Part
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Part', 'artifactID',    'foreign key', NULL, NULL, NULL, 'char', '36','uuid', 0,1,0 , 1),
( 'caom2.Part', 'partID',        'unique part identifier', 'caom2:Part.id', NULL, NULL, 'char', '36','uuid', 0,1,0 , 2),

( 'caom2.Part', 'name',          'name of this part', 'caom2:Part.name', NULL, NULL, 'char', '128*', NULL, 1,0,0 , 3),
( 'caom2.Part', 'productType',   'product type (science, calibration, auxiliary, preview, info)', 'caom2:Part.productType', NULL, NULL, 'char', '32*', NULL, 1,0,0 , 4),

( 'caom2.Part', 'lastModified',  'timestamp of last modification of this row', 'caom2:Part.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0 , 10),
( 'caom2.Part', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form <algorithm>:<value>)', 'caom2:Part.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 11),
( 'caom2.Part', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form <algorithm>:<value>)', 'caom2:Part.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 12)
;

-- Chunk
insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.Chunk', 'partID',       'parent identifier', NULL, NULL, NULL, 'char', '36','uuid', 0,1,0 , 1),
( 'caom2.Chunk', 'chunkID',    'unique chunk identifier', 'caom2:Chunk.id', NULL, NULL, 'char', '36','uuid', 0,1,0 , 2),

( 'caom2.Chunk', 'productType',   'deprecated', 'caom2:Chunk.productType', NULL, NULL, 'char', '32*', NULL, 1,0,0 , 999),

( 'caom2.Chunk', 'naxis', 'size of the representation at uri', 'caom2:Chunk.naxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 10),
( 'caom2.Chunk', 'positionAxis1', 'axis for the first position coordinate', 'caom2:Chunk.positionAxis1', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 11),
( 'caom2.Chunk', 'positionAxis2', 'axis for the second position coordinate', 'caom2:Chunk.positionAxis2', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 12),
( 'caom2.Chunk', 'energyAxis', 'axis for the energy coordinate', 'caom2:Chunk.energyAxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 13),
( 'caom2.Chunk', 'timeAxis', 'axis for the time coordinate', 'caom2:Chunk.timeAxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 14),
( 'caom2.Chunk', 'polarizationAxis', 'axis for the polarization coordinate', 'caom2:Chunk.polarizationAxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 15),
( 'caom2.Chunk', 'observableAxis', 'axis for the observable quantity', 'caom2:Chunk.observableAxis', NULL, NULL, 'int', NULL, NULL, 1,0,0 , 16),

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

( 'caom2.Chunk', 'observable_independent_axis_ctype', 'independent observable axis coordinate type', 'caom2:Chunk.observable.independent.axis.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 153),
( 'caom2.Chunk', 'observable_independent_axis_cunit', 'independent observable axis coordinate unit', 'caom2:Chunk.observable.independent.axis.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 154),
( 'caom2.Chunk', 'observable_independent_bin', 'independent observable pixel value', 'caom2:Chunk.observable.independent.bin', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 155),
( 'caom2.Chunk', 'observable_dependent_axis_ctype', 'dependent observable axis coordinate type', 'caom2:Chunk.observable.dependent.axis.ctype', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 156),
( 'caom2.Chunk', 'observable_dependent_axis_cunit', 'dependent observable axis coordinate unit', 'caom2:Chunk.observable.dependent.axis.cunit', NULL, NULL, 'char', '8*', NULL, 1,0,0 , 157),
( 'caom2.Chunk', 'observable_dependent_bin', 'dependent observable pixel value', 'caom2:Chunk.observable.dependent.bin', NULL, NULL, 'long', NULL, NULL, 1,0,0 , 158),

( 'caom2.Chunk', 'lastModified',  'timestamp of last modification of this row', 'caom2:Chunk.lastModified', NULL, NULL, 'char', '23*', 'timestamp', 1,1,0 , 200),
( 'caom2.Chunk', 'metaChecksum', 'checksum of the metadata in this entity (URI of the form <algorithm>:<value>)', 'caom2:Chunk.metaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 201),
( 'caom2.Chunk', 'accMetaChecksum', 'checksum of the metadata in this entity+children (URI of the form <algorithm>:<value>)', 'caom2:Chunk.accMetaChecksum', NULL, NULL, 'char', '*', 'uri', 1,0,0 , 202)
;

-- join tables
insert into tap_schema.columns11 (table_name,column_name,description,datatype,arraysize,xtype,principal,indexed,std) values
('caom2.ObservationMember', 'compositeID', 'composite observation identifier', 'char', '36','uuid', 0,1,0),
('caom2.ObservationMember', 'simpleID', 'simple observation identifier', 'char', '*', 'uri', 0,1,0),
('caom2.ProvenanceInput', 'outputID', 'output plane identifier', 'char', '36','uuid', 0,1,0),
('caom2.ProvenanceInput', 'inputID', 'input plane identifier', 'char', '*', 'uri', 0,1,0)
;

-- EnumField
insert into tap_schema.columns11 (table_name,column_name,description,utype,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.EnumField', 'num_tuples',    'number of occurances of this combination', NULL, 'long',NULL,NULL, 0,1,0 , 1),
( 'caom2.EnumField', 'max_time_bounds_cval1', 'maximum timestamp of observations with this combination', NULL, 'double', NULL, NULL, 0,1,0 , 2),

( 'caom2.EnumField', 'collection',    'name of the data collection', 'caom2:Observation.collection', 'char', '64*', NULL, 0,1,0 , 3),
( 'caom2.EnumField', 'telescope_name',    'name of the telescope', 'caom2:Observation.telescope.name', 'char', '64*', NULL, 0,1,0 , 4),
( 'caom2.EnumField', 'instrument_name',    'name of the instrument', 'caom2:Observation.instrument.name', 'char', '64*', NULL, 0,1,0 , 5),
( 'caom2.EnumField', 'type',    'type of observation (from FITS OBSTYPE keyword)', 'caom2:Observation.type', 'char', '32*', NULL, 0,1,0 , 6),
( 'caom2.EnumField', 'intent',    'intent of the observation (one of: science, calibration)', 'caom2:Observation.intent', 'char', '32*', NULL, 0,1,0 , 7),

( 'caom2.EnumField', 'dataProductType',    'IVOA ObsCore data product type', 'caom2:Plane.dataProductType', 'char', '128*', NULL, 0,1,0 , 8),
( 'caom2.EnumField', 'calibrationLevel',    'IVOA ObsCore calibration level (0,1,2,3,...)', 'caom2:Plane.calibrationLevel', 'int', NULL, NULL, 0,1,0 , 9),
( 'caom2.EnumField', 'energy_emBand',    'IVOA name for energy band (Radio,Millimeter,Infrared,Optical,UV,EUV,X-ray,Gamma-ray)', 'caom2:Plane.energy.emBand', 'char', '32*', NULL, 0,1,0 , 10),
( 'caom2.EnumField', 'energy_bandpassName', 'collection-specific name for energy band (e.g. filter name)', 'caom2:Plane.energy.bandpassName', 'char', '32*',NULL, 0,1,0 , 11)
;

-- distinct_ tables
insert into tap_schema.columns11 (table_name,column_name,description,utype,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.distinct_proposal_id', 'num_tuples',    'number of occurances of this value', NULL, 'long',NULL,NULL, 0,1,0 , NULL),
( 'caom2.distinct_proposal_id', 'proposal_id',    'telescope proposal identifier', 'caom2:Observation.proposal.id', 'char', '64*',NULL, 0,1,0 , NULL),

( 'caom2.distinct_proposal_pi', 'num_tuples',    'number of occurances of this value', NULL, 'long', NULL, NULL, 0,1,0 , NULL),
( 'caom2.distinct_proposal_pi', 'proposal_pi',    'principle investigator on proposal', 'caom2:Observation.proposal.pi', 'char', '64*',NULL, 0,1,0 , NULL),

( 'caom2.distinct_proposal_title', 'num_tuples',    'number of occurances of this value', NULL, 'long', NULL, NULL, 0,1,0 , NULL),
( 'caom2.distinct_proposal_title', 'proposal_title',    'title of proposal', 'caom2:Observation.proposal.title', 'char', '256*',NULL, 0,1,0 , NULL)
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
('caom2-composite-member', 'obsID', 'compositeID' ),
('caom2-member-simple', 'simpleID', 'observationURI'),
('caom2-plane-prov', 'planeID', 'outputID' ),
('caom2-prov-input', 'inputID', 'planeURI')
;

-- ObsCoreEnumField
insert into tap_schema.columns11 (table_name,column_name,description,utype,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'caom2.ObsCoreEnumField', 'num_tuples',    'number of occurances of this combination', NULL, 'long', NULL, NULL, 0,1,0 , 10),
( 'caom2.ObsCoreEnumField', 'max_t_min', 'maximum timestamp of observations with this combination', NULL, 'double', NULL, NULL, 0,1,0 , 11),

( 'caom2.ObsCoreEnumField', 'obs_collection',    'name of the data collection', 'obscore:DataID.Collection', 'char', '64*',NULL, 0,1,0 , 1),
( 'caom2.ObsCoreEnumField', 'facility_name',    'name of the telescope', 'obscore:Provenance.ObsConfig.Facility.name', 'char', '64*',NULL, 0,1,0,2),
( 'caom2.ObsCoreEnumField', 'instrument_name',    'name of the instrument', 'obscore:Provenance.ObsConfig.Instrument.name', 'char', '64*',NULL, 0,1,0 ,3),
( 'caom2.ObsCoreEnumField', 'dataproduct_type',    'IVOA ObsCore data product type', 'obscore:Obs.dataProductType', 'char', '16*',NULL, 0,1,0 ,4),
( 'caom2.ObsCoreEnumField', 'calib_level',    'IVOA ObsCore calibration level (0,1,2,3,...)', 'obscore:Obs.calibLevel', 'int', NULL, NULL, 0,1,0,5)
;

-- caom2.SIAv1 view
insert into tap_schema.tables11 (schema_name,table_name,table_type,description,utype) values
( 'caom2', 'caom2.SIAv1', 'view', 'SIAv1 view on CAOM-2.0: caom.Observation JOIN caom.Plane JOIN caom2.Artifact, limited to calibrated science images', NULL );

insert into tap_schema.columns11 (table_name,column_name,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std) values
( 'caom2.SIAv1', 'collection', 		'data collection this observation belongs to', 		NULL, NULL, 			'char', NULL,NULL, 1,1,1 ),
( 'caom2.SIAv1', 'publisherDID', 	'unique product identifier', 	'VOX:Image_Title', NULL, 	'char', '128*', NULL, 1,1,1 ),
( 'caom2.SIAv1', 'instrument_name', 	'name of the instrument used to collect the data', 	'INST_ID', NULL, 	'char', '128*',NULL, 1,1,1 ),

( 'caom2.SIAv1', 'position_bounds', 	'boundary of the image', 				NULL, 'deg', 	'double', '*', 'polygon', 1,1,1 ),

( 'caom2.SIAv1', 'position_center_ra', 	'RA of central coordinates', 				'POS_EQ_RA_MAIN', 'deg', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'position_center_dec', 'DEC of central coordinates', 				'POS_EQ_DEC_MAIN', 'deg', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'position_naxes', 	'number of axes', 					'VOX:Image_Naxes', NULL, 	'int', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'position_naxis',	'dimensions (number of pixels) along spatial axes', 	'VOX:Image_Naxis', NULL, 	'long', 2, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'position_scale', 	'pixel size along spatial axes', 			'VOX:Image_Scale', NULL, 	'double', 2, NULL, 1,1,1 ),

( 'caom2.SIAv1', 'energy_bounds_center', 'medianvalue on  energy axis (barycentric wavelength)',  'VOX:BandPass_RefValue', 'm', 'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'energy_bounds_cval1', 'lower bound on energy axis (barycentric wavelength)', 	'VOX:BandPass_LoLimit', 'm', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'energy_bounds_cval2', 'upper bound on energy axis (barycentric wavelength)', 	'VOX:BandPass_HiLimit', 'm', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'energy_units', 	'units used for energy values', 			'VOX:BandPass_Unit', NULL, 	'char', '32*',NULL, 1,1,1 ),
( 'caom2.SIAv1', 'energy_bandpassName', 'collection-specific name for energy band (e.g. filter name)', 'VOX:BandPass_ID', NULL, 'char', '32*',NULL, 1,1,1 ),

( 'caom2.SIAv1', 'time_bounds_center', 	'central value on time axis (Modified Julian Day)', 	'VOX:Image_MJDateObs', 'd', 	'double', NULL, NULL, 1,1,1 ),
( 'caom2.SIAv1', 'time_bounds_cval1', 	'lower bound on time axis (Modified Julian Day)', 	'time.start;obs.exposure', 'd', 'double', NULL, NULL, 1,1,0 ),
( 'caom2.SIAv1', 'time_bounds_cval2', 	'upper bound on time axis (Modified Julian Day)', 	'time.end;obs.exposure', 'd', 	'double', NULL, NULL, 1,1,0 ),
( 'caom2.SIAv1', 'time_exposure', 	'actual exposure time', 				'time.duration;obs.exposure', 'sec', 'double', NULL, NULL, 1,1,0 ),

( 'caom2.SIAv1', 'imageFormat', 	'mimetype of the data file(s)', 'VOX:Image_Format', NULL, 'char', '128*', NULL, 1,0,1 ),
( 'caom2.SIAv1', 'accessURL', 		'access URL for the complete image', 'VOX:Image_AccessReference', NULL, 'char', '*', 'clob', 1,0,1 ),

( 'caom2.SIAv1', 'metaRelease',  'UTC timestamp when metadata is publicly visible',              NULL, NULL,                     'char', '23*', 'timestamp', 0,1,0 ),
( 'caom2.SIAv1', 'dataRelease',  'UTC timestamp when data is publicly available',                NULL, NULL,                     'char', '23*', 'timestamp', 0,1,0 )
;

insert into tap_schema.tables11 (schema_name,table_name,table_type,description) values
('caom2', 'caom2.HarvestSkipURI', 'table', 'list of observations that failed to be harvested');

insert into tap_schema.columns11 (table_name,column_name,description,datatype,arraysize,xtype,principal,indexed,std) values
( 'caom2.HarvestSkipURI', 'source', 'harvesting source', 'char', '*', NULL, 1,1,1),
( 'caom2.HarvestSkipURI', 'cname', 'entity (class name)', 'char', '*', NULL, 1,1,1),
( 'caom2.HarvestSkipURI', 'skipID', 'URI of skipped entity instance', 'char', '*', 'uuid', 1,1,1),
( 'caom2.HarvestSkipURI', 'tryAfter', 'timestamp for next (re)try', 'char', '23*', 'timestamp', 1,1,1),
( 'caom2.HarvestSkipURI', 'errorMessage', 'reason for harvest failure', 'char', '*', NULL, 1,0,1),
( 'caom2.HarvestSkipURI', 'lastModified', 'last modification of this entry', 'char', '23*', 'timestamp', 1,0,1);


-- backwards compatible: fill "size" column with values from arraysize set above
-- where arraysize is a possibly variable-length 1-dimensional value
update tap_schema.columns11 SET "size" = replace(arraysize::varchar,'*','')::int 
WHERE table_name LIKE 'caom2.%'
  AND arraysize IS NOT NULL
  AND arraysize NOT LIKE '%x%'
  AND arraysize != '*';
