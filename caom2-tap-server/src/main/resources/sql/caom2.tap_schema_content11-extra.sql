-- additional tables and views to support data discovery --

insert into tap_schema.tables11 (schema_name,table_name,table_type,description,utype,table_index) values
-- index start at 30
( 'caom2', 'caom2.EnumField', 'table', 'pre-computed aggregate (group by) materialised view built from enumerated types in CAOM model', NULL , 30),
( 'caom2', 'caom2.ObsCoreEnumField', 'table', 'pre-computed aggregate (group by) materialised view built from enumerated types in ObsCore-1.1 model', NULL, 31),
( 'caom2', 'caom2.distinct_proposal_id', 'table', 'pre-computed materialised view of distinct caom2.Observation.proposal_id values', NULL , 32),
( 'caom2', 'caom2.distinct_proposal_pi', 'table', 'pre-computed materialised view of distinct caom2.Observation.proposal_pi values', NULL , 33),
( 'caom2', 'caom2.distinct_proposal_title', 'table', 'pre-computed materialised view of distinct caom2.Observation.proposal_title values', NULL , 34)
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
