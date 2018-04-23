
-- additional tables and views to support data discovery --

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

