
-- delete key columns for keys from tables in the caom2 schema
delete from tap_schema.key_columns11 where
key_id in (select key_id from tap_schema.keys11 where 
    from_table in (select table_name from tap_schema.tables11 where schema_name = 'ivoa')
    or
    target_table in (select table_name from tap_schema.tables11 where schema_name = 'ivoa')
)
;

-- delete keys from tables in the ivoa schema
delete from tap_schema.keys11 where 
from_table in (select table_name from tap_schema.tables11 where schema_name = 'ivoa')
or
target_table in (select table_name from tap_schema.tables11 where schema_name = 'ivoa')
;

-- delete columns from tables in the ivoa schema
delete from tap_schema.columns11 where table_name in 
(select table_name from tap_schema.tables11 where schema_name = 'ivoa')
;

-- delete tables
delete from tap_schema.tables11 where schema_name = 'ivoa'
;

-- delete the schema
delete from tap_schema.schemas11 where schema_name = 'ivoa'
;

insert into tap_schema.schemas11 (schema_name,description) values
('ivoa', 'tables and views defined by the IVOA, sometimes including prototypes!')
;

-- ivoa
-- index start at 10
insert into tap_schema.tables11 (schema_name,table_name,table_type,description,table_index) values
( 'ivoa', 'ivoa.ObsCore', 'view', 'WD-ObsCore-1.2 observations', 10),
( 'ivoa', 'ivoa.ObsCore_radio', 'view', 'PEN-ObsCoreExtensionForRadioData-1.0 radio extension', 11)
;

insert into tap_schema.columns11 
    (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index,column_id) values
( 'ivoa.ObsCore', 'obs_publisher_did', 	'obscore:Curation.PublisherDID',
    'publisher dataset identifier', 'meta.ref.uri;meta.curation', NULL, 'char', '256*','uri', 1,1,1,1, 'ivoaPublisherID');

insert into tap_schema.columns11 
    (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'ivoa.ObsCore', 'obs_collection', 	'obscore:DataID.Collection',
    'short name for the data colection', 'meta.id', NULL, 'char','128*',NULL, 1,0,1,2),
( 'ivoa.ObsCore', 'facility_name', 	'obscore:Provenance.ObsConfig.Facility.name',
    'telescope name', 'meta.id;instr.tel', NULL, 'char','128*',NULL, 1,0,1,3),
( 'ivoa.ObsCore', 'instrument_name', 	'obscore:Provenance.ObsConfig.Instrument.name',
    'instrument name', 'meta.id;instr', NULL, 'char','128*',NULL, 1,0,1,4),
( 'ivoa.ObsCore', 'target_name', 'obscore:Target.Name',
    'name of intended target', 'meta.id;src', NULL, 'char', '32*',NULL, 1,0,1,5),
( 'ivoa.ObsCore', 'obs_id', 	'obscore:DataID.observationID',
    'internal dataset identifier', 'meta.id', NULL, 'char','128*',NULL, 1,0,1,6);

insert into tap_schema.columns11 
    (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'ivoa.ObsCore', 'obs_creator_did', 	'obscore:DataID.creatorDID',
    'creator dataset identifier', 'meta.id', NULL, 'char', '256*','uri', 1,1,1,7),
( 'ivoa.ObsCore', 'obs_release_date', 'obscore:Curation.releaseDate',
    'timestamp of date the data becomes publicly available', 'time.release', NULL, 'char','23*','timestamp', 0,0,1,8),
( 'ivoa.ObsCore', 'dataproduct_type', 	'obscore:ObsDataset.dataProductType',
    'type of product', 'meta.code.class', NULL, 'char', '128*',NULL, 1,0,1,9),
( 'ivoa.ObsCore', 'calib_level', 	'obscore:ObsDataset.calibLevel',
    'calibration level (0,1,2,3)', 'meta.code;obs.calib', NULL, 'int',NULL,NULL, 1,0,1,10),
( 'ivoa.ObsCore', 'access_url', 'obscore:Access.Reference',
    'URL to download the data', 'meta.ref.url', NULL, 'char', '*','clob', 1,0,1,20),
( 'ivoa.ObsCore', 'access_format', 'obscore:Access.Format',
    'format of the data file(s)', 'meta.code.mime', NULL, 'char','128*',NULL, 1,0,1,21),
( 'ivoa.ObsCore', 'access_estsize', 'obscore:Access.Size',
    'estimated size of the download', 'phys.size;meta.file', 'kbyte', 'long',NULL,NULL, 1,0,1,22);

insert into tap_schema.columns11 
    (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'ivoa.ObsCore', 's_ra', 'obscore:Char.SpatialAxis.Coverage.Location.Coord.Position2D.Value2.C1',
    'RA of central coordinates', 'pos.eq.ra', 'deg', 'double',NULL,NULL, 1,0,1,40),
( 'ivoa.ObsCore', 's_dec', 'obscore:Char.SpatialAxis.Coverage.Location.Coord.Position2D.Value2.C2',
    'DEC of central coordinates', 'pos.eq.dec', 'deg', 'double',NULL,NULL, 1,0,1,41),
( 'ivoa.ObsCore', 's_fov', 'obscore:Char.SpatialAxis.Coverage.Bounds.Extent.diameter',
    'size of the region covered', 'phys.angSize;instr.fov', 'deg', 'double',NULL,NULL, 1,0,1,42),
( 'ivoa.ObsCore', 's_fov_min', 'obscore:Char.SpatialAxis.Coverage.Bounds.Extent.LoLim',
    'min size of the region covered (e.g. at all energies)', 'phys.angSize;instr.fov;stat.min', 'deg', 'double',NULL,NULL, 1,0,1,43),
( 'ivoa.ObsCore', 's_region', 'obscore:Char.SpatialAxis.Coverage.Support.Area',
    'region bounded by observation', 'pos.outline;obs.field', NULL, 'char','*','adql:REGION', 1,1,1,44),
( 'ivoa.ObsCore', 's_resolution', 'obscore:Char.SpatialAxis.Resolution.refval.value',
    'typical spatial resolution', 'pos.angResolution', 'arcsec', 'double',NULL,NULL, 1,0,1,45),
( 'ivoa.ObsCore', 's_resolution_min', '???',
    'smallest spatial resolution', 'pos.angResolution;stat.min', 'arcsec', 'double',NULL,NULL, 1,0,1,46),
( 'ivoa.ObsCore', 's_resolution_max', '???',
    'largest spatial resolution', 'pos.angResolution;stat.max', 'arcsec', 'double',NULL,NULL, 1,0,1,47),
( 'ivoa.ObsCore', 's_xel1', 'obscore:Char.SpatialAxis.numBins1', 
    'dimensions (number of pixels) along one spatial axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,48),
( 'ivoa.ObsCore', 's_xel2', 'obscore:Char.SpatialAxis.numBins2', 
    'dimensions (number of pixels) along the other spatial axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,49);

insert into tap_schema.columns11 
    (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'ivoa.ObsCore', 'em_min', 'obscore:Char.SpectralAxis.Coverage.Bounds.Limits.LoLimit',
    'start spectral coordinate value', 'em.wl;stat.min', 'm', 'double',NULL,NULL, 1,1,1,50),
( 'ivoa.ObsCore', 'em_max', 'obscore:Char.SpectralAxis.Coverage.Bounds.Limits.HiLimit',
    'stop spectral coordinate value', 'em.wl;stat.max', 'm', 'double',NULL,NULL, 1,1,1,51),
( 'ivoa.ObsCore', 'em_res_power', 'obscore:Char.SpectralAxis.Resolution.ResolPower.refval',
    'typical spectral resolution', 'spect.resolution', NULL, 'double',NULL,NULL, 1,0,1,52),
( 'ivoa.ObsCore', 'em_resolution', '???',
    'typical spectral resolution', 'spect.resolution', NULL, 'double',NULL,NULL, 1,0,1,53),
( 'ivoa.ObsCore', 'em_resolution_min', '???',
    'minimum spectral resolution', 'spect.resolution;stat.min', NULL, 'double',NULL,NULL, 1,0,1,54),
( 'ivoa.ObsCore', 'em_resolution_max', '???',
    'maximum spectral resolution', 'spect.resolution;stat.max', NULL, 'double',NULL,NULL, 1,0,1,55),
( 'ivoa.ObsCore', 'em_xel', 'obscore:Char.SpectralAxis.numBins', 
    'dimensions (number of pixels) along the energy axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,56),
( 'ivoa.ObsCore', 'em_ucd', 'obscore:Char.SpectralAxis.ucd',
    'UCD describing the spectral axis', 'meta.ucd', NULL, 'char','32*',NULL, 1,0,1,57 );

insert into tap_schema.columns11 
    (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'ivoa.ObsCore', 't_min', 'obscore:Char.TimeAxis.Coverage.Bounds.Limits.StartTime',
    'start time of observation (MJD)', 'time.start;obs.exposure', 'd', 'double',NULL,NULL, 1,1,1,60),
( 'ivoa.ObsCore', 't_max', 'obscore:Char.TimeAxis.Coverage.Bounds.Limits.StopTime',
    'end time of observation (MJD)', 'time.end;obs.exposure', 'd', 'double',NULL,NULL, 1,1,1,61),
( 'ivoa.ObsCore', 't_exptime', 'obscore:Char.TimeAxis.Coverage.Support.Extent',
    'typical exposure time', 'time.duration;obs.exposure', 's', 'double',NULL,NULL, 1,1,1,62),
( 'ivoa.ObsCore', 't_exptime_min', '???',
    'minimum exposure time', 'time.duration;obs.exposure;stat.min', 's', 'double',NULL,NULL, 1,1,1,63),
( 'ivoa.ObsCore', 't_exptime_max', '???',
    'maximum exposure time', 'time.duration;obs.exposure;stat.max', 's', 'double',NULL,NULL, 1,1,1,64),
( 'ivoa.ObsCore', 't_resolution', 'obscore:Char.TimeAxis.Resolution.refval.value',
    'typical temporal resolution', 'time.resolution', 's', 'double',NULL,NULL, 1,0,1,65),
( 'ivoa.ObsCore', 't_xel', 'obscore:Char.TimeAxis.numBins', 
    'dimensions (number of pixels) along the time axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,66);

insert into tap_schema.columns11 
    (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'ivoa.ObsCore', 'pol_states', 'obscore:Char.PolarizationAxis.stateList',
    'polarization states present in the data', 'meta.code;phys.polarization', NULL, 'char','32*',NULL, 1,0,1,70),
( 'ivoa.ObsCore', 'pol_xel', 'obscore:Char.PolarizationAxis.numBins', 
    'dimensions (number of pixels) along the polarization axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,71),
( 'ivoa.ObsCore', 'o_ucd', 'obscore:Char.ObservableAxis.ucd',
    'UCD describing the observable axis (pixel values)', 'meta.ucd', NULL, 'char','32*',NULL, 1,0,1,80),
( 'ivoa.ObsCore', 'lastModified', 'caom2:Observation.maxLastModified',
    'timestamp of last modification of the metadata', NULL, NULL, 'char','23*','timestamp', 0,1,0,90);

insert into tap_schema.columns11 (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'ivoa.ObsCore_radio', 'obs_publisher_did', NULL, 'natural join key', NULL, NULL, 'char', '256*','uri', 1,1,1,1),
( 'ivoa.ObsCore_radio', 'uv_distance_min', '???',
    'minimum distance in the UV plane', 'stat.fourier;pos;stat.min', NULL, 'double',NULL,NULL, 1,0,1,2),
( 'ivoa.ObsCore_radio', 'uv_distance_max', '???',
    'maximum distance in the UV plane', 'stat.fourier;pos;stat.max', NULL, 'double',NULL,NULL, 1,0,1,3),
( 'ivoa.ObsCore_radio', 'uv_distribution_ecc', '???',
    'eccentricity of the UV distribution', 'stat.fourier;pos', NULL, 'double',NULL,NULL, 1,0,1,4),
( 'ivoa.ObsCore_radio', 'uv_distribution_fill', '???',
    'filling factor of UV distribution', 'stat.fourier;pos;arith.ratio', NULL, 'double',NULL,NULL, 1,0,1,5);

insert into tap_schema.keys11 (key_id,from_table,target_table,description) values
('ivoa-core-radio', 'ivoa.ObsCore_radio', 'ivoa.ObsCore','standard way to join ivoa.ObsCore and ivoa.ObsCore_radio');

insert into tap_schema.key_columns11 (key_id,from_column,target_column) values
('ivoa-core-radio', 'obs_publisher_id', 'obs_publisher_id');

-- backwards compatible: fill "size" column with values from arraysize set above
-- where arraysize is a possibly variable-length 1-dimensional value
update tap_schema.columns11 SET "size" = replace(arraysize::varchar,'*','')::int 
WHERE table_name LIKE 'ivoa.%'
  AND arraysize IS NOT NULL
  AND arraysize NOT LIKE '%x%'
  AND arraysize != '*';
