
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
('ivoa', 'tables and views defined by the IVOA, including prototypes!')
;

-- ivoa.ObsCore view
-- index start at 10
insert into tap_schema.tables11 (schema_name,table_name,table_type,description,table_index) values
( 'ivoa', 'ivoa.ObsCore', 'view', 'ObsCore-1.1 observation table', 10 ),
( 'ivoa', 'ivoa.ObsFile', 'view', 'ObsCore-1.x file table', 11 ),
( 'ivoa', 'ivoa.ObsPart', 'view', 'ObsCore-1.x part table', 12 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index,id) values
( 'ivoa.ObsCore', 'obs_publisher_did', 	'obscore:Curation.PublisherDID',
    'publisher dataset identifier', 'meta.ref.url;meta.curation', NULL, 'char', '256*',NULL, 1,1,1,1, 'caomPlaneURI')
;
insert into tap_schema.columns11 (table_name,column_name,utype,description,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'ivoa.ObsCore', 'obs_collection', 	'obscore:DataID.Collection',
    'short name for the data colection', 'meta.id', NULL, 'char','128*',NULL, 1,0,1,2 ),
( 'ivoa.ObsCore', 'facility_name', 	'obscore:Provenance.ObsConfig.Facility.name',
    'telescope name', 'meta.id;instr.tel', NULL, 'char','128*',NULL, 1,0,1,3 ),
( 'ivoa.ObsCore', 'instrument_name', 	'obscore:Provenance.ObsConfig.Instrument.name',
    'instrument name', 'meta.id;instr', NULL, 'char','128*',NULL, 1,0,1,4 ),

( 'ivoa.ObsCore', 'obs_id', 	'obscore:DataID.observationID',
    'internal dataset identifier', 'meta.id', NULL, 'char','128*',NULL, 1,0,1,5 ),
    
( 'ivoa.ObsCore', 'dataproduct_type', 	'obscore:ObsDataset.dataProductType',
    'type of product', 'meta.id', NULL, 'char', '128*',NULL, 1,0,1,6 ),
( 'ivoa.ObsCore', 'calib_level', 	'obscore:ObsDataset.calibLevel',
    'calibration level (0,1,2,3)', 'meta.code;obs.calib', NULL, 'int',NULL,NULL, 1,0,1,7 ),

( 'ivoa.ObsCore', 'obs_release_date', 'obscore:Curation.releaseDate',
    'timestamp of date the data becomes publicly available', 'time.release', NULL, 'char','23*','timestamp', 0,0,1,8 ),

( 'ivoa.ObsCore', 'target_name', 'obscore:Target.Name',
    'name of intended target', 'meta.id;src', NULL, 'char', '32*',NULL, 1,0,1,9 ),

( 'ivoa.ObsCore', 's_ra', 'obscore:Char.SpatialAxis.Coverage.Location.Coord.Position2D.Value2.C1',
    'RA of central coordinates', 'pos.eq.ra', 'deg', 'double',NULL,NULL, 1,0,1,10 ),
( 'ivoa.ObsCore', 's_dec', 'obscore:Char.SpatialAxis.Coverage.Location.Coord.Position2D.Value2.C2',
    'DEC of central coordinates', 'pos.eq.dec', 'deg', 'double',NULL,NULL, 1,0,1,11 ),
( 'ivoa.ObsCore', 's_fov', 'obscore:Char.SpatialAxis.Coverage.Bounds.Extent.diameter',
    'size of the region covered (~diameter of minimum bounding circle)', 'phys.angSize;instr.fov', 'deg', 'double',NULL,NULL, 1,0,1,12 ),
( 'ivoa.ObsCore', 's_region', 'obscore:Char.SpatialAxis.Coverage.Support.Area',
    'region bounded by observation', 'pos.outline;obs.field', 'deg', 'char','*','adql:REGION', 1,1,1,13 ),
( 'ivoa.ObsCore', 's_resolution', 'obscore:Char.SpatialAxis.Resolution.refval.value',
    'typical spatial resolution', 'pos.angResolution', 'arcsec', 'double',NULL,NULL, 1,0,1,14 ),
( 'ivoa.ObsCore', 's_xel1', 'obscore:Char.SpatialAxis.numBins1', 
    'dimensions (number of pixels) along one spatial axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,15 ),
( 'ivoa.ObsCore', 's_xel2', 'obscore:Char.SpatialAxis.numBins2', 
    'dimensions (number of pixels) along the other spatial axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,16 ),

( 'ivoa.ObsCore', 't_min', 'obscore:Char.TimeAxis.Coverage.Bounds.Limits.StartTime',
    'start time of observation (MJD)', 'time.start;obs.exposure', 'd', 'double',NULL,NULL, 1,1,1,17 ),
( 'ivoa.ObsCore', 't_max', 'obscore:Char.TimeAxis.Coverage.Bounds.Limits.StopTime',
    'end time of observation (MJD)', 'time.end;obs.exposure', 'd', 'double',NULL,NULL, 1,1,1,18 ),
( 'ivoa.ObsCore', 't_exptime', 'obscore:Char.TimeAxis.Coverage.Support.Extent',
    'exposure time of observation', 'time.duration;obs.exposure', 's', 'double',NULL,NULL, 1,1,1,19 ),
( 'ivoa.ObsCore', 't_resolution', 'obscore:Char.TimeAxis.Resolution.refval.value',
    'typical temporal resolution', 'time.resolution', 's', 'double',NULL,NULL, 1,0,1,20 ),
( 'ivoa.ObsCore', 't_xel', 'obscore:Char.TimeAxis.numBins', 
    'dimensions (number of pixels) along the time axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,21 ),

( 'ivoa.ObsCore', 'em_min', 'obscore:Char.SpectralAxis.Coverage.Bounds.Limits.LoLimit',
    'start spectral coordinate value', 'em.wl;stat.min', 'm', 'double',NULL,NULL, 1,1,1,22 ),
( 'ivoa.ObsCore', 'em_max', 'obscore:Char.SpectralAxis.Coverage.Bounds.Limits.HiLimit',
    'stop spectral coordinate value', 'em.wl;stat.max', 'm', 'double',NULL,NULL, 1,1,1,23 ),
( 'ivoa.ObsCore', 'em_res_power', 'obscore:Char.SpectralAxis.Resolution.ResolPower.refval',
    'typical spectral resolution', 'spect.resolution', NULL, 'double',NULL,NULL, 1,0,1,24 ),
( 'ivoa.ObsCore', 'em_xel', 'obscore:Char.SpectralAxis.numBins', 
    'dimensions (number of pixels) along the energy axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,25 ),
( 'ivoa.ObsCore', 'em_ucd', 'obscore:Char.SpectralAxis.ucd',
    'UCD describing the spectral axis', 'meta.ucd', NULL, 'char','32*',NULL, 1,0,1,26 ),

( 'ivoa.ObsCore', 'pol_states', 'obscore:Char.PolarizationAxis.stateList',
    'polarization states present in the data', 'meta.code;phys.polarization', NULL, 'char','32*',NULL, 1,0,1,27 ),
( 'ivoa.ObsCore', 'pol_xel', 'obscore:Char.PolarizationAxis.numBins', 
    'dimensions (number of pixels) along the polarization axis', 'meta.number', NULL, 'long',NULL,NULL, 1,0,1,28 ),

( 'ivoa.ObsCore', 'o_ucd', 'obscore:Char.ObservableAxis.ucd',
    'UCD describing the observable axis (pixel values)', 'meta.ucd', NULL, 'char','32*',NULL, 1,0,1,29 ),

( 'ivoa.ObsCore', 'access_url', 'obscore:Access.Reference',
    'URL to download the data', 'meta.ref.url', NULL, 'char', '*','clob', 1,0,1,30 ),
( 'ivoa.ObsCore', 'access_format', 'obscore:Access.Format',
    'format of the data file(s)', 'meta.code.mime', NULL, 'char','128*',NULL, 1,0,1,31 ),
( 'ivoa.ObsCore', 'access_estsize', 'obscore:Access.Size',
    'estimated size of the download', 'phys.size;meta.file', 'kbyte', 'long',NULL,NULL, 1,0,1,32 ),
    
( 'ivoa.ObsCore', 'core_id', NULL, 'primary key', NULL, NULL, 'char','36','uuid', 0,1,0,33 ),

( 'ivoa.ObsCore', 'lastModified', NULL,
    'timestamp of last modification of the metadata', NULL, NULL, 'char','23*','timestamp', 0,1,0,34 )
;

insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'ivoa.ObsFile', 'uri', 'URI for this file', 'caom2:Artifact.uri', NULL, NULL, 'char','128*',NULL, 1,1,1,1 ),
( 'ivoa.ObsFile', 'content_type', 'format of the data file', 'caom2:Artifact:contentType', 'meta.id;class', NULL, 'char','128*',NULL, 1,0,1,2 ),
( 'ivoa.ObsFile', 'content_length', 'size of the download', 'caom2:Artifact:contentLength', 'phys.size;meta.file', 'byte', 'long',NULL,NULL, 1,0,1,3 ),
( 'ivoa.ObsFile', 'last_modified', 'timestamp of last modification of the metadata', 'caom2:Artifact.lastModified', NULL, NULL, 'char','23*','timestamp', 0,1,0,4 ),
( 'ivoa.ObsFile', 'core_id', 'foreign key', NULL, NULL, NULL, 'char','36','uuid', 0,1,0,5 ),
( 'ivoa.ObsFile', 'file_id', 'primary key', NULL, NULL, NULL, 'char','36','uuid', 0,1,0,6 )
;

insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'ivoa.ObsPart', 'name', 'name of this part (e.g. FITS extension name or number)', 'caom2:Part.name',  NULL, NULL, 'char','128*',NULL, 1,1,1,1 ),
( 'ivoa.ObsPart', 'naxis', 'size of the representation at uri', 'caom2:Chunk.naxis',                    NULL, NULL, 'int',NULL,NULL, 1,0,0,2 ),
( 'ivoa.ObsPart', 's_axis1', 'axis for the first position coordinate', 'caom2:Chunk.positionAxis1',     NULL, NULL, 'int',NULL,NULL, 1,0,0,3 ),
( 'ivoa.ObsPart', 's_axis2', 'axis for the second position coordinate', 'caom2:Chunk.positionAxis2',    NULL, NULL, 'int',NULL,NULL, 1,0,0,4 ),
( 'ivoa.ObsPart', 'em_axis', 'axis for the energy coordinate', 'caom2:Chunk.energyAxis',                NULL, NULL, 'int',NULL,NULL, 1,0,0,5 ),
( 'ivoa.ObsPart', 't_axis', 'axis for the time coordinate', 'caom2:Chunk.timeAxis',                     NULL, NULL, 'int',NULL,NULL, 1,0,0,6 ),
( 'ivoa.ObsPart', 'p_axis', 'axis for the polarization coordinate', 'caom2:Chunk.polarizationAxis',     NULL, NULL, 'int',NULL,NULL, 1,0,0,7 ),

-- index start at 20
( 'ivoa.ObsPart', 's_coordsys', 'coordinate system name', 'caom2:Chunk.position.coordsys',              NULL, NULL, 'char','8*',NULL, 1,0,0,20 ),
( 'ivoa.ObsPart', 's_equinox', 'coordinate equinox', 'caom2:Chunk.position.equinox',                    NULL, 'a', 'double',NULL,NULL, 1,0,0,21 ),

( 'ivoa.ObsPart', 's_ctype1', 'coordinate type for first position axis', 'caom2:Chunk.position.axis.axis1.ctype', NULL, NULL, 'char','8*',NULL, 1,0,0,22 ),
( 'ivoa.ObsPart', 's_ctype2', 'coordinate type for second position axis', 'caom2:Chunk.position.axis.axis2.ctype', NULL, NULL, 'char','8*',NULL, 1,0,0,23 ),
( 'ivoa.ObsPart', 's_cunit1', 'coordinate unit for first position axis', 'caom2:Chunk.position.axis.axis1.cunit', NULL, NULL, 'char','8*',NULL, 1,0,0,24 ),
( 'ivoa.ObsPart', 's_cunit2', 'coordinate unit for second position axis', 'caom2:Chunk.position.axis.axis2.cunit', NULL, NULL, 'char','8*',NULL, 1,0,0,25 ),
( 'ivoa.ObsPart', 's_syser1', 'systematic coordinate error for first position axis', 'caom2:Chunk.position.axis.error1.syser', NULL, NULL, 'double',NULL,NULL, 1,0,0,26 ),
( 'ivoa.ObsPart', 's_syser2', 'systematic coordinate error for second position axis', 'caom2:Chunk.position.axis.error2.syser', NULL, NULL, 'double',NULL,NULL, 1,0,0,27 ),
( 'ivoa.ObsPart', 's_rnder1', 'random coordinate error for first position axis', 'caom2:Chunk.position.axis.error1.rnder', NULL, NULL, 'double',NULL,NULL, 1,0,0,28 ),
( 'ivoa.ObsPart', 's_rnder2', 'random coordinate error for second position axis', 'caom2:Chunk.position.axis.error2.rnder', NULL, NULL, 'double',NULL,NULL, 1,0,0,29 ),
( 'ivoa.ObsPart', 's_naxis1', 'number of pixels on first position axis', 'caom2:Chunk.position.axis.function.dimension.naxis1', NULL, NULL, 'long',NULL,NULL, 1,0,0,30 ),
( 'ivoa.ObsPart', 's_naxis2', 'number of pixels on second for position axis', 'caom2:Chunk.position.axis.function.dimension.naxis2', NULL, NULL, 'long',NULL,NULL, 1,0,0,31 ),
( 'ivoa.ObsPart', 's_crpix1', 'reference pixel on first position axis', 'caom2:Chunk.position.axis.function.refCoord.coord1.pix', NULL, NULL, 'double',NULL,NULL, 1,0,0,32 ),
( 'ivoa.ObsPart', 's_crpix2', 'reference pixel on second position axis', 'caom2:Chunk.position.axis.function.refCoord.coord1.val', NULL, NULL, 'double',NULL,NULL, 1,0,0,33 ),
( 'ivoa.ObsPart', 's_crval1', 'reference value on first position axis', 'caom2:Chunk.position.axis.function.refCoord.coord2.pix', NULL, NULL, 'double',NULL,NULL, 1,0,0,34 ),
( 'ivoa.ObsPart', 's_crval2', 'reference value on second position axis', 'caom2:Chunk.position.axis.function.refCoord.coord2.val', NULL, NULL, 'double',NULL,NULL, 1,0,0,35 ),
( 'ivoa.ObsPart', 's_cd11', 'CD[1,1] for position axis', 'caom2:Chunk.position.axis.function.cd11', NULL, NULL, 'double',NULL,NULL, 1,0,0,36 ),
( 'ivoa.ObsPart', 's_cd12', 'CD[1,2] for position axis', 'caom2:Chunk.position.axis.function.cd12', NULL, NULL, 'double',NULL,NULL, 1,0,0,37 ),
( 'ivoa.ObsPart', 's_cd21', 'CD[2,1] for position axis', 'caom2:Chunk.position.axis.function.cd21', NULL, NULL, 'double',NULL,NULL, 1,0,0,38 ),
( 'ivoa.ObsPart', 's_cd22', 'CD[2,2] for position axis', 'caom2:Chunk.position.axis.function.cd22', NULL, NULL, 'double',NULL,NULL, 1,0,0,39 ),

-- index start at 50
( 'ivoa.ObsPart', 'em_ctype', 'coordinate type for energy axis', 'caom2:Chunk.energy.axis.axis.ctype', NULL, NULL, 'char','8*',NULL, 1,0,0,50 ),
( 'ivoa.ObsPart', 'em_cunit', 'coordinate unit for energy axis', 'caom2:Chunk.energy.axis.axis.cunit', NULL, NULL, 'char','8*',NULL, 1,0,0,51 ),
( 'ivoa.ObsPart', 'em_syser', 'systematic coordinate error for energy axis', 'caom2:Chunk.energy.axis.error.syser', NULL, NULL, 'double',NULL,NULL, 1,0,0,52 ),
( 'ivoa.ObsPart', 'em_rnder', 'random coordinate error for energy axis', 'caom2:Chunk.energy.axis.error.rnder', NULL, NULL, 'double',NULL,NULL, 1,0,0,53 ),
( 'ivoa.ObsPart', 'em_naxis', 'number of pixels on energy axis', 'caom2:Chunk.energy.axis.function.naxis', NULL, NULL, 'long',NULL,NULL, 1,0,0,54 ),
( 'ivoa.ObsPart', 'em_crpix', 'reference pixel on energy axis', 'caom2:Chunk.energy.axis.function.refCoord.pix', NULL, NULL, 'double',NULL,NULL, 1,0,0,55 ),
( 'ivoa.ObsPart', 'em_crval', 'reference value on energy axis', 'caom2:Chunk.energy.axis.function.refCoord.val', NULL, NULL, 'double',NULL,NULL, 1,0,0,56 ),
( 'ivoa.ObsPart', 'em_cdelt', 'delta-value on energy axis', 'caom2:Chunk.energy.axis.function.delta', NULL, NULL, 'double',NULL,NULL, 1,0,0,57 ),
( 'ivoa.ObsPart', 'em_specsys', 'SPECSYS', 'caom2:Chunk.energy.specsys', NULL, NULL, 'char','8*',NULL, 1,0,0,58 ),
( 'ivoa.ObsPart', 'em_ssysobs', 'SSYSOBS', 'caom2:Chunk.energy.ssysobs', NULL, NULL, 'char','8*',NULL, 1,0,0,59 ),
( 'ivoa.ObsPart', 'em_ssyssrc', 'SSYSSRC', 'caom2:Chunk.energy.ssyssrc', NULL, NULL, 'char','8*',NULL, 1,0,0,60 ),
( 'ivoa.ObsPart', 'em_restfrq', 'RESTFRQ', 'caom2:Chunk.energy.restfrq', NULL, NULL, 'double',NULL,NULL, 1,0,0,61 ),
( 'ivoa.ObsPart', 'em_restwav', 'RESTWAV', 'caom2:Chunk.energy.restwav',  NULL, NULL, 'double',NULL,NULL, 1,0,0,62 ),
( 'ivoa.ObsPart', 'em_velosys', 'VELOSYS', 'caom2:Chunk.energy.velosys',  NULL, NULL, 'double',NULL,NULL, 1,0,0,63 ),
( 'ivoa.ObsPart', 'em_zsource', 'ZSOURCE', 'caom2:Chunk.energy.zsource',  NULL, NULL, 'double',NULL,NULL, 1,0,0,64 ),
( 'ivoa.ObsPart', 'em_velang', 'VELANG', 'caom2:Chunk.energy.velang', NULL,  NULL, 'double',NULL,NULL, 1,0,0,65 ),

-- index start at 80
( 'ivoa.ObsPart', 't_ctype', 'coordinate type for time axis', 'caom2:Chunk.time.axis.axis.ctype', NULL, NULL, 'char','8*',NULL, 1,0,0,80 ),
( 'ivoa.ObsPart', 't_cunit', 'coordinate unit for time axis', 'caom2:Chunk.time.axis.axis.cunit', NULL, NULL, 'char','8*',NULL, 1,0,0,81 ),
( 'ivoa.ObsPart', 't_syser', 'systematic coordinate error for time axis', 'caom2:Chunk.time.axis.error.syser', NULL, NULL, 'double',NULL,NULL, 1,0,0,82 ),
( 'ivoa.ObsPart', 't_rnder', 'random coordinate error for time axis', 'caom2:Chunk.time.axis.error.rnder', NULL, NULL, 'double',NULL,NULL, 1,0,0,83 ),
( 'ivoa.ObsPart', 't_naxis', 'number of pixels on time axis', 'caom2:Chunk.time.axis.function.naxis', NULL, NULL, 'long',NULL,NULL, 1,0,0,84 ),
( 'ivoa.ObsPart', 't_crpix', 'reference pixel on time axis', 'caom2:Chunk.time.axis.function.refCoord.pix', NULL, NULL, 'double',NULL,NULL, 1,0,0,85 ),
( 'ivoa.ObsPart', 't_crval', 'reference value on time axis', 'caom2:Chunk.time.axis.function.refCoord.val', NULL, NULL, 'double',NULL,NULL, 1,0,0,86 ),
( 'ivoa.ObsPart', 't_cdelt', 'delta-value on time axis', 'caom2:Chunk.time.axis.function.delta', NULL, NULL, 'double',NULL,NULL, 1,0,0,87 ),
( 'ivoa.ObsPart', 't_timesys', 'TIMESYS', 'caom2:Chunk.time.timesys', NULL, NULL, 'char','8*',NULL, 1,0,0,88 ),
( 'ivoa.ObsPart', 't_trefpos', 'TREFPOS', 'caom2:Chunk.time.trefpos', NULL, NULL, 'char','8*',NULL, 1,0,0,89 ),
( 'ivoa.ObsPart', 't_mjdref',  'MJDREF', 'caom2:Chunk.time.mjdref', NULL, 'd', 'double',NULL,NULL, 1,0,0,90 ),

-- index start at 110
( 'ivoa.ObsPart', 'p_ctype', 'coordinate type for polarization axis', 'caom2:Chunk.polarization.axis.axis.ctype', NULL, NULL, 'char','8*',NULL, 1,0,0,110 ),
( 'ivoa.ObsPart', 'p_cunit', 'coordinate unit for polarization axis', 'caom2:Chunk.polarization.axis.axis.cunit', NULL, NULL, 'char','8*',NULL, 1,0,0,111 ),
( 'ivoa.ObsPart', 'p_syser', 'systematic coordinate error for polarization axis', 'caom2:Chunk.polarization.axis.error.syser', NULL, NULL, 'double',NULL,NULL, 1,0,0,112 ),
( 'ivoa.ObsPart', 'p_rnder', 'random coordinate error for polarization axis', 'caom2:Chunk.polarization.axis.error.rnder', NULL, NULL, 'double',NULL,NULL, 1,0,0,113 ),
( 'ivoa.ObsPart', 'p_naxis', 'number of pixels on polarization axis', 'caom2:Chunk.polarization.axis.function.naxis', NULL, NULL, 'long',NULL,NULL, 1,0,0,114 ),
( 'ivoa.ObsPart', 'p_crpix', 'reference pixel on polarization axis', 'caom2:Chunk.polarization.axis.function.refCoord.pix', NULL, NULL, 'double',NULL,NULL, 1,0,0,115 ),
( 'ivoa.ObsPart', 'p_crval', 'reference value on polarization axis', 'caom2:Chunk.polarization.axis.function.refCoord.val', NULL, NULL, 'double',NULL,NULL, 1,0,0,116 ),
( 'ivoa.ObsPart', 'p_cdelt', 'delta-value on polarization axis', 'caom2:Chunk.polarization.axis.function.delta', NULL, NULL, 'double',NULL,NULL, 1,0,0,117 ),

-- index start at 200
( 'ivoa.ObsPart', 'last_modified', 'timestamp of last modification of the metadata', 'caom2:Part.lastModified', NULL, NULL, 'char', '23*','timestamp', 0,0,0,200 ),
( 'ivoa.ObsPart', 'file_id', 'foreign key', NULL, NULL, NULL, 'char','36','uuid', 0,1,0,201 ),
( 'ivoa.ObsPart', 'part_id', 'primary key', NULL, NULL, NULL, 'char','36','uuid', 0,1,0,202 )
;


insert into tap_schema.keys11 (key_id,from_table,target_table,description) values
('ivoa-o-f', 'ivoa.ObsFile', 'ivoa.ObsCore','standard way to join the ivoa.ObsCore and ivoa.ObsFile tables'),
('ivoa-f-p', 'ivoa.ObsPart', 'ivoa.ObsFile', 'standard way to join the ivoa.ObsFile and ivoa.ObsPart tables')
;

insert into tap_schema.key_columns11 (key_id,from_column,target_column) values
('ivoa-o-f', 'core_id', 'core_id'),
('ivoa-f-p', 'file_id', 'file_id')
;

-- backwards compatible: fill "size" column with values from arraysize set above
-- where arraysize is a possibly variable-length 1-dimensional value
update tap_schema.columns11 SET "size" = replace(arraysize::varchar,'*','')::int 
WHERE table_name LIKE 'ivoa.%'
  AND arraysize IS NOT NULL
  AND arraysize NOT LIKE '%x%'
  AND arraysize != '*';
