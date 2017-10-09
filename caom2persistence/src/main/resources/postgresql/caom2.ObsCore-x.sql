
create or replace view <schema>.ObsFile
(
    uri,
    content_type,
    content_length,
    last_modified,

-- foreign key in CAOM
    core_id,
-- primary key
    file_id,

-- hidden columns (not in tap_schema) for CAOM access control
    planeID,
    metaRelease, metaReadAccessGroups
)
AS SELECT 
    a.uri,
    a.contentType,
    a.contentLength,
    a.lastModified,

-- foreign key in CAOM
    a.planeID,
-- primary key
    a.artifactID,

    a.planeID,
    a.metaRelease, a.metaReadAccessGroups
FROM <schema>.Artifact AS a
;

create or replace view <schema>.ObsPart
(
    name,
    naxis,
    s_axis1, s_axis2, 
    s_ctype1, s_ctype2, 
    s_cunit1, s_cunit2, 
    s_syser1, s_syser2, 
    s_rnder1, s_rnder2,
    s_naxis1, s_naxis2, 
    s_crpix1, s_crpix2, 
    s_crval1, s_crval2, 
    s_cd11, s_cd12, 
    s_cd21, s_cd22,
    s_coordsys, s_equinox,

    em_axis, 
    em_ctype, 
    em_cunit, 
    em_syser, 
    em_rnder,
    em_naxis, 
    em_crpix, 
    em_crval, 
    em_cdelt, 
    em_specsys, em_ssysobs, em_restfrq, em_restwav, em_velosys, em_zsource, em_velang, em_ssyssrc,
    
    t_axis, 
    t_ctype, 
    t_cunit, 
    t_syser, 
    t_rnder, 
    t_naxis, 
    t_crpix, 
    t_crval, 
    t_cdelt, 
    t_timesys, t_trefpos, t_mjdref,

    p_axis,
    p_ctype,
    p_cunit,
    p_syser,
    p_rnder,
    p_naxis,
    p_crpix,
    p_crval,
    p_cdelt,

    last_modified,
-- foreign key
    file_id,
-- primary key
    part_id,

-- hidden columns (not in tap_schema) for CAOM access control
    planeID,
    metaRelease, metaReadAccessGroups
)
AS SELECT 
    p.name,
    c.naxis,
    c.positionAxis1, c.positionAxis2, 
    c.position_axis_axis1_ctype, c.position_axis_axis2_ctype, 
    c.position_axis_axis1_cunit, c.position_axis_axis2_cunit, 
    c.position_axis_error1_syser, c.position_axis_error1_syser, 
    c.position_axis_error1_rnder, c.position_axis_error1_rnder, 
    c.position_axis_function_dimension_naxis1, c.position_axis_function_dimension_naxis2, 
    c.position_axis_function_refcoord_coord1_pix, c.position_axis_function_refcoord_coord2_pix, 
    c.position_axis_function_refcoord_coord1_val, c.position_axis_function_refcoord_coord2_val, 
    c.position_axis_function_cd11, c.position_axis_function_cd12, 
    c.position_axis_function_cd21, c.position_axis_function_cd22,
    c.position_coordsys, c.position_equinox,

    c.energyAxis, 
    c.energy_axis_axis_ctype, 
    c.energy_axis_axis_cunit, 
    c.energy_axis_error_syser, 
    c.energy_axis_error_rnder, 
    c.energy_axis_function_naxis, 
    c.energy_axis_function_refCoord_pix, 
    c.energy_axis_function_refCoord_val, 
    c.energy_axis_function_delta, 
    c.energy_specsys, c.energy_ssysobs, c.energy_restfrq, c.energy_restwav, c.energy_velosys, c.energy_zsource, c.energy_velang, c.energy_ssyssrc,

    c.timeAxis, 
    c.time_axis_axis_ctype, 
    c.time_axis_axis_cunit, 
    c.time_axis_error_syser, 
    c.time_axis_error_rnder, 
    c.time_axis_function_naxis, 
    c.time_axis_function_refCoord_pix, 
    c.time_axis_function_refCoord_val, 
    c.time_axis_function_delta, 
    c.time_timesys, c.time_trefpos, c.time_mjdref,

    c.polarizationAxis,
    c.polarization_axis_axis_ctype,
    c.polarization_axis_axis_cunit,
    c.polarization_axis_error_syser,
    c.polarization_axis_error_rnder,
    c.polarization_axis_function_naxis,
    c.polarization_axis_function_refCoord_pix,
    c.polarization_axis_function_refCoord_val,
    c.polarization_axis_function_delta,

    c.lastModified,

-- foreign key in CAOM
    p.artifactID,
-- primary key
    p.partID,

    p.planeID,
    p.metaRelease, p.metaReadAccessGroups
FROM <schema>.Part AS p JOIN <schema>.Chunk AS c ON p.partID = c.partID
;
