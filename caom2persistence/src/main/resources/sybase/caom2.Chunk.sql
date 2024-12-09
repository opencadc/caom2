
create table caom2_Chunk
(
    productType varchar(64) null,
    naxis int null,
    positionAxis1 int null,
    positionAxis2 int null,
    energyAxis int null,
    timeAxis int null,
    polarizationAxis int null,
    observableAxis int null,

    position_coordsys varchar(16) null,
    position_equinox double precision null,
    position_resolution double precision null,

    position_axis_axis1_ctype varchar(16) null,
    position_axis_axis1_cunit varchar(16) null,
    position_axis_axis2_ctype varchar(16) null,
    position_axis_axis2_cunit varchar(16) null,
    position_axis_error1_syser double precision null,
    position_axis_error1_rnder double precision null,
    position_axis_error2_syser double precision null,
    position_axis_error2_rnder double precision null,
--    position_axis_range text null,
    position_axis_range_start_coord1_pix double precision null,
    position_axis_range_start_coord1_val double precision null,
    position_axis_range_start_coord2_pix double precision null,
    position_axis_range_start_coord2_val double precision null,
    position_axis_range_end_coord1_pix double precision null,
    position_axis_range_end_coord1_val double precision null,
    position_axis_range_end_coord2_pix double precision null,
    position_axis_range_end_coord2_val double precision null,
    position_axis_bounds text null,
--    position_axis_function text null,
    position_axis_function_dimension_naxis1 bigint null,
    position_axis_function_dimension_naxis2 bigint null,
    position_axis_function_refcoord_coord1_pix double precision null,
    position_axis_function_refCoord_coord1_val double precision null,
    position_axis_function_refCoord_coord2_pix double precision null,
    position_axis_function_refCoord_coord2_val double precision null,
    position_axis_function_cd11 double precision null,
    position_axis_function_cd12 double precision null,
    position_axis_function_cd21 double precision null,
    position_axis_function_cd22 double precision null,


    energy_specsys varchar(16) null,
    energy_ssysobs varchar(16) null,
    energy_restfrq double precision null,
    energy_restwav double precision null,
    energy_velosys double precision null,
    energy_zsource double precision null,
    energy_velang double precision null,
    energy_ssyssrc varchar(16) null,
    energy_bandpassName varchar(64) null,
    energy_resolvingPower double precision null,
    energy_transition_species varchar(32) null,
    energy_transition_transition varchar(32) null,

    energy_axis_axis_ctype varchar(16) null,
    energy_axis_axis_cunit varchar(16) null,
    energy_axis_error_syser double precision null,
    energy_axis_error_rnder double precision null,
--    energy_axis_range text null,
    energy_axis_range_start_pix double precision null,
    energy_axis_range_start_val double precision null,
    energy_axis_range_end_pix double precision null,
    energy_axis_range_end_val double precision null,
    energy_axis_bounds text null,
--    energy_axis_function text null,
    energy_axis_function_naxis bigint null,
    energy_axis_function_refCoord_pix double precision null,
    energy_axis_function_refCoord_val double precision null,
    energy_axis_function_delta double precision null,

    time_timesys varchar(16) null,
    time_trefpos varchar(16) null,
    time_mjdref double precision null,
    time_exposure double precision null,
    time_resolution double precision null,
    time_axis_axis_ctype varchar(16) null,
    time_axis_axis_cunit varchar(16) null,
    time_axis_error_syser double precision null,
    time_axis_error_rnder double precision null,
--    time_axis_range text null,
    time_axis_range_start_pix double precision null,
    time_axis_range_start_val double precision null,
    time_axis_range_end_pix double precision null,
    time_axis_range_end_val double precision null,
    time_axis_bounds text null,
--    time_axis_function text null,
    time_axis_function_naxis bigint null,
    time_axis_function_refCoord_pix double precision null,
    time_axis_function_refCoord_val double precision null,
    time_axis_function_delta double precision null,

    polarization_axis_axis_ctype varchar(16) null,
    polarization_axis_axis_cunit varchar(16) null,
    polarization_axis_error_syser double precision null,
    polarization_axis_error_rnder double precision null,
--    polarization_axis_range text null,
    polarization_axis_range_start_pix double precision null,
    polarization_axis_range_start_val double precision null,
    polarization_axis_range_end_pix double precision null,
    polarization_axis_range_end_val double precision null,
    polarization_axis_bounds text null,
--    polarization_axis_function text null,
    polarization_axis_function_naxis bigint null,
    polarization_axis_function_refCoord_pix double precision null,
    polarization_axis_function_refCoord_val double precision null,
    polarization_axis_function_delta double precision null,

    observable_dependent_axis_ctype varchar(64) null,
    observable_dependent_axis_cunit varchar(64) null,
    observable_dependent_bin bigint null,
    observable_independent_axis_ctype varchar(64) null,
    observable_independent_axis_cunit varchar(64) null,
    observable_independent_bin bigint null,

-- internal
    obsID bigint not null,
    planeID bigint not null,
    artifactID bigint not null,
    metaRelease datetime null,
    partID bigint not null references caom2_Part (partID),
    chunkID bigint not null primary key nonclustered,
    lastModified datetime not null,
    maxLastModified datetime not null,
    metaChecksum varchar(36),
    accMetaChecksum varchar(36)
)
lock datarows
;

create clustered index part2chunk on caom2_Chunk (partID)
;

create index lastModified on caom2_Chunk (lastModified)
;
