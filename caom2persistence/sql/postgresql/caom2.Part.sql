
drop table if exists caom2.Part;

create table caom2.Part
(
    name varchar(1024) not null,
    productType varchar(64),
    
-- optimisation
    metaReadAccessGroups tsvector default '',

-- internal
    metaRelease timestamp,
    obsID uuid not null, -- change: UUID
    planeID uuid not null, -- change: UUID
    artifactID uuid not null references caom2.Artifact (artifactID), -- change: UUID
    partID uuid not null primary key using index tablespace caom_index, -- change: UUID
    lastModified timestamp not null,
    maxLastModified timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

-- this is for Artifact join Part
create index i_artifactID on caom2.Part (artifactID)
tablespace caom_index
;

-- tag the clustering index
cluster i_artifactID on caom2.Part
;

-- this is for asset updates
create index ip_planeID on caom2.Part (planeID)
tablespace caom_index
;

