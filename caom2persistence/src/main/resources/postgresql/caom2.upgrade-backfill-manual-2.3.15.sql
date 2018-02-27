
-- to modify this file with a specific <schema>, try:
-- cat caom2.upgrade-backfill-manual-2.3.15.sql | sed 's/<schema>/caom2/g' > doit.sql

-- this back fill should be run after upgrading the database.schema by
-- using caom2persistence-2.3.15

-- back fill the ObservationMember table
insert into <schema>.ObservationMember (compositeID,simpleID)
    select o.obsID, t.uri 
    from <schema>.Observation o, unnest(string_to_array(o.members, ' ')) t(uri)
    where o.members is not null
ON CONFLICT DO NOTHING;


-- backfill the ProvenanceInput table
insert into <schema>.ProvenanceInput (outputID,inputID)
    select p.planeID, t.uri
    from <schema>.Plane p, unnest(string_to_array(p.provenance_inputs, ' ')) t(uri)
    where p.provenance_inputs is not null
ON CONFLICT DO NOTHING;
